"""
app.py
------
Flask backend for the Federated Clinical Query Fabric prototype.

Run:
    python app.py

Then open:
    http://localhost:5000
"""

import os
import functools
from datetime import datetime

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

import firebase_config as fbc
from firebase_config import get_db, verify_token, issue_dev_token, AuthError, USING_FIREBASE, BACKEND_MODE
import federated_engine as fe
from federated_engine import (
    FederatedEngine, QueryValidationError, all_institutions_public,
    simulate_failure, restore_institution, institution_status, public_terminology,
    INSTITUTIONS,
)

app = Flask(__name__, static_folder=None)
CORS(app)

db = get_db()
engine = FederatedEngine(db)

# --------------------------------------------------------------------------
# Demo users (development authentication only - see README "Authentication")
# Passwords are only ever compared server-side; never trust a client-supplied
# role.
# --------------------------------------------------------------------------

DEMO_USERS = {
    "researcher1": {"password": "researcher123", "role": "RESEARCHER", "name": "Dr. Elena Marsh"},
    "operator1": {"password": "operator123", "role": "INSTITUTION_OPERATOR", "name": "Sam Ortiz (Ops)"},
    "auditor1": {"password": "auditor123", "role": "AUDITOR", "name": "Priya Nair (Audit)"},
    "admin1": {"password": "admin123", "role": "ADMIN", "name": "Jordan Lee (Admin)"},
}

ROLE_PERMISSIONS = {
    "RESEARCHER": {"submit_query", "view_own_queries", "view_provenance", "view_institutions"},
    "INSTITUTION_OPERATOR": {"view_institutions", "manage_institutions", "view_own_queries"},
    "AUDITOR": {"view_audit", "view_provenance", "validate_result", "view_institutions"},
    "ADMIN": {"submit_query", "view_own_queries", "view_provenance", "view_institutions",
              "manage_institutions", "view_audit", "validate_result", "manage_users"},
}


def require_auth(permission=None):
    def decorator(view_fn):
        @functools.wraps(view_fn)
        def wrapper(*args, **kwargs):
            auth_header = request.headers.get("Authorization", "")
            token = auth_header.split(" ", 1)[1] if auth_header.startswith("Bearer ") else None
            try:
                identity = verify_token(token)
            except AuthError as e:
                engine.audit("AUTHORIZATION_DENIED", {"uid": None, "role": None, "name": None},
                             {"reason": str(e), "path": request.path})
                return jsonify({"error": "UNAUTHENTICATED", "message": str(e)}), 401

            if permission and permission not in ROLE_PERMISSIONS.get(identity["role"], set()):
                engine.audit("AUTHORIZATION_DENIED", identity, {"reason": "insufficient role", "path": request.path})
                return jsonify({"error": "FORBIDDEN", "message": "Your role does not permit this action"}), 403

            request.user = identity
            return view_fn(*args, **kwargs)
        return wrapper
    return decorator


# --------------------------------------------------------------------------
# Health
# --------------------------------------------------------------------------

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.utcnow().isoformat() + "Z",
        "backend_mode": BACKEND_MODE,
        "using_firebase": USING_FIREBASE,
    })


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------

@app.route("/api/auth/login", methods=["POST"])
def login():
    body = request.get_json(silent=True) or {}
    username = body.get("username", "")
    password = body.get("password", "")

    user = DEMO_USERS.get(username)
    if not user or user["password"] != password:
        engine.audit("AUTHORIZATION_DENIED", {"uid": username, "role": None, "name": None},
                     {"reason": "bad credentials"})
        return jsonify({"error": "INVALID_CREDENTIALS", "message": "Invalid username or password"}), 401

    token = issue_dev_token(uid=username, role=user["role"], display_name=user["name"])
    identity = {"uid": username, "role": user["role"], "name": user["name"]}
    engine.audit("LOGIN", identity, {})
    return jsonify({"token": token, "uid": username, "role": user["role"], "name": user["name"],
                    "backend_mode": BACKEND_MODE})


# --------------------------------------------------------------------------
# Institutions
# --------------------------------------------------------------------------

@app.route("/api/institutions")
@require_auth("view_institutions")
def list_institutions():
    return jsonify({"institutions": all_institutions_public()})


@app.route("/api/institutions/<inst_id>")
@require_auth("view_institutions")
def get_institution(inst_id):
    if inst_id not in INSTITUTIONS:
        return jsonify({"error": "NOT_FOUND", "message": "Unknown institution"}), 404
    match = [i for i in all_institutions_public() if i["id"] == inst_id][0]
    return jsonify(match)


@app.route("/api/institutions/<inst_id>/simulate-failure", methods=["POST"])
@require_auth("manage_institutions")
def api_simulate_failure(inst_id):
    if inst_id not in INSTITUTIONS:
        return jsonify({"error": "NOT_FOUND", "message": "Unknown institution"}), 404
    simulate_failure(inst_id)
    engine.audit("FAILURE_SIMULATED", request.user, {"institution": inst_id})
    return jsonify({"institution": inst_id, "status": institution_status(inst_id)})


@app.route("/api/institutions/<inst_id>/restore", methods=["POST"])
@require_auth("manage_institutions")
def api_restore(inst_id):
    if inst_id not in INSTITUTIONS:
        return jsonify({"error": "NOT_FOUND", "message": "Unknown institution"}), 404
    restore_institution(inst_id)
    engine.audit("INSTITUTION_RESTORED", request.user, {"institution": inst_id})
    return jsonify({"institution": inst_id, "status": institution_status(inst_id)})


# --------------------------------------------------------------------------
# Terminology (for the query builder UI)
# --------------------------------------------------------------------------

@app.route("/api/terminology")
@require_auth()
def api_terminology():
    return jsonify(public_terminology())


# --------------------------------------------------------------------------
# Query
# --------------------------------------------------------------------------

@app.route("/api/query", methods=["POST"])
@require_auth("submit_query")
def submit_query():
    body = request.get_json(silent=True) or {}
    try:
        record = engine.execute_query(body, request.user)
    except QueryValidationError as e:
        return jsonify({"error": "QUERY_VALIDATION_FAILED", "message": str(e)}), 400
    except Exception as e:  # never leak stack traces
        return jsonify({"error": "INTERNAL_ERROR", "message": "Query execution failed"}), 500
    return jsonify(record)


@app.route("/api/query/<query_id>")
@require_auth("view_own_queries")
def get_query(query_id):
    record = engine.get_query(query_id)
    if not record:
        return jsonify({"error": "NOT_FOUND", "message": "Query not found"}), 404
    return jsonify(record)


@app.route("/api/query/<query_id>/validate", methods=["POST"])
@require_auth("validate_result")
def validate_query(query_id):
    result = engine.validate_result(query_id, request.user)
    return jsonify(result)


@app.route("/api/query/<query_id>/provenance")
@require_auth("view_provenance")
def query_provenance(query_id):
    prov = engine.get_provenance(query_id)
    if not prov:
        return jsonify({"error": "NOT_FOUND", "message": "Provenance not found"}), 404
    return jsonify(prov)


# --------------------------------------------------------------------------
# Audit
# --------------------------------------------------------------------------

@app.route("/api/audit")
@require_auth("view_audit")
def api_audit():
    return jsonify({"entries": engine.list_audit()})


# --------------------------------------------------------------------------
# Benchmark
# --------------------------------------------------------------------------

@app.route("/api/benchmark", methods=["POST"])
@require_auth("submit_query")
def run_benchmark():
    result = engine.run_benchmark(request.user)
    return jsonify(result)


@app.route("/api/benchmark")
@require_auth()
def list_benchmarks():
    return jsonify({"benchmarks": engine.list_benchmarks()})


# --------------------------------------------------------------------------
# Frontend serving (single-URL deployment per project brief)
# --------------------------------------------------------------------------

FRONTEND_DIR = os.path.dirname(os.path.abspath(__file__))


@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "frontend.html")


@app.route("/styles.css")
def styles():
    return send_from_directory(FRONTEND_DIR, "styles.css")


@app.errorhandler(404)
def not_found(e):
    if request.path.startswith("/api/"):
        return jsonify({"error": "NOT_FOUND", "message": "Endpoint not found"}), 404
    return send_from_directory(FRONTEND_DIR, "frontend.html")


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "INTERNAL_ERROR", "message": "An unexpected error occurred"}), 500


if __name__ == "__main__":
    print(f"[app] Backend mode: {BACKEND_MODE}")
    print("[app] Demo credentials: researcher1/researcher123, operator1/operator123, "
          "auditor1/auditor123, admin1/admin123")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
