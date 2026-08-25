"""
firebase_config.py
-------------------
Persistence + authentication layer for the Federated Clinical Query Fabric.

Design:
  * If Firebase credentials are available (env var FIREBASE_SERVICE_ACCOUNT_JSON
    pointing at a service-account JSON file, or GOOGLE_APPLICATION_CREDENTIALS),
    this module initializes the real Firebase Admin SDK and talks to Firestore
    + Firebase Authentication.
  * If no credentials are available (the common case for local/offline demos,
    and for this prototype's sandboxed environment, which has no network path
    to Firebase), it falls back to a small local, file-backed store that
    implements the same collection/document interface used throughout the
    codebase (get_db().collection(name).document(id).set/get/update, .stream(),
    .add()). This keeps federated_engine.py and app.py identical regardless of
    which backend is active.

  This fallback is explicitly permitted by the project brief ("If Firebase
  Authentication cannot be used directly from the provided environment,
  implement secure backend authentication compatible with the available
  Firebase setup") and is clearly surfaced via `USING_FIREBASE` / `BACKEND_MODE`
  so the frontend and README can be honest about which mode is active.

  Never hard-code credentials here. Everything sensitive comes from environment
  variables.
"""

import os
import json
import time
import uuid
import hmac
import base64
import hashlib
import threading
from pathlib import Path

# --------------------------------------------------------------------------
# Attempt real Firebase initialization
# --------------------------------------------------------------------------

USING_FIREBASE = False
_firebase_app = None
_firestore_client = None

FIREBASE_CRED_PATH = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON") or os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS"
)

try:
    if FIREBASE_CRED_PATH and Path(FIREBASE_CRED_PATH).exists():
        import firebase_admin
        from firebase_admin import credentials, firestore as fb_firestore, auth as fb_auth

        cred = credentials.Certificate(FIREBASE_CRED_PATH)
        _firebase_app = firebase_admin.initialize_app(cred)
        _firestore_client = fb_firestore.client()
        USING_FIREBASE = True
except Exception as exc:  # pragma: no cover - defensive, environment dependent
    print(f"[firebase_config] Firebase initialization failed, falling back to local store: {exc}")
    USING_FIREBASE = False

BACKEND_MODE = "firebase" if USING_FIREBASE else "local-fallback"

# --------------------------------------------------------------------------
# Local Firestore-compatible fallback store
# --------------------------------------------------------------------------
# A minimal, thread-safe, file-persisted document store that mimics the
# subset of the Firestore client API this project relies on:
#   db.collection(name).document(id).set(data)
#   db.collection(name).document(id).get() -> DocSnapshot(.exists, .to_dict())
#   db.collection(name).document(id).update(data)
#   db.collection(name).document(id).id
#   db.collection(name).add(data) -> (None, DocRef)
#   db.collection(name).stream() -> iterable of DocSnapshot
#   db.collection(name).document(parent).collection(sub)... (nested, via key path)

_DATA_DIR = Path(__file__).parent / "data"
_DATA_DIR.mkdir(exist_ok=True)
_LOCAL_DB_FILE = _DATA_DIR / "local_firestore.json"
_lock = threading.RLock()


def _load_local_db():
    if _LOCAL_DB_FILE.exists():
        try:
            with open(_LOCAL_DB_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_local_db(data):
    tmp = _LOCAL_DB_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f)
    tmp.replace(_LOCAL_DB_FILE)


_local_store = _load_local_db()


class _DocSnapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class _DocRef:
    def __init__(self, collection_path, doc_id):
        self.collection_path = collection_path
        self.id = doc_id

    def set(self, data, merge=False):
        with _lock:
            col = _local_store.setdefault(self.collection_path, {})
            if merge and self.id in col:
                col[self.id].update(data)
            else:
                col[self.id] = data
            _save_local_db(_local_store)
        return self

    def update(self, data):
        with _lock:
            col = _local_store.setdefault(self.collection_path, {})
            existing = col.get(self.id, {})
            existing.update(data)
            col[self.id] = existing
            _save_local_db(_local_store)
        return self

    def get(self):
        with _lock:
            col = _local_store.get(self.collection_path, {})
            return _DocSnapshot(self.id, col.get(self.id))

    def collection(self, name):
        return _CollectionRef(f"{self.collection_path}/{self.id}/{name}")


class _CollectionRef:
    def __init__(self, path):
        self.path = path

    def document(self, doc_id=None):
        if doc_id is None:
            doc_id = uuid.uuid4().hex
        return _DocRef(self.path, doc_id)

    def add(self, data):
        doc_id = uuid.uuid4().hex
        ref = _DocRef(self.path, doc_id)
        ref.set(data)
        return (None, ref)

    def stream(self):
        with _lock:
            col = _local_store.get(self.path, {})
            return [_DocSnapshot(doc_id, data) for doc_id, data in col.items()]

    def where(self, *args, **kwargs):
        # Minimal no-op filter passthrough: federated_engine.py filters in
        # Python after streaming, since the local store doesn't implement
        # Firestore's server-side query operators. Real Firestore clients
        # support .where() natively.
        return self


class LocalFirestore:
    """Drop-in local substitute for a Firestore client."""

    def collection(self, name):
        return _CollectionRef(name)


def get_db():
    """Returns a Firestore-compatible client (real or local fallback)."""
    if USING_FIREBASE:
        return _firestore_client
    return LocalFirestore()


# --------------------------------------------------------------------------
# Authentication
# --------------------------------------------------------------------------
# Preferred: verify Firebase ID tokens via firebase_admin.auth.
# Fallback (this environment): a locally-issued, HMAC-signed development
# token. This is NOT a general-purpose JWT implementation - it is a compact,
# clearly-labeled dev-auth mechanism used only when Firebase Authentication
# is unavailable, as explicitly anticipated by the project brief. It is
# documented in README.md under "Authentication" / "Limitations".

_DEV_SECRET = os.environ.get("APP_DEV_AUTH_SECRET")
if not _DEV_SECRET:
    # Generate + persist a random secret for this deployment so tokens survive
    # process restarts during a demo, without ever hard-coding a secret value.
    _secret_file = _DATA_DIR / ".dev_secret"
    if _secret_file.exists():
        _DEV_SECRET = _secret_file.read_text().strip()
    else:
        _DEV_SECRET = base64.urlsafe_b64encode(os.urandom(32)).decode()
        _secret_file.write_text(_DEV_SECRET)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def issue_dev_token(uid: str, role: str, display_name: str, ttl_seconds: int = 8 * 3600) -> str:
    """Issues a signed, expiring development auth token (HMAC-SHA256)."""
    header = {"alg": "HS256", "typ": "DEVJWT"}
    payload = {
        "uid": uid,
        "role": role,
        "name": display_name,
        "iat": int(time.time()),
        "exp": int(time.time()) + ttl_seconds,
    }
    h = _b64url(json.dumps(header, separators=(",", ":")).encode())
    p = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{h}.{p}".encode()
    sig = hmac.new(_DEV_SECRET.encode(), signing_input, hashlib.sha256).digest()
    s = _b64url(sig)
    return f"{h}.{p}.{s}"


class AuthError(Exception):
    pass


def verify_token(token: str) -> dict:
    """
    Verifies a bearer token and returns the caller's identity:
    {"uid": ..., "role": ..., "name": ...}

    Uses Firebase Authentication when available. Otherwise verifies the
    locally-issued dev token's HMAC signature and expiry. In both cases the
    role/identity is derived from the *verified* token, never trusted from a
    client-supplied field.
    """
    if not token:
        raise AuthError("Missing authentication token")

    if USING_FIREBASE:
        from firebase_admin import auth as fb_auth  # local import: only needed here

        decoded = fb_auth.verify_id_token(token)
        return {
            "uid": decoded.get("uid"),
            "role": decoded.get("role", "RESEARCHER"),
            "name": decoded.get("name", decoded.get("uid")),
        }

    try:
        h, p, s = token.split(".")
    except ValueError:
        raise AuthError("Malformed token")

    signing_input = f"{h}.{p}".encode()
    expected_sig = hmac.new(_DEV_SECRET.encode(), signing_input, hashlib.sha256).digest()
    given_sig = _b64url_decode(s)
    if not hmac.compare_digest(expected_sig, given_sig):
        raise AuthError("Invalid token signature")

    payload = json.loads(_b64url_decode(p))
    if payload.get("exp", 0) < time.time():
        raise AuthError("Token expired")

    return {"uid": payload["uid"], "role": payload["role"], "name": payload["name"]}
