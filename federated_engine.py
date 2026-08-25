"""
federated_engine.py
--------------------
Core logic for the Federated Clinical Query Fabric.

The central idea this file implements:

    The researcher sends a clinical question to the federation. Each
    institution computes the answer against its own data. Only authorized
    aggregate information comes back. Patient records never leave the
    institution and never pass through a central database.

Contents:
  * INSTITUTIONS       - four participating institutions, each with its own
                          heterogeneous local schema and policy.
  * TERMINOLOGY         - a small canonical -> local terminology mapping
                          (ICD-10 / RxNorm-style / LOINC-style identifiers).
  * SyntheticDataStore   - deterministic per-institution synthetic patient
                          data generation (never leaves the institution).
  * FederatedEngine     - validation, planning, per-institution execution,
                          aggregation, suppression, provenance, hashing,
                          result validation, audit logging, benchmarking.
"""

import hashlib
import json
import random
import time
import uuid
from datetime import date, datetime, timedelta

# --------------------------------------------------------------------------
# Institution registry
# --------------------------------------------------------------------------
# Each institution has its OWN local schema. The federation never normalizes
# these into one shared schema - it translates the canonical query into each
# institution's local field names/codes at query time (see SCHEMA_MAPPINGS).

INSTITUTIONS = {
    "hospital_alpha": {
        "id": "hospital_alpha",
        "name": "Hospital Alpha",
        "type": "General Hospital",
        "record_count": 2200,
        "local_schema": ["patient_id", "condition_code", "medication_name", "hba1c", "admission_date"],
        "policy": {
            "allowed_operations": ["patient_count"],
            "minimum_group_size": 5,
            "allow_lab_filters": True,
        },
    },
    "hospital_beta": {
        "id": "hospital_beta",
        "name": "Hospital Beta",
        "type": "Regional Hospital",
        "record_count": 1800,
        "local_schema": ["person_id", "diagnosis", "drug_code", "lab_hba1c", "encounter_date"],
        "policy": {
            "allowed_operations": ["patient_count"],
            "minimum_group_size": 5,
            "allow_lab_filters": True,
        },
    },
    "lab_gamma": {
        "id": "lab_gamma",
        "name": "Diagnostic Lab Gamma",
        "type": "Diagnostic Laboratory",
        "record_count": 2600,
        "local_schema": ["subject_key", "test_code", "numeric_value", "test_date"],
        "policy": {
            # Lab Gamma only holds lab test data, not diagnosis/medication -
            # a realistic policy restriction demonstrating heterogeneous
            # institutional capability.
            "allowed_operations": ["patient_count"],
            "minimum_group_size": 10,
            "allow_lab_filters": True,
        },
    },
    "hospital_delta": {
        "id": "hospital_delta",
        "name": "Hospital Delta",
        "type": "Teaching Hospital",
        "record_count": 2000,
        "local_schema": ["record_id", "condition", "rx", "lab_value_hba1c", "visit_date"],
        "policy": {
            "allowed_operations": ["patient_count"],
            "minimum_group_size": 5,
            "allow_lab_filters": True,
        },
    },
}

# Mutable runtime status (failure simulation toggles this per institution).
_RUNTIME_STATUS = {inst_id: {"online": True, "simulated_failure": False} for inst_id in INSTITUTIONS}


# --------------------------------------------------------------------------
# Terminology: canonical concept -> local institutional representation
# --------------------------------------------------------------------------

TERMINOLOGY = {
    "diagnosis": {
        "E11": {  # ICD-10: Type 2 diabetes mellitus
            "label": "Type 2 Diabetes",
            "system": "ICD-10",
            "local": {
                "hospital_alpha": "E11",
                "hospital_beta": "TYPE2_DM",
                "lab_gamma": None,  # lab doesn't hold diagnoses
                "hospital_delta": "T2D",
            },
        },
        "I10": {
            "label": "Hypertension",
            "system": "ICD-10",
            "local": {
                "hospital_alpha": "I10",
                "hospital_beta": "HTN",
                "lab_gamma": None,
                "hospital_delta": "HYPERTENSION",
            },
        },
        "J45": {
            "label": "Asthma",
            "system": "ICD-10",
            "local": {
                "hospital_alpha": "J45",
                "hospital_beta": "ASTHMA",
                "lab_gamma": None,
                "hospital_delta": "ASTHMA",
            },
        },
    },
    "medication": {
        "RX_METFORMIN": {
            "label": "Metformin",
            "system": "RxNorm-style",
            "local": {
                "hospital_alpha": "METFORMIN",
                "hospital_beta": "RX_METFORMIN",
                "lab_gamma": None,
                "hospital_delta": "METFORMIN",
            },
        },
        "RX_INSULIN": {
            "label": "Insulin",
            "system": "RxNorm-style",
            "local": {
                "hospital_alpha": "INSULIN",
                "hospital_beta": "RX_INSULIN",
                "lab_gamma": None,
                "hospital_delta": "INSULIN",
            },
        },
        "RX_ATORVASTATIN": {
            "label": "Atorvastatin",
            "system": "RxNorm-style",
            "local": {
                "hospital_alpha": "ATORVASTATIN",
                "hospital_beta": "RX_ATORVASTATIN",
                "lab_gamma": None,
                "hospital_delta": "ATORVASTATIN",
            },
        },
    },
    "lab": {
        "LOINC_4548-4": {
            "label": "HbA1c",
            "system": "LOINC-style",
            "local": {
                "hospital_alpha": "hba1c",
                "hospital_beta": "lab_hba1c",
                "lab_gamma": "HBA1C",
                "hospital_delta": "lab_value_hba1c",
            },
        },
        "LOINC_2160-0": {
            "label": "Creatinine",
            "system": "LOINC-style",
            "local": {
                "hospital_alpha": "hba1c",  # only HbA1c modeled numerically in alpha for this prototype
                "hospital_beta": "lab_hba1c",
                "lab_gamma": "CREATININE",
                "hospital_delta": "lab_value_hba1c",
            },
        },
    },
}


def _find_concept(concept_type, code):
    table = TERMINOLOGY.get(concept_type, {})
    if code in table:
        return code, table[code]
    for k, v in table.items():
        if v["label"].lower() == str(code).lower():
            return k, v
    return None, None


# --------------------------------------------------------------------------
# Synthetic data (deterministic, generated once per process, never leaves
# the owning institution's "local computation" boundary)
# --------------------------------------------------------------------------

class SyntheticDataStore:
    """
    Generates and holds deterministic synthetic patient-level data FOR EACH
    institution, in that institution's own heterogeneous schema. This data
    is only ever touched by that institution's local execution function -
    the federated coordinator never reads it directly or centrally.
    """

    def __init__(self):
        self._data = {}
        for inst_id, inst in INSTITUTIONS.items():
            self._data[inst_id] = self._generate(inst_id, inst["record_count"])

    def _generate(self, inst_id, count):
        rng = random.Random(f"seed::{inst_id}")  # deterministic per institution
        diagnoses_alpha = ["E11", "I10", "J45", None]
        meds_alpha = ["METFORMIN", "INSULIN", "ATORVASTATIN", None]

        diagnoses_beta = ["TYPE2_DM", "HTN", "ASTHMA", None]
        meds_beta = ["RX_METFORMIN", "RX_INSULIN", "RX_ATORVASTATIN", None]

        diagnoses_delta = ["T2D", "HYPERTENSION", "ASTHMA", None]
        meds_delta = ["METFORMIN", "INSULIN", "ATORVASTATIN", None]

        start = date(2023, 1, 1)
        records = []
        for i in range(count):
            offset_days = rng.randint(0, 900)
            rec_date = (start + timedelta(days=offset_days)).isoformat()

            if inst_id == "hospital_alpha":
                records.append({
                    "patient_id": f"ALPHA-{i:06d}",
                    "condition_code": rng.choice(diagnoses_alpha),
                    "medication_name": rng.choice(meds_alpha),
                    "hba1c": round(rng.uniform(4.5, 11.5), 1),
                    "admission_date": rec_date,
                })
            elif inst_id == "hospital_beta":
                records.append({
                    "person_id": f"BETA-{i:06d}",
                    "diagnosis": rng.choice(diagnoses_beta),
                    "drug_code": rng.choice(meds_beta),
                    "lab_hba1c": round(rng.uniform(4.5, 11.5), 1),
                    "encounter_date": rec_date,
                })
            elif inst_id == "lab_gamma":
                records.append({
                    "subject_key": f"GAMMA-{i:06d}",
                    "test_code": rng.choice(["HBA1C", "CREATININE"]),
                    "numeric_value": round(rng.uniform(0.5, 12.0), 2),
                    "test_date": rec_date,
                })
            else:  # hospital_delta
                records.append({
                    "record_id": f"DELTA-{i:06d}",
                    "condition": rng.choice(diagnoses_delta),
                    "rx": rng.choice(meds_delta),
                    "lab_value_hba1c": round(rng.uniform(4.5, 11.5), 1),
                    "visit_date": rec_date,
                })
        return records

    def local_data(self, inst_id):
        return self._data[inst_id]


_SYNTHETIC = SyntheticDataStore()


# --------------------------------------------------------------------------
# Query validation
# --------------------------------------------------------------------------

class QueryValidationError(Exception):
    pass


ALLOWED_CONDITION_TYPES = {"diagnosis", "medication", "lab"}
ALLOWED_LAB_OPERATORS = {">", ">=", "<", "<=", "==", "!="}


def validate_canonical_query(query: dict):
    if not isinstance(query, dict):
        raise QueryValidationError("Query must be a JSON object")
    if query.get("query_type") != "patient_count":
        raise QueryValidationError("Only 'patient_count' query_type is currently supported")

    conditions = query.get("conditions", [])
    if not isinstance(conditions, list) or len(conditions) == 0:
        raise QueryValidationError("At least one condition is required")

    for c in conditions:
        ctype = c.get("type")
        if ctype not in ALLOWED_CONDITION_TYPES:
            raise QueryValidationError(f"Unsupported condition type: {ctype}")
        if ctype in ("diagnosis", "medication"):
            if not c.get("code"):
                raise QueryValidationError(f"{ctype} condition requires a 'code'")
        if ctype == "lab":
            if not c.get("code"):
                raise QueryValidationError("lab condition requires a 'code'")
            op = c.get("operator")
            if op not in ALLOWED_LAB_OPERATORS:
                raise QueryValidationError(f"Invalid lab operator: {op}")
            try:
                float(c.get("value"))
            except (TypeError, ValueError):
                raise QueryValidationError("lab condition requires a numeric 'value'")

    logic = query.get("logic", "AND")
    if logic not in ("AND", "OR"):
        raise QueryValidationError("logic must be AND or OR")

    date_range = query.get("date_range")
    if date_range:
        try:
            datetime.fromisoformat(date_range["start"])
            datetime.fromisoformat(date_range["end"])
        except Exception:
            raise QueryValidationError("date_range must have valid ISO 'start' and 'end' dates")

    return True


def compute_query_hash(query: dict) -> str:
    canonical_json = json.dumps(query, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode()).hexdigest()


def generate_query_id() -> str:
    # Human-readable sequential-looking ID, e.g. Q-2026-3f9a1c
    return f"Q-{datetime.utcnow().year}-{uuid.uuid4().hex[:6]}"


# --------------------------------------------------------------------------
# Per-institution local execution
# --------------------------------------------------------------------------

def _record_matches(inst_id, record, condition):
    ctype = condition["type"]
    if ctype == "diagnosis":
        code, concept = _find_concept("diagnosis", condition["code"])
        if not concept:
            return False
        local_code = concept["local"].get(inst_id)
        if local_code is None:
            return False  # institution doesn't hold this concept type
        field = {"hospital_alpha": "condition_code", "hospital_beta": "diagnosis", "hospital_delta": "condition"}.get(inst_id)
        if field is None:
            return False  # e.g. lab_gamma has no diagnosis field
        return record.get(field) == local_code

    if ctype == "medication":
        code, concept = _find_concept("medication", condition["code"])
        if not concept:
            return False
        local_code = concept["local"].get(inst_id)
        if local_code is None:
            return False
        field = {"hospital_alpha": "medication_name", "hospital_beta": "drug_code", "hospital_delta": "rx"}.get(inst_id)
        if field is None:
            return False
        return record.get(field) == local_code

    if ctype == "lab":
        code, concept = _find_concept("lab", condition["code"])
        if not concept:
            return False
        op = condition["operator"]
        threshold = float(condition["value"])

        if inst_id == "lab_gamma":
            local_test_code = concept["local"].get(inst_id)
            if record.get("test_code") != local_test_code:
                return False
            value = record.get("numeric_value")
        else:
            field = {
                "hospital_alpha": "hba1c",
                "hospital_beta": "lab_hba1c",
                "hospital_delta": "lab_value_hba1c",
            }.get(inst_id)
            if field is None:
                return False
            value = record.get(field)

        if value is None:
            return False

        ops = {
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
        }
        return ops[op](value, threshold)

    return False


def _record_in_date_range(inst_id, record, date_range):
    if not date_range:
        return True
    field = {
        "hospital_alpha": "admission_date",
        "hospital_beta": "encounter_date",
        "lab_gamma": "test_date",
        "hospital_delta": "visit_date",
    }[inst_id]
    rec_date = record.get(field)
    if not rec_date:
        return True
    return date_range["start"] <= rec_date <= date_range["end"]


def execute_institution_query(inst_id: str, canonical_query: dict) -> dict:
    """
    Executes the canonical query AGAINST THIS INSTITUTION'S OWN LOCAL DATA
    ONLY. Returns aggregate-only information - never patient-level records.
    Simulates realistic network/compute latency and honors simulated failure
    toggles and minimum-group-size suppression policy.
    """
    started = time.perf_counter()
    status_flags = _RUNTIME_STATUS[inst_id]
    policy = INSTITUTIONS[inst_id]["policy"]

    # Simulate network/compute latency proportional to record count.
    simulated_latency = random.uniform(0.05, 0.18)
    time.sleep(simulated_latency)

    if status_flags["simulated_failure"] or not status_flags["online"]:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        return {
            "institution": inst_id,
            "status": "TIMEOUT",
            "patient_count": None,
            "execution_time_ms": elapsed_ms,
            "reason": "Institution unreachable (simulated failure)",
        }

    if canonical_query.get("query_type") not in policy["allowed_operations"]:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        return {
            "institution": inst_id,
            "status": "DENIED",
            "patient_count": None,
            "execution_time_ms": elapsed_ms,
            "reason": "Operation not permitted by institutional policy",
        }

    records = _SYNTHETIC.local_data(inst_id)
    logic = canonical_query.get("logic", "AND")
    conditions = canonical_query.get("conditions", [])
    date_range = canonical_query.get("date_range")

    matched = 0
    for record in records:
        if not _record_in_date_range(inst_id, record, date_range):
            continue
        results = [_record_matches(inst_id, record, c) for c in conditions]
        ok = all(results) if logic == "AND" else any(results)
        if ok:
            matched += 1

    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)

    min_group = policy.get("minimum_group_size", 0)
    if matched < min_group:
        return {
            "institution": inst_id,
            "status": "SUPPRESSED",
            "patient_count": None,
            "execution_time_ms": elapsed_ms,
            "reason": f"Result below institutional minimum group size ({min_group})",
        }

    return {
        "institution": inst_id,
        "status": "SUCCESS",
        "patient_count": matched,
        "execution_time_ms": elapsed_ms,
        "reason": None,
    }


# --------------------------------------------------------------------------
# Failure simulation controls
# --------------------------------------------------------------------------

def simulate_failure(inst_id):
    if inst_id not in _RUNTIME_STATUS:
        raise KeyError(inst_id)
    _RUNTIME_STATUS[inst_id]["simulated_failure"] = True
    _RUNTIME_STATUS[inst_id]["online"] = False


def restore_institution(inst_id):
    if inst_id not in _RUNTIME_STATUS:
        raise KeyError(inst_id)
    _RUNTIME_STATUS[inst_id]["simulated_failure"] = False
    _RUNTIME_STATUS[inst_id]["online"] = True


def institution_status(inst_id):
    return _RUNTIME_STATUS[inst_id]


def all_institutions_public():
    """Institution metadata safe to expose to the frontend (no patient data)."""
    out = []
    for inst_id, inst in INSTITUTIONS.items():
        rt = _RUNTIME_STATUS[inst_id]
        out.append({
            "id": inst_id,
            "name": inst["name"],
            "type": inst["type"],
            "local_schema": inst["local_schema"],
            "policy": inst["policy"],
            "online": rt["online"],
            "simulated_failure": rt["simulated_failure"],
            "record_count": inst["record_count"],
        })
    return out


# --------------------------------------------------------------------------
# FederatedEngine: orchestration, aggregation, provenance, validation, audit
# --------------------------------------------------------------------------

class FederatedEngine:
    def __init__(self, db):
        self.db = db

    # ---- audit -----------------------------------------------------------
    def audit(self, event_type, user, details=None):
        audit_id = uuid.uuid4().hex
        entry = {
            "id": audit_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": event_type,
            "user": user.get("uid") if user else None,
            "user_name": user.get("name") if user else None,
            "role": user.get("role") if user else None,
            "details": details or {},
        }
        self.db.collection("audit_logs").document(audit_id).set(entry)
        return entry

    # ---- query execution ---------------------------------------------------
    def execute_query(self, canonical_query: dict, user: dict) -> dict:
        validate_canonical_query(canonical_query)

        query_id = generate_query_id()
        query_hash = compute_query_hash(canonical_query)
        institutions = list(INSTITUTIONS.keys())

        self.audit("QUERY_SUBMITTED", user, {"query_id": query_id, "query_hash": query_hash})

        institution_results = []
        for inst_id in institutions:
            result = execute_institution_query(inst_id, canonical_query)
            institution_results.append(result)
            event = {
                "SUCCESS": "NODE_SUCCESS",
                "TIMEOUT": "NODE_TIMEOUT",
                "DENIED": "NODE_FAILURE",
                "SUPPRESSED": "NODE_SUCCESS",
            }.get(result["status"], "NODE_FAILURE")
            self.audit(event, user, {"query_id": query_id, "institution": inst_id, "status": result["status"]})

            # Store per-institution execution record (aggregate-only).
            self.db.collection("queries").document(query_id).collection("executions").document(inst_id).set(result)

        aggregation = self._aggregate(institution_results)

        record = {
            "query_id": query_id,
            "researcher_id": user.get("uid"),
            "researcher_name": user.get("name"),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "canonical_query": canonical_query,
            "query_hash": query_hash,
            "participating_institutions": institutions,
            "institution_results": institution_results,
            "final_result": aggregation["total"],
            "completeness": aggregation["completeness"],
            "successful_institutions": aggregation["successful_institutions"],
            "failed_institutions": aggregation["failed_institutions"],
            "validation_status": "NOT_VALIDATED",
        }
        self.db.collection("queries").document(query_id).set(record)

        provenance = {
            "query_id": query_id,
            "researcher_id": user.get("uid"),
            "timestamp": record["timestamp"],
            "canonical_query": canonical_query,
            "query_hash": query_hash,
            "participating_institutions": institutions,
            "institution_statuses": {r["institution"]: r["status"] for r in institution_results},
            "local_execution_results": institution_results,
            "execution_times_ms": {r["institution"]: r["execution_time_ms"] for r in institution_results},
            "aggregation_method": "SUM_OF_SUCCESSFUL_INSTITUTION_COUNTS",
            "final_result": aggregation["total"],
            "completeness": aggregation["completeness"],
        }
        self.db.collection("provenance").document(query_id).set(provenance)

        self.audit("RESULT_AGGREGATED", user, {"query_id": query_id, "final_result": aggregation["total"],
                                                "completeness": aggregation["completeness"]})

        return record

    def _aggregate(self, institution_results):
        successful = [r for r in institution_results if r["status"] == "SUCCESS"]
        suppressed = [r for r in institution_results if r["status"] == "SUPPRESSED"]
        failed = [r for r in institution_results if r["status"] in ("TIMEOUT", "DENIED", "UNAVAILABLE", "FAILED")]

        total = sum(r["patient_count"] for r in successful)  # failed nodes are NEVER treated as 0
        n = len(institution_results)
        completeness = "COMPLETE" if len(successful) + len(suppressed) == n else "INCOMPLETE"

        return {
            "total": total,
            "completeness": completeness,
            "successful_institutions": [r["institution"] for r in successful],
            "suppressed_institutions": [r["institution"] for r in suppressed],
            "failed_institutions": [r["institution"] for r in failed],
        }

    # ---- retrieval ---------------------------------------------------------
    def get_query(self, query_id):
        snap = self.db.collection("queries").document(query_id).get()
        return snap.to_dict() if snap.exists else None

    def get_provenance(self, query_id):
        snap = self.db.collection("provenance").document(query_id).get()
        return snap.to_dict() if snap.exists else None

    def list_audit(self, limit=200):
        entries = [s.to_dict() for s in self.db.collection("audit_logs").stream()]
        entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
        return entries[:limit]

    # ---- validation ---------------------------------------------------------
    def validate_result(self, query_id, user):
        record = self.get_query(query_id)
        if not record:
            return {"query_id": query_id, "valid": False, "reason": "Query not found"}

        provenance = self.get_provenance(query_id)
        if not provenance:
            return {"query_id": query_id, "valid": False, "reason": "Provenance not found"}

        recomputed_hash = compute_query_hash(record["canonical_query"])
        if recomputed_hash != record["query_hash"]:
            result = {"query_id": query_id, "valid": False, "reason": "Query hash mismatch (query definition changed)"}
            self.audit("RESULT_VALIDATED", user, {"query_id": query_id, "valid": False})
            self.db.collection("queries").document(query_id).update({"validation_status": "INVALID"})
            return result

        executions = [s.to_dict() for s in self.db.collection("queries").document(query_id).collection("executions").stream()]
        if len(executions) != len(record["participating_institutions"]):
            result = {"query_id": query_id, "valid": False, "reason": "Missing institution execution records"}
            self.audit("RESULT_VALIDATED", user, {"query_id": query_id, "valid": False})
            return result

        successful = [e for e in executions if e["status"] == "SUCCESS"]
        failed = [e for e in executions if e["status"] in ("TIMEOUT", "DENIED", "UNAVAILABLE", "FAILED")]

        # Rule: failed nodes must never have been counted as zero/contributing.
        for f in failed:
            if f.get("patient_count") not in (None,):
                result = {"query_id": query_id, "valid": False,
                          "reason": f"Failed institution {f['institution']} incorrectly carries a numeric count"}
                self.audit("RESULT_VALIDATED", user, {"query_id": query_id, "valid": False})
                return result

        expected_total = sum(e["patient_count"] for e in successful)
        if expected_total != record["final_result"]:
            result = {
                "query_id": query_id,
                "valid": False,
                "reason": "Aggregation mismatch",
                "expected": expected_total,
                "recorded": record["final_result"],
            }
            self.audit("RESULT_VALIDATED", user, {"query_id": query_id, "valid": False})
            self.db.collection("queries").document(query_id).update({"validation_status": "INVALID"})
            return result

        n = len(executions)
        suppressed = [e for e in executions if e["status"] == "SUPPRESSED"]
        expected_completeness = "COMPLETE" if len(successful) + len(suppressed) == n else "INCOMPLETE"
        if expected_completeness != record["completeness"]:
            result = {"query_id": query_id, "valid": False, "reason": "Completeness status incorrect",
                      "expected": expected_completeness, "recorded": record["completeness"]}
            self.audit("RESULT_VALIDATED", user, {"query_id": query_id, "valid": False})
            self.db.collection("queries").document(query_id).update({"validation_status": "INVALID"})
            return result

        result = {
            "query_id": query_id,
            "valid": True,
            "reason": "All checks passed",
            "expected_total": expected_total,
            "recorded_total": record["final_result"],
            "completeness": record["completeness"],
        }
        self.audit("RESULT_VALIDATED", user, {"query_id": query_id, "valid": True})
        self.db.collection("queries").document(query_id).update({"validation_status": "VALID"})
        return result

    # ---- benchmark ---------------------------------------------------------
    def run_benchmark(self, user):
        """
        Actually executes and times BOTH:
          - a federated query (per-institution local computation, coordinator
            only sees aggregate results)
          - a centralized baseline (all synthetic records logically combined
            into one in-memory dataset and scanned in one pass)
        against the same demo clinical question, and reports real measured
        numbers. The centralized path exists ONLY for this benchmark - it is
        never used to answer real queries.
        """
        demo_query = {
            "query_type": "patient_count",
            "conditions": [
                {"type": "diagnosis", "code": "E11"},
                {"type": "medication", "code": "RX_METFORMIN"},
                {"type": "lab", "code": "LOINC_4548-4", "operator": ">", "value": 7},
            ],
            "logic": "AND",
        }

        # --- Federated timing ---
        fed_start = time.perf_counter()
        institution_results = [execute_institution_query(inst_id, demo_query) for inst_id in INSTITUTIONS]
        fed_elapsed_ms = round((time.perf_counter() - fed_start) * 1000, 1)
        agg = self._aggregate(institution_results)

        # Data "transferred" in the federated model: only small aggregate
        # JSON payloads (a handful of ints/strings) leave each institution.
        fed_bytes = sum(len(json.dumps(r)) for r in institution_results)

        # --- Centralized baseline timing ---
        # Logically pools ALL institutions' synthetic records into one
        # in-memory dataset (simulating what a centralized architecture would
        # require) and scans them in a single pass. This is benchmark-only.
        cen_start = time.perf_counter()
        total_records_scanned = 0
        centralized_matches = 0
        for inst_id in INSTITUTIONS:
            for record in _SYNTHETIC.local_data(inst_id):
                total_records_scanned += 1
                results = [_record_matches(inst_id, record, c) for c in demo_query["conditions"]]
                if all(results):
                    centralized_matches += 1
        cen_elapsed_ms = round((time.perf_counter() - cen_start) * 1000, 1)

        # Data "transferred" in the centralized model: every raw record would
        # need to travel to the central point in a real deployment.
        approx_record_size_bytes = 120
        cen_bytes = total_records_scanned * approx_record_size_bytes

        benchmark_id = uuid.uuid4().hex
        result = {
            "benchmark_id": benchmark_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "query_used": demo_query,
            "federated": {
                "latency_ms": fed_elapsed_ms,
                "institutions": len(INSTITUTIONS),
                "successful_institutions": len(agg["successful_institutions"]),
                "failed_institutions": len(agg["failed_institutions"]),
                "data_transferred_bytes": fed_bytes,
                "central_patient_database": False,
                "final_result": agg["total"],
            },
            "centralized_baseline": {
                "latency_ms": cen_elapsed_ms,
                "institutions": 1,
                "records_scanned": total_records_scanned,
                "data_transferred_bytes": cen_bytes,
                "central_patient_database": True,
                "final_result": centralized_matches,
            },
        }
        self.db.collection("benchmarks").document(benchmark_id).set(result)
        self.audit("BENCHMARK_RUN", user, {"benchmark_id": benchmark_id})
        return result

    def list_benchmarks(self, limit=20):
        entries = [s.to_dict() for s in self.db.collection("benchmarks").stream()]
        entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
        return entries[:limit]


# --------------------------------------------------------------------------
# Terminology exposure (safe, non-patient metadata for the frontend query
# builder)
# --------------------------------------------------------------------------

def public_terminology():
    def entries(kind):
        return [
            {"code": code, "label": v["label"], "system": v["system"]}
            for code, v in TERMINOLOGY[kind].items()
        ]
    return {
        "diagnosis": entries("diagnosis"),
        "medication": entries("medication"),
        "lab": entries("lab"),
    }
