"""Dynamic patient monitoring, the app's logic (no I/O, unit-tested by tests/test_monitor_example.py).

The workflow: a base rule watches emergency-department registrations. Each registration adds that patient to the app's
patient rules, one per silver source, each `patient_id IN (<the census>)` AND what the app wants about them (updates,
transfers, discharges, admit diagnoses, selected labs and vitals, radiology, notes). Once the app has the chief
complaint, it decides whether the patient qualifies; one who doesn't is removed at once, and every patient is removed on
discharge or transfer. Adding or removing a patient is an edit of the patient rules' IN list (putSubscription), which
the hub indexes on patient_id: it doesn't touch any other rule, of this app or another.

Live only: a patient's records from before their rules went live (between the registration notification and the edit's
ACTIVE) aren't delivered. Read them from the app's own views if the workflow needs them.
"""

from dataclasses import dataclass, field

TEMPLATE = "ed-respiratory-monitoring"  # the data template the platform grants this workflow (subscriber_templates)
ED_POINTS_OF_CARE = [f"ED{i:02d}" for i in range(1, 9)]
LABS = ["2823-3", "2345-7", "718-7", "2160-0", "1920-8", "1751-7"]  # LOINC (alt_observation_cd)
VITALS = ["572 PULSE", "573 RESP", "569 SPO2", "574 TEMP"]  # flowsheet rows (observation_cd)
RESPIRATORY_WORDS = ("cough", "breath", "fever", "pneumonia", "wheez")
RESPIRATORY_CODES = ("J", "R05", "R06", "R50")  # ICD-10-CM prefixes: respiratory diseases, cough, dyspnea, fever


def _list(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


BASE = {
    "ed-registrations": f"""
        SELECT patient_id, visit_id, patient_point_of_care, chief_complaint, msg_type FROM silver_encounters
        WHERE msg_type = 'ADT^A04' AND patient_class = 'E' AND patient_point_of_care IN ({_list(ED_POINTS_OF_CARE)})""",
}


def patient_rules(patients: list[str]) -> dict[str, str | None]:
    """Name -> SQL of each patient rule for this census (None: no patients, so the rule is cancelled)."""
    if not patients:
        return dict.fromkeys(_PATIENT)
    ids = _list(sorted(patients))
    return {name: sql.format(patients=ids) for name, sql in _PATIENT.items()}


_PATIENT = {
    "ed-patient-encounters": """
        SELECT patient_id, visit_id, msg_type, patient_point_of_care, chief_complaint FROM silver_encounters
        WHERE patient_id IN ({patients}) AND msg_type IN ('ADT^A08', 'ADT^A02', 'ADT^A03')""",
    "ed-patient-diagnoses": """
        SELECT patient_id, visit_id, diagnosis_cd, diagnosis_type FROM silver_diagnoses
        WHERE patient_id IN ({patients}) AND diagnosis_type IN ('ADMITTING', 'WORKING', 'FINAL')""",
    "ed-patient-results": f"""
        SELECT patient_id, visit_id, observation_cd, alt_observation_cd, value, value_num, units, abnormal_flag,
               diagnostic_service FROM silver_observations
        WHERE patient_id IN ({{patients}}) AND (
              (alt_observation_cd_system = 'LN' AND alt_observation_cd IN ({_list(LABS)}))
           OR (observation_cd_system = 'FLOWSHEET' AND observation_cd IN ({_list(VITALS)}))
           OR diagnostic_service = 'RAD')""",
    "ed-patient-orders": """
        SELECT patient_id, visit_id, order_cd, diagnostic_service FROM silver_orders
        WHERE patient_id IN ({patients}) AND diagnostic_service = 'RAD'""",
    "ed-patient-notes": """
        SELECT patient_id, visit_id, document_type, document_text FROM silver_documents
        WHERE patient_id IN ({patients})""",
}
PATIENT_RULES = tuple(_PATIENT)


def respiratory(chief_complaint: str | None = None, diagnosis_cd: str | None = None) -> bool:
    """The stand-in for the app's own qualification model: a respiratory complaint or admit diagnosis."""
    text = (chief_complaint or "").lower()
    return any(w in text for w in RESPIRATORY_WORDS) or (diagnosis_cd or "").upper().startswith(RESPIRATORY_CODES)


@dataclass
class Patient:
    mrn: str
    visit: str | None
    since: float
    chief_complaint: str | None = None
    qualified: bool | None = None  # None: not evaluated yet


@dataclass
class Census:
    """The patients the app watches. Each method returns what happened ('added', 'removed: <why>', or None)."""

    patients: dict[str, Patient] = field(default_factory=dict)
    removed: dict[str, str] = field(default_factory=dict)  # mrn -> why, for this run
    version: int = 0  # bumps on every change; the app applies a new version to its patient rules

    def ids(self) -> list[str]:
        return sorted(self.patients)

    def registered(self, payload: dict, now: float) -> str | None:
        """A base-rule notification: an ED registration."""
        mrn = payload.get("patient_id")
        if not mrn or mrn in self.patients:
            return None
        self.patients[mrn] = Patient(mrn, payload.get("visit_id"), now, payload.get("chief_complaint"))
        self.removed.pop(mrn, None)
        self.version += 1
        return "added"

    def on_patient_record(self, rule: str, payload: dict) -> str | None:
        """A patient-rule notification: evaluate the patient, or end the encounter."""
        p = self.patients.get(payload.get("patient_id"))
        if p is None:
            return None
        if rule == "ed-patient-encounters":
            if payload.get("msg_type") in ("ADT^A03", "ADT^A02"):
                return self._remove(p, "discharged" if payload["msg_type"] == "ADT^A03" else "transferred")
            p.chief_complaint = payload.get("chief_complaint") or p.chief_complaint
            return self._evaluate(p)
        if rule == "ed-patient-diagnoses" and payload.get("diagnosis_type") == "ADMITTING":
            if respiratory(diagnosis_cd=payload.get("diagnosis_cd")):
                p.qualified = True
            return None
        return None

    def _evaluate(self, p: Patient) -> str | None:
        """After the chief complaint arrives: keep a respiratory patient, release anyone else at once."""
        if p.qualified is None:
            p.qualified = respiratory(p.chief_complaint)
            if not p.qualified:
                return self._remove(p, "not qualified")
        return None

    def _remove(self, p: Patient, why: str) -> str:
        del self.patients[p.mrn]
        self.removed[p.mrn] = why
        self.version += 1
        return f"removed: {why}"
