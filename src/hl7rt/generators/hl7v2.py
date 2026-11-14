"""Synthetic raw HL7 v2.5.1 messages (ADT^A01/A02/A03/A04/A08, ORU^R01, ORM^O01, MDM^T02) at a steady rate.

Messages are built by field number (`segment("PID", {3: ..., 7: ...})`) so positions can't drift. They carry the shapes
real subscription rules filter on (docs/examples/criteria.md): labs with LOINC as the alternate code
(`alt_observation_cd_system = 'LN'`), flowsheet vitals (`observation_cd_system = 'FLOWSHEET'`), ICD-10-CM diagnoses with
a type on admits, registrations, and updates, and patients spread over several points of care. All values are synthetic.

Alongside that background traffic, `Journeys` plays coherent emergency-department encounters, one patient and visit
each, for dynamic (patient-specific) subscriptions to follow: registration (A04, chief complaint, admitting diagnosis),
an update (A08), labs and vitals, and for respiratory complaints a chest X-ray order, its report, and a note, ending in a
discharge (A03) or a transfer (A02). Steps are seconds apart, and journey patients are outside the background population.
Each record carries a `sent_ms` header so the producer -> Event Hubs hop can be measured too.
A small share of messages are deliberately invalid so the quarantine topic has something to show.
"""

import argparse
import heapq
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from hl7rt import eventhubs

ENCODING = "^~\\&"
FACILITIES = ["FAC01", "FAC02", "FAC03"]
MESSAGE_MIX = [("ADT^A04", 3), ("ADT^A01", 1), ("ADT^A08", 1), ("ADT^A02", 1), ("ADT^A03", 1), ("ORU^R01", 5),
               ("ORM^O01", 2), ("MDM^T02", 1)]  # fmt: skip
STRUCTURE = {"ADT": "ADT_A01", "ORU": "ORU_R01", "ORM": "ORM_O01", "MDM": "MDM_T02"}
# (LOINC, name, units, low, high, local code): OBX-3 = local^name^LAB^LOINC^name^LN, as the feed rules expect
LABS = [("2823-3", "Potassium", "mmol/L", 3.5, 5.1, "K"), ("2345-7", "Glucose", "mg/dL", 70, 99, "GLU"),
        ("718-7", "Hemoglobin", "g/dL", 12.0, 17.5, "HGB"), ("2160-0", "Creatinine", "mg/dL", 0.6, 1.3, "CREAT"),
        ("1920-8", "AST", "U/L", 10, 40, "AST"), ("1751-7", "Albumin", "g/dL", 3.5, 5.0, "ALB")]  # fmt: skip
# (flowsheet row, name, units, low, high): OBX-3 = row^name^FLOWSHEET
VITALS = [("572 PULSE", "Pulse", "/min", 60, 100), ("573 RESP", "Respirations", "/min", 12, 20),
          ("569 SPO2", "SpO2", "%", 95, 100), ("574 TEMP", "Temperature", "degF", 97.0, 99.5)]  # fmt: skip
VITALS_SHARE = 0.3  # of ORU^R01 messages
DIAGNOSES = [("J18.9", "Pneumonia, unspecified organism"), ("B05.9", "Measles without complication"),
             ("I21.9", "Acute myocardial infarction, unspecified"), ("E11.9", "Type 2 diabetes mellitus"),
             ("R50.9", "Fever, unspecified"), ("U07.1", "COVID-19")]  # fmt: skip
DIAGNOSIS_TYPES = ["ADMITTING", "WORKING", "FINAL"]
POINTS_OF_CARE = [f"ED{i:02d}" for i in range(1, 9)] + ["ICU1", "MED4", "SURG2"]
ED_POINTS_OF_CARE = [p for p in POINTS_OF_CARE if p.startswith("ED")]
# ED journeys: (code, chief complaint, respiratory?) and the admitting diagnoses that go with each kind
CHIEF_COMPLAINTS = [("CC-COUGH", "Cough", True), ("CC-SOB", "Shortness of breath", True), ("CC-FEVER", "Fever", True),
                    ("CC-ANKLE", "Ankle injury", False), ("CC-LAC", "Laceration", False)]  # fmt: skip
RESPIRATORY_DIAGNOSES = [("J18.9", "Pneumonia, unspecified organism"), ("R05.9", "Cough, unspecified"),
                         ("R06.02", "Shortness of breath")]  # fmt: skip
OTHER_DIAGNOSES = [("S93.401A", "Sprain of ankle, initial encounter"), ("S01.81XA", "Laceration of head")]
CHEST_XRAY = "71046^XR Chest 2 Views^CPT"


def segment(name: str, fields: dict[int, str]) -> str:
    """One segment from {field number: value}. MSH numbering counts the field separator as MSH-1."""
    if name == "MSH":
        top = max(fields, default=2)
        return "MSH|" + ENCODING + "".join("|" + fields.get(i, "") for i in range(3, top + 1))
    top = max(fields, default=0)
    return name + "".join("|" + fields.get(i, "") for i in range(1, top + 1))


def message(segments: list[str]) -> str:
    return "\r".join(segments)


def _flag(v: float, lo: float, hi: float) -> str:
    if v > hi * 1.25:
        return "HH"
    if v < lo * 0.75:
        return "LL"
    return "H" if v > hi else "L" if v < lo else "N"


def build(n: int, rng: random.Random, bad_ratio: float = 0.01, patients: int = 50_000) -> tuple[str, str]:
    """Return (key, message) for message number n. Key = patient MRN, so a patient's messages stay ordered."""
    mrn = f"MRN{rng.randint(1, patients):06d}"
    if rng.random() < bad_ratio:
        return mrn, rng.choice(["not an hl7 message", segment("PID", {1: "1", 3: mrn})])
    kind = rng.choices([t for t, _ in MESSAGE_MIX], weights=[w for _, w in MESSAGE_MIX])[0]
    fac = rng.choice(FACILITIES)
    patient_class = "E" if kind in ("ADT^A04", "ADT^A01") else "I"
    diagnoses = None
    if kind in ("ADT^A01", "ADT^A04", "ADT^A08"):
        diagnoses = [(c, nm, rng.choice(DIAGNOSIS_TYPES)) for c, nm in rng.sample(DIAGNOSES, rng.randint(1, 2))]
    pv1 = {2: patient_class, 3: f"{rng.choice(POINTS_OF_CARE)}^01^A^{fac}", 19: f"V{n:09d}"}
    return mrn, _message(n, rng, kind, mrn, fac, pv1, diagnoses=diagnoses, vitals=rng.random() < VITALS_SHARE)


def _message(n: int, rng: random.Random, kind: str, mrn: str, fac: str, pv1: dict[int, str], pv2: dict[int, str] = None,
             diagnoses: list[tuple[str, str, str]] = None, vitals: bool = False, radiology: bool = False,
             order: str = None, note: list[str] = None) -> str:  # fmt: skip
    """One message of `kind` for a patient and visit (PV1 fields beyond set ID and admit time are the caller's)."""
    now = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    family = kind.split("^")[0]
    segs = [
        segment(
            "MSH",
            {
                3: "EPIC",
                4: fac,
                5: "DATABRICKS",
                6: "DBX",
                7: now,
                9: f"{kind}^{STRUCTURE[family]}",
                10: f"CTRL{n:010d}",
                11: "P",
                12: "2.5.1",
                13: str(n),
            },
        ),  # fmt: skip
        segment(
            "PID",
            {
                1: "1",
                3: f"{mrn}^^^EPIC^MR",
                5: "DOE^JANE",
                7: "19800101",
                8: rng.choice("FM"),
                10: "2106-3^White^CDCREC",
                18: f"ACCT{n:09d}^^^EPIC^AN",
                22: "2186-5^Not Hispanic or Latino^CDCREC",
            },
        ),  # fmt: skip
        segment("PV1", {1: "1", 44: now, **pv1}),
    ]
    if pv2:
        segs.append(segment("PV2", pv2))
    if family in ("ADT", "MDM"):
        segs.insert(1, segment("EVN", {1: kind.split("^")[1], 2: now}))
    for i, (code, name, kind_) in enumerate(diagnoses or [], 1):
        segs.append(segment("DG1", {1: str(i), 3: f"{code}^{name}^ICD-10-CM", 5: now, 6: kind_}))
    order = order or f"ORD{n}"
    if kind == "ORU^R01" and radiology:
        segs += [segment("ORC", {1: "RE", 2: order}), segment("OBR", {1: "1", 2: order, 4: CHEST_XRAY, 7: now, 24: "RAD", 25: "F"}),
                 segment("OBX", {1: "1", 2: "TX", 3: "IMP^Impression^L", 5: rng.choice(
                     ["Right lower lobe consolidation.", "No acute cardiopulmonary process."]), 11: "F", 14: now})]  # fmt: skip
    elif kind == "ORU^R01":
        panel = "VITALS^Vital Signs^FLOWSHEET" if vitals else "80048^Basic Metabolic Panel^CPT"
        segs.append(segment("OBR", {1: "1", 2: order, 4: panel, 7: now}))
        rows = [(f"{c}^{nm}^FLOWSHEET", u, lo, hi) for c, nm, u, lo, hi in VITALS] if vitals else [
            (f"{local}^{nm}^LAB^{c}^{nm}^LN", u, lo, hi) for c, nm, u, lo, hi, local in LABS]  # fmt: skip
        for i, (code, units, lo, hi) in enumerate(rows, 1):
            v = round(rng.uniform(lo * 0.6, hi * 1.4), 1)
            segs.append(
                segment(
                    "OBX",
                    {
                        1: str(i),
                        2: "NM",
                        3: code,
                        5: str(v),
                        6: units,
                        7: f"{lo}-{hi}",
                        8: _flag(v, lo, hi),
                        11: "F",
                        14: now,
                    },
                )  # fmt: skip
            )
    elif kind == "ORM^O01":
        test = {4: CHEST_XRAY, 24: "RAD"} if radiology else {4: "85025^CBC^CPT"}
        segs += [segment("ORC", {1: "NW", 2: order}), segment("OBR", {1: "1", 2: order, **test})]
    elif kind == "MDM^T02":
        segs.append(segment("TXA", {1: "1", 2: "ED^ED Provider Note" if note else "DS", 4: now, 12: f"DOC{n}",
                                    17: rng.choice(["AU", "IP", "DI"])}))  # fmt: skip
        for i, line in enumerate(note or [], 1):
            segs.append(segment("OBX", {1: str(i), 2: "TX", 3: "NOTE^Note text^L", 5: line, 11: "F"}))
    return message(segs)


@dataclass(order=True)
class _Journey:
    next_at: float
    mrn: str = field(compare=False)
    steps: list = field(compare=False)  # remaining (kind, options) steps


class Journeys:
    """Coherent ED encounters (see the module docstring), `per_minute` new ones a minute, each step `gap_s` seconds
    after the last. due(n, now) returns the next due step's (key, message), or None."""

    def __init__(self, rng: random.Random, per_minute: float = 6, gap_s: tuple[float, float] = (10, 40),
                 patients: int = 50_000):  # fmt: skip
        self._rng, self._every, self._gap = rng, 60.0 / per_minute if per_minute else None, gap_s
        self._patients, self._started, self._next_start, self._heap = patients, 0, None, []

    def _start(self, now: float) -> None:
        rng = self._rng
        self._started += 1
        code, complaint, respiratory = rng.choice(CHIEF_COMPLAINTS)
        dx = rng.choice(RESPIRATORY_DIAGNOSES if respiratory else OTHER_DIAGNOSES)
        mrn, fac, poc = f"MRN{self._patients + self._started:06d}", rng.choice(FACILITIES), rng.choice(ED_POINTS_OF_CARE)
        ed = {2: "E", 3: f"{poc}^01^A^{fac}", 4: "E", 19: f"VED{self._started:07d}"}
        order = f"RAD{self._started}"
        steps = [("ADT^A04", {"pv1": ed, "pv2": {3: f"{code}^{complaint}"}, "diagnoses": [(*dx, "ADMITTING")]}),
                 ("ADT^A08", {"pv1": ed, "pv2": {3: f"{code}^{complaint}"}}),
                 ("ORU^R01", {"pv1": ed}), ("ORU^R01", {"pv1": ed, "vitals": True})]  # fmt: skip
        if respiratory:
            steps += [("ORM^O01", {"pv1": ed, "radiology": True, "order": order}),
                      ("ORU^R01", {"pv1": ed, "radiology": True, "order": order}),
                      ("MDM^T02", {"pv1": ed, "note": [f"{complaint} since yesterday.", "Crackles at the right base."]})]  # fmt: skip
        if rng.random() < 0.5:
            steps.append(("ADT^A02", {"pv1": {**ed, 2: "I", 3: f"MED4^12^B^{fac}", 6: f"{poc}^01^A^{fac}"}}))
        else:
            steps.append(("ADT^A03", {"pv1": {**ed, 36: "01", 45: datetime.now(UTC).strftime("%Y%m%d%H%M%S")}}))
        heapq.heappush(self._heap, _Journey(now, mrn, [(fac, *s) for s in steps]))

    def due(self, n: int, now: float) -> tuple[str, str] | None:
        if self._every is not None:
            if self._next_start is None:
                self._next_start = now
            while self._next_start <= now:
                self._start(self._next_start)
                self._next_start += self._every
        if not self._heap or self._heap[0].next_at > now:
            return None
        j = heapq.heappop(self._heap)
        (fac, kind, opts), rest = j.steps[0], j.steps[1:]
        if rest:
            heapq.heappush(self._heap, _Journey(j.next_at + self._rng.uniform(*self._gap), j.mrn, rest))
        return j.mrn, _message(n, self._rng, kind, j.mrn, fac, **opts)


def main() -> None:
    from confluent_kafka import Producer
    from databricks.sdk.runtime import dbutils

    p = argparse.ArgumentParser(description=__doc__)
    for a in ["--namespace", "--service-credential", "--topic"]:
        p.add_argument(a, required=True)
    p.add_argument("--rate", type=float, default=50, help="messages per second")
    p.add_argument("--bad-ratio", type=float, default=0.01)
    p.add_argument("--patients", type=int, default=50_000, help="size of the synthetic patient population")
    p.add_argument("--journeys-per-minute", type=float, default=6, help="new ED journeys a minute (0: none)")
    args = p.parse_args()

    credential = dbutils.credentials.getServiceCredentialsProvider(args.service_credential)
    producer = Producer(eventhubs.producer_config(args.namespace, credential))
    rng, n, interval = random.Random(), 0, 1.0 / args.rate
    journeys = Journeys(rng, args.journeys_per_minute, patients=args.patients)
    next_at = time.monotonic()
    while True:
        key, msg = journeys.due(n, time.monotonic()) or build(n, rng, args.bad_ratio, args.patients)
        producer.produce(args.topic, key=key, value=msg, headers={"sent_ms": str(int(time.time() * 1000))})
        producer.poll(0)
        n += 1
        if n % 10_000 == 0:
            print(f"produced {n:,} messages to {args.topic}", flush=True)
        next_at += interval
        time.sleep(max(0.0, next_at - time.monotonic()))


if __name__ == "__main__":
    main()
