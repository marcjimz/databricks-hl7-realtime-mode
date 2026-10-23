"""Sample HL7 v2.5.1 messages for tests, built by field number so positions can't drift."""

from hl7rt.generators.hl7v2 import message, segment

# A hand-written golden message: the builder is checked against it in test_hl7v2.py.
GOLDEN_ADT_A04 = "\r".join(
    [
        "MSH|^~\\&|EPIC|FAC01|DATABRICKS|DBX|20261023083015||ADT^A04^ADT_A01|CTRL0001|P|2.5.1|42",
        "PID|1||MRN00042^^^EPIC^MR||DOE^JANE||19800101|F",
    ]
)

MSH = {3: "EPIC", 4: "FAC01", 5: "DATABRICKS", 6: "DBX", 7: "20261023083015", 11: "P", 12: "2.5.1"}

PID = segment(
    "PID",
    {
        1: "1",
        3: "MRN00042^^^EPIC^MR~999999999^^^SSA^SS",
        4: "ALT77^^^LEGACY^PI",
        5: "DOE^JANE^Q",
        7: "19800101",
        8: "F",
        10: "2106-3^White^CDCREC",
        18: "ACCT0001^^^EPIC^AN",
        22: "2186-5^Not Hispanic or Latino^CDCREC",
    },
)
PV1 = segment("PV1", {1: "1", 2: "E", 3: "ED^01^A^FAC01", 19: "V000000042", 44: "20261023080000"})


def adt_a04(control_id: str = "CTRL0001") -> str:
    return message(
        [segment("MSH", {**MSH, 9: "ADT^A04^ADT_A01", 10: control_id, 13: "42"}), segment("EVN", {1: "A04"}), PID, PV1]
    )


def oru_r01(control_id: str = "CTRL0002") -> str:
    return message(
        [
            segment("MSH", {**MSH, 9: "ORU^R01^ORU_R01", 10: control_id}),
            PID,
            PV1,
            segment("ORC", {1: "RE", 2: "ORD1"}),
            segment("OBR", {1: "1", 2: "ORD1", 4: "80048^Basic Metabolic Panel^CPT^BMP^BMP^L", 7: "20261023081500"}),
            segment(
                "OBX",
                {
                    1: "1",
                    2: "NM",
                    3: "2823-3^Potassium^LN^K^Potassium^L",
                    5: "6.8",
                    6: "mmol/L",
                    7: "3.5-5.1",
                    8: "HH",
                    11: "F",
                    14: "20261023082000",
                },
            ),  # fmt: skip
            segment(
                "OBX",
                {1: "2", 2: "NM", 3: "2345-7^Glucose^LN", 5: "92", 6: "mg/dL", 7: "70-99", 8: "N", 11: "F"},
            ),
        ]
    )


def orm_o01(control_id: str = "CTRL0003") -> str:
    return message(
        [
            segment("MSH", {**MSH, 9: "ORM^O01^ORM_O01", 10: control_id}),
            PID,
            PV1,
            segment("ORC", {1: "NW", 2: "ORD9"}),
            segment("OBR", {1: "1", 2: "ORD9", 4: "85025^Complete Blood Count^CPT^CBC^CBC^L"}),
        ]
    )


def mdm_t02(control_id: str = "CTRL0004") -> str:
    return message(
        [
            segment("MSH", {**MSH, 9: "MDM^T02^MDM_T02", 10: control_id}),
            segment("EVN", {1: "T02"}),
            PID,
            PV1,
            segment("TXA", {1: "1", 2: "DS", 12: "DOC1", 17: "AU"}),
        ]
    )
