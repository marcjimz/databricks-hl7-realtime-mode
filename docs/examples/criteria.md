# Example: an existing subscription system's criteria, as Databricks SQL rules

A customer's current subscription service defines each subscription's matching criteria in Java with a `CriteriaBuilder`,
which serializes them to XML of XPath tests over a flattened record set. Around 200 applications use it, each with rules
like the ones below. This page keeps their shape, maps them to this repo's rules (`SELECT ... FROM <silver source>
WHERE ...`), and lists what to watch when converting. Hostnames, credentials, people, and patient IDs are replaced with
placeholders. Codes are public (ICD-10-CM, LOINC) or synthetic.

## The Java builder (sanitized)

```java
RestfulServiceClient client = (RestfulServiceClient) clientFactory.getServiceClient(RestfulServiceClientImp.class, true);
SubscriptionDefinition sd = client.subscriptionDefinition(subscription_id);
Subscription subscription = Subscription.fromDefinition(sd, subscriber_id, subscribing_from_application);
Properties alertProperties = sd.toProperties();
String subscriptionCriteria =
    CriteriaBuilder
        .criteria(SqlDataSet.class, "dataModified")
        .or()
            .and()
                .subjectAndResource(Type.PATIENT, ADMIT_DATA)                     // message types
                .addCondition(POINT_OF_CARE_FIELD, Operator.IN, POINT_OF_CARE)
                .addCondition("diagnosis_cd", Operator.EQ, "B05.9")
                .addCondition("diagnosis_cd_system", Operator.EQ, "ICD-10-CM")
            .and()
            .and()
                .subjectAndResource(Type.PATIENT, CHIEF_COMPLAINT_DATA)
                .addCondition(CHIEF_COMPLAINT_DATA_TYPE_FIELD, Operator.EQ, CHIEF_COMPLAINT_DATA_TYPE)
                .addCondition(POINT_OF_CARE_FIELD, Operator.IN, POINT_OF_CARE)
            .and()
        .or()
        .route(alertProperties)
        .toApplication("rest-hook-endpoint")
            .endPoint(DestinationSpecification.RestHook, "<key>", "https://<host>/rest-hook-endpoint/callback")
            .payload(Payload.Data)
            .addMessageHeader("Authorization", "Bearer <token>")
        .toUser()
            .clinician(subscriber_id)
            .notifyBy(DeliveryMethod.eMail, "<clinician@example.org>")
        .toApplication("ed-triage-risk-score")
            .endPoint(DestinationSpecification.AMQP, "<exchange>", "amqp://<host>:5672/<exchange>")
            .addMessageHeader(AMQP_SERVICE_USER, "<user>")
            .addMessageHeader(AMQP_SERVICE_PASSWORD, "<password>")
        .build();

subscription.setContent(subscriptionCriteria);
client.subscribe(subscription);                                              // subscribe
client.subscription(subscription_id, subscriber_id, subscribing_from_application);   // read
client.unsubscribe(subscription_id, subscriber_id, subscribing_from_application);    // unsubscribe

String[] POINT_OF_CARE = {"ED01", "ED02", "ED03", /* ~30 emergency department units */ "MED4"};
String[] ADMIT_DATA = {"ADT_A01", "ADT_A04", "ADT_A08"};
String[] CHIEF_COMPLAINT_DATA = {"ADT_A04", "ADT_A08"};
String CHIEF_COMPLAINT_DATA_TYPE_FIELD = "diagnosis_type", CHIEF_COMPLAINT_DATA_TYPE = "ADMITTING";
String LOINC_CODE_FIELD = "alt_observation_cd_system", LOINC_CODE_VALUE_FIELD = "alt_observation_cd", LOINC_CODE_SYSTEM = "LN";
String[] LOINC_CODES = {"2746-6", "1920-8", "2069-3", "2028-9", "6768-6", /* ~60 codes */ "1751-7"};
String FLOW_SHEET_CODE_FIELD = "observation_cd_system", FLOW_SHEET_CODE_VALUE_FIELD = "observation_cd";
String FLOW_SHEET_CODES_SYSTEM = "FLOWSHEET";
String[] FLOW_SHEET_CODES = {"574 TEMP", "571 BP", "572 PULSE", "573 RESP", "569 SPO2", /* ~17 rows */ "543 FIO2"};

enum Operator { EQ("="), NEQ("!="), LE("<="), LT("<"), GE(">="), GT(">"),
                BEGINS_WITH("starts-with(@attribute,'value')"), ENDS_WITH("ends-with(@attribute,'value')"),
                BETWEEN(" between "), LIKE("contains(@attribute,'value')"),
                IN(" @attribute = value_1 or @attribute = value_2 "),
                NOT_IN(" @attribute != value_1 or @attribute != value_2 ") }
```

## The XML it serializes to

```xml
<or>
  <and>
    <content-class>...SqlDataSet</content-class>
    <xpath>.//schema/string/text() = 'PATIENT' and (.//table[(@name='ORU_R01' or @name='-1')])</xpath>
    <xpath>.//column[@name = 'patient_id' and (@value = 'MRN000123' or @value = 'MRN000456' or ...)]</xpath>
    <or>
      <and>
        <xpath>.//column[@name = 'alt_observation_cd_system' and (@value = 'LN')]</xpath>
        <xpath>.//column[@name = 'alt_observation_cd' and (@value = '1920-8' or @value = '20570-8' or ...)]</xpath>
      </and>
      <and>
        <xpath>.//column[@name = 'observation_cd_system' and (@value = 'FLOWSHEET')]</xpath>
        <xpath>.//column[@name = 'observation_cd' and (@value = '571 BP' or @value = '572 PULSE' or ...)]</xpath>
      </and>
    </or>
  </and>
  <and>
    <content-class>...SqlDataSet</content-class>
    <xpath>.//schema/string/text() = 'PATIENT' and (.//table[@name='ADT_A04'])</xpath>
    <xpath>.//column[@name = 'patient_class' and (@value = '103')]</xpath>
    <xpath>.//column[@name = 'patient_point_of_care' and (@value = '-1' or @value = 'ED01' or ...)]</xpath>
    <xpath>.//column[@name = 'msg_seq' and @value = '0']</xpath>
  </and>
</or>
```

## The same rules here

Each `<and>` group names message types (`table/@name`, e.g. `ADT_A04` = our `msg_type` `ADT^A04`) plus conditions on
columns. The column names are the Epic silver contract's, which are this repo's silver column names. A group's
conditions all live on one silver entity row (every row carries its message's patient, visit, and header fields), so
a whole criteria tree becomes one rule on one source:

| Their criteria | Silver source | Our rule |
|---|---|---|
| Admits / registrations / updates at ED points of care with a diagnosis, or admitting chief complaints | `silver_diagnoses` | `SELECT patient_id, diagnosis_cd, diagnosis_type, patient_point_of_care FROM silver_diagnoses WHERE patient_point_of_care IN ('ED01', 'ED02', ...) AND ((msg_type IN ('ADT^A01', 'ADT^A04', 'ADT^A08') AND diagnosis_cd = 'B05.9' AND diagnosis_cd_system = 'ICD-10-CM') OR (msg_type IN ('ADT^A04', 'ADT^A08') AND diagnosis_type = 'ADMITTING'))` |
| A patient list's labs (LOINC) or vitals (flowsheet rows) | `silver_observations` | `SELECT patient_id, observation_cd, alt_observation_cd, value_num, units FROM silver_observations WHERE msg_type = 'ORU^R01' AND patient_id IN ('MRN000123', ...) AND ((alt_observation_cd_system = 'LN' AND alt_observation_cd IN ('1920-8', '20570-8', ...)) OR (observation_cd_system = 'FLOWSHEET' AND observation_cd IN ('571 BP', '572 PULSE', ...)))` |
| ED registrations of one patient class, first in sequence | `silver_encounters` | `SELECT patient_id, patient_point_of_care FROM silver_encounters WHERE msg_type = 'ADT^A04' AND patient_class = '103' AND patient_point_of_care IN ('ED01', ...) AND msg_seq = '0'` |

| Criteria operator | Databricks SQL |
|---|---|
| `EQ` / `NEQ` / `LT` / `LE` / `GT` / `GE` | `=` / `<>` / `<` / `<=` / `>` / `>=` |
| `IN` | `col IN (...)` |
| `NOT_IN` | `col NOT IN (...)` |
| `BETWEEN` | `col BETWEEN a AND b` |
| `BEGINS_WITH` / `ENDS_WITH` / `LIKE` (contains) | `startswith(col, 'x')` / `endswith(col, 'x')` / `contains(col, 'x')` |

## What to watch when converting

- **`NOT_IN` is generated wrong.** `@a != v1 or @a != v2` is true for every value once the list has two different
  values, so today those conditions match everything. Translate to `NOT IN`, and confirm with the rule's owner which
  behavior they rely on.
- **Lists have stray spaces and repeats.** Several LOINC entries are `" 10466-1"` with a leading space, which never
  equals a code. Trim when converting, and drop duplicates (`2069-3`, `763-3`, `704-7`, ... appear twice).
- **`'-1'` is a sentinel in their data** (in `table/@name` and in `patient_point_of_care`), probably "unknown". Its
  meaning has to be confirmed. It's either a real value to match or `IS NULL` here.
- **`.//column[...]` matches across the whole record set.** In XPath, each test is true if *any* row has that value, so
  "system = LN" and "code IN (...)" can match on two different OBX segments of one result. Here each condition holds on
  the same observation row, which is almost certainly what the author meant. Flag rules where it isn't.
- **NULLs:** SQL `<>` and `NOT IN` don't match a missing value; check rules that depend on absent columns.
- **Delivery isn't part of a rule.** Routing to REST hooks, email, and AMQP is the existing service's job. Here each
  app receives its matches on its own Event Hub (docs/integration.md), and anything that fans out from there is the
  app's.
- **Their shape is ours, apart from OR.** Long `IN` lists, `=`, `<>`, ranges, and an OR of AND groups are all plain
  Databricks SQL that the Subscription API accepts today.
