-- Subscription template (SQL). `make sub-new NAME=<name> MODE=rtm|microbatch` copies it; edit, then deploy.
-- Results go to the topic and table <catalog>.<schema>.sub_<name>.
--
-- name: _template
-- mode: rtm
-- key: patient_id
--
-- Query gold_encounters (one row per ADT event), gold_observations (one per lab result), or gold_orders.
-- Always SELECT _meta: it carries the record id and timestamps used for upserts and latency.
SELECT _meta, patient_id, visit_id, patient_facility, observation_cd_desc, value_num, abnormal_flag
FROM gold_observations
WHERE abnormal_flag IN ('HH', 'LL')
