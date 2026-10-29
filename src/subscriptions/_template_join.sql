-- Subscription template (SQL join): event A, then event B for the same patient within a time window.
-- `make sub-new NAME=<name> MODE=rtm|microbatch JOIN=1` copies it. The platform applies watermarks.
--
-- name: _template_join
-- mode: rtm
-- key: patient_id
--
-- Select each side's _meta (as _meta_<side>) so the record id covers both events.
SELECT e._meta AS _meta_registration, o._meta AS _meta_lab,
       o.patient_id, o.patient_facility, e.visit_id, o.observation_cd_desc, o.value_num, o.abnormal_flag
FROM gold_encounters e
JOIN gold_observations o
  ON e.patient_id = o.patient_id
 AND o.msg_dt_tm BETWEEN e.msg_dt_tm AND e.msg_dt_tm + INTERVAL 60 MINUTES
WHERE e.event = 'A04' AND e.patient_class = 'E' AND o.abnormal_flag IN ('HH', 'LL')
