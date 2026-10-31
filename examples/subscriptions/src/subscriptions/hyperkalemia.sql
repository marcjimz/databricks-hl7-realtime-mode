-- Hyperkalemia: any potassium result above 6.0 mmol/L, delivered in seconds (micro-batch, serverless).
-- name: hyperkalemia
-- mode: microbatch
-- key: patient_id
SELECT _meta, patient_id, visit_id, patient_facility, value_num, abnormal_flag
FROM gold_observations
WHERE observation_cd = '2823-3'   -- potassium (LOINC)
  AND value_num > 6.0
