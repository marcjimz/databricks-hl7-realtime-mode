-- The Power BI dashboard's data: small, pre-aggregated views in ${catalog}.powerbi, read with DirectQuery (one query
-- per visual per page refresh). Each reads a short window of the lakehouse mirror's streaming tables
-- (${catalog}.${schema}) or the registry's monitor views (${registry_catalog}.subscription_monitor), the same queries as
-- the AI/BI dashboards (src/dashboards/*.lvdash.json). The read-only Power BI service principal may read these views
-- only. Rendered and applied by infra/powerbi.py (`make powerbi`); statements are separated by ";\n\n".

CREATE SCHEMA IF NOT EXISTS ${catalog}.powerbi COMMENT 'Views the live Power BI dashboard reads (make powerbi)';

-- Hot path -------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.powerbi.hop_latency COMMENT 'Per-hop latency, last 15 minutes' AS
SELECT hop, count(*) AS messages,
  round(percentile_approx(hop_ms, 0.5)) AS p50_ms, round(percentile_approx(hop_ms, 0.95)) AS p95_ms,
  round(percentile_approx(hop_ms, 0.99)) AS p99_ms, round(max(hop_ms)) AS max_ms,
  round(avg(CASE WHEN hop_ms < 200 THEN 1.0 ELSE 0.0 END), 4) AS under_200ms
FROM (SELECT '1 raw → bronze' AS hop, kafka_timestamp AS ts, hop_ms FROM ${catalog}.${schema}.bronze_hl7
  UNION ALL SELECT '2 bronze → silver', kafka_timestamp, hop_ms FROM ${catalog}.${schema}.silver_messages)
WHERE ts >= timestampadd(MINUTE, -15, current_timestamp()) AND hop_ms < 600000
GROUP BY hop;

CREATE OR REPLACE VIEW ${catalog}.powerbi.hop_p99_by_minute COMMENT 'Per-hop p99 by minute, last hour' AS
SELECT hop, date_trunc('MINUTE', ts) AS minute, round(percentile_approx(hop_ms, 0.99)) AS p99_ms, count(*) AS messages
FROM (SELECT '1 raw → bronze' AS hop, kafka_timestamp AS ts, hop_ms FROM ${catalog}.${schema}.bronze_hl7
  UNION ALL SELECT '2 bronze → silver', kafka_timestamp, hop_ms FROM ${catalog}.${schema}.silver_messages)
WHERE ts >= timestampadd(MINUTE, -60, current_timestamp()) AND hop_ms < 600000
GROUP BY hop, date_trunc('MINUTE', ts);

CREATE OR REPLACE VIEW ${catalog}.powerbi.headline COMMENT 'Headline numbers, last 15 minutes' AS
SELECT
  (SELECT round(count(*) / 900.0, 1) FROM ${catalog}.${schema}.raw_hl7
    WHERE kafka_timestamp >= timestampadd(MINUTE, -15, current_timestamp())) AS msgs_per_sec,
  (SELECT round(percentile_approx(e2e_ms, 0.99)) FROM ${catalog}.${schema}.silver_observations
    WHERE kafka_timestamp >= timestampadd(MINUTE, -15, current_timestamp()) AND e2e_ms < 600000) AS raw_to_silver_p99_ms,
  (SELECT count(*) FROM ${catalog}.${schema}.silver_observations
    WHERE kafka_timestamp >= timestampadd(MINUTE, -15, current_timestamp())) AS observations,
  (SELECT count(*) FROM ${catalog}.${schema}.quarantine_hl7
    WHERE kafka_timestamp >= timestampadd(MINUTE, -15, current_timestamp())) AS quarantined;

-- Change detection: one cheap value that moves whenever new silver data lands. Power BI polls it and refreshes the page
-- only when it changed.
CREATE OR REPLACE VIEW ${catalog}.powerbi.last_update COMMENT 'Newest silver observation (page-refresh change detection)' AS
SELECT max(kafka_timestamp) AS updated_at FROM ${catalog}.${schema}.silver_observations
WHERE kafka_timestamp >= timestampadd(MINUTE, -5, current_timestamp());

-- Clinical activity ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.powerbi.messages_by_minute COMMENT 'Parsed messages per minute by type, last hour' AS
SELECT date_trunc('MINUTE', kafka_timestamp) AS minute, coalesce(msg_type, '(none)') AS msg_type, count(*) AS messages
FROM ${catalog}.${schema}.silver_messages
WHERE kafka_timestamp >= timestampadd(MINUTE, -60, current_timestamp())
GROUP BY date_trunc('MINUTE', kafka_timestamp), coalesce(msg_type, '(none)');

CREATE OR REPLACE VIEW ${catalog}.powerbi.critical_labs_by_facility COMMENT 'Critical lab results (HH/LL), last hour' AS
SELECT coalesce(patient_facility, '(none)') AS facility, observation_cd_desc AS observation, abnormal_flag,
  count(*) AS results
FROM ${catalog}.${schema}.silver_observations
WHERE abnormal_flag IN ('HH', 'LL') AND kafka_timestamp >= timestampadd(MINUTE, -60, current_timestamp())
GROUP BY coalesce(patient_facility, '(none)'), observation_cd_desc, abnormal_flag;

CREATE OR REPLACE VIEW ${catalog}.powerbi.latest_critical_labs COMMENT 'The 50 newest critical lab results' AS
SELECT kafka_timestamp AS silver_at, patient_id, patient_facility AS facility, observation_cd_desc AS observation,
  value_num, units, abnormal_flag, round(e2e_ms) AS raw_to_silver_ms
FROM ${catalog}.${schema}.silver_observations
WHERE abnormal_flag IN ('HH', 'LL') AND kafka_timestamp >= timestampadd(MINUTE, -15, current_timestamp())
ORDER BY kafka_timestamp DESC LIMIT 50;

CREATE OR REPLACE VIEW ${catalog}.powerbi.quarantine_reasons COMMENT 'Quarantined messages by reason, last hour' AS
SELECT reason, count(*) AS messages FROM ${catalog}.${schema}.quarantine_hl7
WHERE kafka_timestamp >= timestampadd(MINUTE, -60, current_timestamp())
GROUP BY reason;

-- Subscriptions --------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.powerbi.subscriptions COMMENT 'Subscriptions that are not cancelled, by app' AS
SELECT app_name, subscription, revision, source, state, rule, registered_at, confirmed_at, seconds_to_confirm
FROM ${registry_catalog}.subscription_monitor.subscriptions WHERE state <> 'CANCELLED';

CREATE OR REPLACE VIEW ${catalog}.powerbi.hub COMMENT 'Subscription hub heartbeat per silver source' AS
SELECT source, CASE WHEN seconds_since_last_batch > 120 THEN 'STALE' ELSE 'OK' END AS status,
  seconds_since_last_batch, last_batch_at, rules
FROM ${registry_catalog}.subscription_monitor.hub;
