-- Subscription registry (Lakebase Postgres). Idempotent: infra/registry.py applies it on every `make registry`.
--
-- Only the Subscription API writes here (src/subscription_api, a Databricks App, as role `subscription_service`). For
-- each call it switches to the calling app's role (SET LOCAL ROLE), so the owner trigger and row-level security below
-- see the app, not the service. App roles are NOLOGIN and granted only to `subscription_service`: an app can't connect
-- or use the Lakebase Data API, so the API is the only way in. The hub pipeline reads subscription_registry and writes
-- status + heartbeat.
--
-- A rule is Databricks SQL, compiled by Databricks against the app's own views when it is registered:
--   query: SELECT <fields> FROM <source> WHERE <where_sql>    (canonical; paste it into the SQL editor)
-- Domain errors raised here: APP_NOT_ONBOARDED, SOURCE_DENIED, FIELDS_DENIED, NOT_FOUND_OR_INACCESSIBLE, OWNERSHIP_DENIED.

DO $$ BEGIN CREATE ROLE subscriber NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE subscription_service NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE SCHEMA IF NOT EXISTS subscription_registry;
CREATE SCHEMA IF NOT EXISTS subscription_api;  -- functions only

-- Platform-owned onboarding: who each app is, what it may read, and where its notifications go.
CREATE TABLE IF NOT EXISTS subscription_registry.apps (
  app_role          text PRIMARY KEY,  -- the app's NOLOGIN Postgres role (app_<name>)
  app_name          text NOT NULL UNIQUE,
  destination_topic text NOT NULL,     -- the app's own Event Hub
  allowed_sources   text[] NOT NULL,
  allowed_fields    text[] NOT NULL    -- readable and filterable columns (also the columns of its UC views)
);
ALTER TABLE subscription_registry.apps ADD COLUMN IF NOT EXISTS service_principal text UNIQUE;  -- application ID

CREATE SEQUENCE IF NOT EXISTS subscription_registry.generation;

CREATE TABLE IF NOT EXISTS subscription_registry.subscriptions (
  subscription_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  owner           text NOT NULL,
  name            text NOT NULL CHECK (name ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
  revision        int  NOT NULL DEFAULT 1,
  enabled         bool NOT NULL DEFAULT true,
  source          text NOT NULL,
  query           text NOT NULL,
  where_sql       text,              -- NULL: every record
  fields          text[] NOT NULL,
  generation      bigint NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (owner, name)
);
CREATE INDEX IF NOT EXISTS subscriptions_generation ON subscription_registry.subscriptions (generation);

-- Migration from structured predicates (2026-11): their owners were the old SP-named roles, so the rows go; apps
-- re-register their SQL rules on their next run.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'subscription_registry' AND table_name = 'subscriptions' AND column_name = 'predicate') THEN
    DROP VIEW IF EXISTS subscription_monitor.subscriptions;  -- recreated below
    DELETE FROM subscription_registry.subscriptions;
    ALTER TABLE subscription_registry.subscriptions DROP COLUMN predicate, DROP COLUMN predicate_sql,
      ADD COLUMN query text NOT NULL, ADD COLUMN where_sql text;
  END IF;
END $$;
DROP FUNCTION IF EXISTS subscription_api.subscribe(text, text, jsonb, text[]);
DROP FUNCTION IF EXISTS subscription_registry.render(jsonb, text, text[], int);
DROP FUNCTION IF EXISTS subscription_registry.literal(jsonb, text, text);
DROP TABLE IF EXISTS subscription_registry.source_columns;

-- Written by the hub: the revision it actually runs.
CREATE TABLE IF NOT EXISTS subscription_registry.status (
  subscription_id uuid PRIMARY KEY REFERENCES subscription_registry.subscriptions ON DELETE CASCADE,
  revision        int  NOT NULL,
  state           text NOT NULL CHECK (state IN ('ACTIVE', 'CANCELLED', 'FAILED')),
  error           text,
  observed_at     timestamptz NOT NULL DEFAULT now()
);

-- Written by the hub every micro-batch, so apps can tell a quiet stream from a stopped hub.
CREATE TABLE IF NOT EXISTS subscription_registry.hub_heartbeat (
  source        text PRIMARY KEY,
  last_batch_at timestamptz NOT NULL,
  batch_id      bigint NOT NULL,
  rules         int NOT NULL
);

-- ---------------------------------------------------------------------------------------------------------------------
-- Every write to subscriptions goes through here: the owner is the acting role (the app, never the service), the app
-- must be onboarded and granted the source and fields, and revision/generation are server-assigned.
CREATE OR REPLACE FUNCTION subscription_registry.stamp() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE app subscription_registry.apps;
BEGIN
  IF TG_OP = 'UPDATE' AND (NEW.owner <> OLD.owner OR NEW.name <> OLD.name OR NEW.subscription_id <> OLD.subscription_id) THEN
    RAISE EXCEPTION 'OWNERSHIP_DENIED: owner, name, and id are immutable' USING ERRCODE = '42501';
  END IF;
  NEW.owner := current_user;
  SELECT * INTO app FROM subscription_registry.apps WHERE app_role = current_user;
  IF NOT FOUND THEN RAISE EXCEPTION 'APP_NOT_ONBOARDED: % has no app binding', current_user USING ERRCODE = '42501'; END IF;
  IF NEW.enabled THEN
    IF NOT NEW.source = ANY (app.allowed_sources) THEN
      RAISE EXCEPTION 'SOURCE_DENIED: % is not granted to this app', NEW.source USING ERRCODE = '42501';
    END IF;
    IF cardinality(NEW.fields) = 0 OR NOT NEW.fields <@ app.allowed_fields THEN
      RAISE EXCEPTION 'FIELDS_DENIED: % not granted to this app',
        (SELECT coalesce(array_agg(x), '{}') FROM unnest(NEW.fields) x WHERE x <> ALL (app.allowed_fields))
        USING ERRCODE = '42501';
    END IF;
  END IF;
  NEW.generation := nextval('subscription_registry.generation');
  NEW.updated_at := now();
  NEW.revision := CASE TG_OP WHEN 'INSERT' THEN 1 ELSE OLD.revision + 1 END;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS stamp ON subscription_registry.subscriptions;
CREATE TRIGGER stamp BEFORE INSERT OR UPDATE ON subscription_registry.subscriptions
  FOR EACH ROW EXECUTE FUNCTION subscription_registry.stamp();

-- ---------------------------------------------------------------------------------------------------------------------
-- Row-level security: an app sees and writes only its own rows. The hub runs as the registry owner (bypasses RLS).
ALTER TABLE subscription_registry.apps ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.status ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS own ON subscription_registry.apps;
CREATE POLICY own ON subscription_registry.apps FOR SELECT TO subscriber USING (app_role = current_user);
DROP POLICY IF EXISTS service ON subscription_registry.apps;  -- the service maps a verified caller to its app role
CREATE POLICY service ON subscription_registry.apps FOR SELECT TO subscription_service USING (true);
DROP POLICY IF EXISTS own ON subscription_registry.subscriptions;
CREATE POLICY own ON subscription_registry.subscriptions TO subscriber
  USING (owner = current_user) WITH CHECK (owner = current_user);
DROP POLICY IF EXISTS own ON subscription_registry.status;
CREATE POLICY own ON subscription_registry.status FOR SELECT TO subscriber
  USING (subscription_id IN (SELECT subscription_id FROM subscription_registry.subscriptions));

-- ---------------------------------------------------------------------------------------------------------------------
-- API, called by the Subscription API as the app's role. SECURITY INVOKER: RLS and the trigger see the app.
CREATE OR REPLACE FUNCTION subscription_api.subscribe(p_name text, p_source text, p_query text, p_where text,
                                                      p_fields text[])
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER AS $$
DECLARE s subscription_registry.subscriptions; topic text;
BEGIN
  INSERT INTO subscription_registry.subscriptions AS t (owner, name, source, query, where_sql, fields)
    VALUES (current_user, p_name, p_source, p_query, p_where, p_fields)
    ON CONFLICT (owner, name) DO UPDATE
      SET source = excluded.source, query = excluded.query, where_sql = excluded.where_sql, fields = excluded.fields,
          enabled = true
      WHERE (t.source, t.query, t.fields, t.enabled) IS DISTINCT FROM (excluded.source, excluded.query, excluded.fields, true)
    RETURNING * INTO s;
  IF s.subscription_id IS NULL THEN  -- identical definition: no new revision
    SELECT * INTO s FROM subscription_registry.subscriptions WHERE owner = current_user AND name = p_name;
  END IF;
  SELECT destination_topic INTO topic FROM subscription_registry.apps WHERE app_role = current_user;
  RETURN jsonb_build_object('subscriptionId', s.subscription_id, 'name', s.name, 'revision', s.revision,
    'state', 'ACCEPTED', 'query', s.query, 'destination', topic);
END $$;

CREATE OR REPLACE FUNCTION subscription_api.unsubscribe(p_name text)
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER AS $$
DECLARE s subscription_registry.subscriptions;
BEGIN
  UPDATE subscription_registry.subscriptions SET enabled = false
    WHERE owner = current_user AND name = p_name AND enabled RETURNING * INTO s;
  IF s.subscription_id IS NULL THEN
    RAISE EXCEPTION 'NOT_FOUND_OR_INACCESSIBLE: no active subscription %', p_name USING ERRCODE = 'P0002';
  END IF;
  RETURN jsonb_build_object('subscriptionId', s.subscription_id, 'name', s.name, 'revision', s.revision, 'state', 'CANCELLING');
END $$;

-- The caller's subscriptions, the hub's verdict for each current revision, and how recently the hub ran per source.
CREATE OR REPLACE FUNCTION subscription_api.list_subscriptions()
RETURNS jsonb LANGUAGE sql STABLE SECURITY INVOKER AS $$
  SELECT jsonb_build_object(
    'subscriptions', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'subscriptionId', s.subscription_id, 'name', s.name, 'revision', s.revision, 'enabled', s.enabled,
        'source', s.source, 'query', s.query, 'fields', s.fields,
        'state', CASE WHEN st.revision = s.revision THEN st.state ELSE 'PENDING' END,
        'error', CASE WHEN st.revision = s.revision THEN st.error END) ORDER BY s.name)
      FROM subscription_registry.subscriptions s LEFT JOIN subscription_registry.status st USING (subscription_id)), '[]'),
    'hub', coalesce((SELECT jsonb_object_agg(source, last_batch_at) FROM subscription_registry.hub_heartbeat), '{}'))
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Platform monitoring (the "HL7 subscriptions" dashboard reads these through the registry's Unity Catalog catalog).
-- Every app's rules in one place, so it's for the platform team only: nothing here is granted to `subscriber`.
CREATE SCHEMA IF NOT EXISTS subscription_monitor;

DROP VIEW IF EXISTS subscription_monitor.subscriptions;  -- views only: dropped and recreated, so columns can change
CREATE VIEW subscription_monitor.subscriptions AS
SELECT a.app_name, a.service_principal, s.name AS subscription, s.revision, s.source,
       s.query AS rule, array_to_string(s.fields, ', ') AS fields,
       CASE WHEN st.revision = s.revision THEN st.state WHEN s.enabled THEN 'PENDING' ELSE 'CANCELLING' END AS state,
       CASE WHEN st.revision = s.revision THEN st.error END AS error,
       s.updated_at AS registered_at,
       CASE WHEN st.revision = s.revision THEN st.observed_at END AS confirmed_at,
       CASE WHEN st.revision = s.revision
            THEN round(extract(epoch FROM st.observed_at - s.updated_at)::numeric, 1)::float8 END AS seconds_to_confirm
FROM subscription_registry.subscriptions s
LEFT JOIN subscription_registry.apps a ON a.app_role = s.owner
LEFT JOIN subscription_registry.status st USING (subscription_id);

DROP VIEW IF EXISTS subscription_monitor.hub;
CREATE VIEW subscription_monitor.hub AS
SELECT source, last_batch_at, round(extract(epoch FROM now() - last_batch_at))::int AS seconds_since_last_batch,
       batch_id, rules
FROM subscription_registry.hub_heartbeat;

REVOKE ALL ON SCHEMA subscription_monitor FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA subscription_monitor FROM PUBLIC;

-- ---------------------------------------------------------------------------------------------------------------------
-- Grants. App roles get the API plus the RLS-filtered table access its INVOKER functions need. The service gets no
-- grants of its own: membership in each app role (infra/registry.py) lets it SET ROLE to its caller, and its policy on
-- apps lets it map a verified caller to that role. Nothing by default.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA subscription_api FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA subscription_registry FROM PUBLIC;
GRANT USAGE ON SCHEMA subscription_api, subscription_registry TO subscriber;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA subscription_api TO subscriber;
GRANT EXECUTE ON FUNCTION subscription_registry.stamp() TO subscriber;
GRANT SELECT ON subscription_registry.apps, subscription_registry.status, subscription_registry.hub_heartbeat TO subscriber;
GRANT SELECT, INSERT, UPDATE ON subscription_registry.subscriptions TO subscriber;
GRANT USAGE ON SEQUENCE subscription_registry.generation TO subscriber;
