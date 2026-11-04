-- Subscription registry (Lakebase Postgres). Idempotent: infra/registry.py applies it on every `make up`.
--
-- Apps call the functions in schema subscription_api through the Lakebase Data API, each as its own service principal's
-- Postgres role (member of `subscriber`). The hub pipeline reads subscription_registry and writes status + heartbeat.
--
-- A subscription's rule is a structured predicate (JSON), never SQL text:
--   {"and": [p, ...]} | {"or": [p, ...]} | {"not": p} | {"field": "<column>", "op": "<op>"[, "value": <literal>]}
-- It is validated when it is written (trigger), so `subscribe` either returns ACCEPTED or raises a domain error:
--   APP_NOT_ONBOARDED, SOURCE_DENIED, FIELDS_DENIED, FIELD_NOT_IN_SOURCE, FIELD_DENIED, OPERATOR_NOT_ALLOWED,
--   TYPE_MISMATCH, PREDICATE_INVALID, PREDICATE_TOO_COMPLEX, NOT_FOUND_OR_INACCESSIBLE, OWNERSHIP_DENIED
-- The hub builds its Spark filter from the same structure (hl7rt.subscriptions), so an accepted rule always runs.

DO $$ BEGIN CREATE ROLE subscriber NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE SCHEMA IF NOT EXISTS subscription_registry;  -- internal; never exposed by the Data API
CREATE SCHEMA IF NOT EXISTS subscription_api;       -- exposed; functions only

-- Platform-owned: the gold sources and their columns (from hl7rt.gold, written by infra/registry.py).
CREATE TABLE IF NOT EXISTS subscription_registry.source_columns (
  source      text NOT NULL,
  column_name text NOT NULL,
  data_type   text NOT NULL CHECK (data_type IN ('string', 'int', 'long', 'double', 'timestamp')),
  PRIMARY KEY (source, column_name)
);

-- Platform-owned onboarding: what each app may read and where its notifications go.
CREATE TABLE IF NOT EXISTS subscription_registry.apps (
  app_role          text PRIMARY KEY,  -- the app's Postgres role = its service principal's application ID
  app_name          text NOT NULL UNIQUE,
  destination_topic text NOT NULL,     -- the app's own Event Hub
  allowed_sources   text[] NOT NULL,
  allowed_fields    text[] NOT NULL    -- readable and filterable columns
);

CREATE SEQUENCE IF NOT EXISTS subscription_registry.generation;

CREATE TABLE IF NOT EXISTS subscription_registry.subscriptions (
  subscription_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  owner           text NOT NULL,
  name            text NOT NULL CHECK (name ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
  revision        int  NOT NULL DEFAULT 1,
  enabled         bool NOT NULL DEFAULT true,
  source          text NOT NULL,
  predicate       jsonb NOT NULL,
  predicate_sql   text NOT NULL DEFAULT '',  -- human-readable rendering; never executed
  fields          text[] NOT NULL,
  generation      bigint NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (owner, name)
);
CREATE INDEX IF NOT EXISTS subscriptions_generation ON subscription_registry.subscriptions (generation);

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
-- Predicate validation. Returns the SQL rendering; raises a domain error otherwise.
CREATE OR REPLACE FUNCTION subscription_registry.literal(v jsonb, t text, f text) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF t = 'string' AND jsonb_typeof(v) = 'string' AND length(v #>> '{}') <= 1000 THEN
    RETURN quote_literal(v #>> '{}');
  ELSIF t IN ('int', 'long') AND jsonb_typeof(v) = 'number' AND (v #>> '{}') ~ '^-?[0-9]{1,18}$' THEN
    RETURN v #>> '{}';
  ELSIF t = 'double' AND jsonb_typeof(v) = 'number' THEN
    RETURN v #>> '{}';
  ELSIF t = 'timestamp' AND jsonb_typeof(v) = 'string'
        AND (v #>> '{}') ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$' THEN
    PERFORM (v #>> '{}')::timestamptz;  -- rejects impossible dates
    RETURN 'TIMESTAMP ' || quote_literal(v #>> '{}');
  END IF;
  RAISE EXCEPTION 'TYPE_MISMATCH: % is %, got %', f, t, v USING ERRCODE = '22023';
EXCEPTION WHEN datetime_field_overflow OR invalid_datetime_format THEN
  RAISE EXCEPTION 'TYPE_MISMATCH: % is %, got %', f, t, v USING ERRCODE = '22023';
END $$;

CREATE OR REPLACE FUNCTION subscription_registry.render(p jsonb, src text, granted text[], depth int DEFAULT 1)
RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE
  keys text[]; f text; op text; t text; v jsonb; parts text[]; child jsonb;
BEGIN
  IF depth > 8 THEN RAISE EXCEPTION 'PREDICATE_TOO_COMPLEX: nested deeper than 8' USING ERRCODE = '22023'; END IF;
  IF jsonb_typeof(p) <> 'object' THEN RAISE EXCEPTION 'PREDICATE_INVALID: expected an object' USING ERRCODE = '22023'; END IF;
  keys := ARRAY(SELECT jsonb_object_keys(p) ORDER BY 1);

  IF keys = ARRAY['and'] OR keys = ARRAY['or'] THEN
    IF jsonb_typeof(p -> keys[1]) <> 'array' OR jsonb_array_length(p -> keys[1]) NOT BETWEEN 2 AND 50 THEN
      RAISE EXCEPTION 'PREDICATE_INVALID: % takes 2 to 50 predicates', keys[1] USING ERRCODE = '22023';
    END IF;
    FOR child IN SELECT * FROM jsonb_array_elements(p -> keys[1]) LOOP
      parts := parts || ('(' || subscription_registry.render(child, src, granted, depth + 1) || ')');
    END LOOP;
    RETURN array_to_string(parts, ' ' || upper(keys[1]) || ' ');
  ELSIF keys = ARRAY['not'] THEN
    RETURN 'NOT (' || subscription_registry.render(p -> 'not', src, granted, depth + 1) || ')';
  ELSIF NOT (keys = ARRAY['field', 'op'] OR keys = ARRAY['field', 'op', 'value']) THEN
    RAISE EXCEPTION 'PREDICATE_INVALID: unknown keys %', keys USING ERRCODE = '22023';
  END IF;

  f := p ->> 'field'; op := p ->> 'op'; v := p -> 'value';
  SELECT data_type INTO t FROM subscription_registry.source_columns WHERE source = src AND column_name = f;
  IF t IS NULL THEN RAISE EXCEPTION 'FIELD_NOT_IN_SOURCE: % is not a column of %', f, src USING ERRCODE = '22023'; END IF;
  -- Filtering on a column is reading it (matches reveal its value), so it must be granted too.
  IF NOT f = ANY (granted) THEN RAISE EXCEPTION 'FIELD_DENIED: % is not granted to this app', f USING ERRCODE = '42501'; END IF;

  IF op IN ('is_null', 'is_not_null') THEN
    IF v IS NOT NULL THEN RAISE EXCEPTION 'PREDICATE_INVALID: % takes no value', op USING ERRCODE = '22023'; END IF;
    RETURN format('%I IS %sNULL', f, CASE op WHEN 'is_not_null' THEN 'NOT ' ELSE '' END);
  ELSIF v IS NULL THEN
    RAISE EXCEPTION 'PREDICATE_INVALID: % needs a value', op USING ERRCODE = '22023';
  ELSIF op IN ('eq', 'ne') THEN
    RETURN format('%I %s %s', f, CASE op WHEN 'eq' THEN '=' ELSE '<>' END, subscription_registry.literal(v, t, f));
  ELSIF op IN ('lt', 'le', 'gt', 'ge') THEN
    IF t = 'string' THEN RAISE EXCEPTION 'OPERATOR_NOT_ALLOWED: % on string column %', op, f USING ERRCODE = '22023'; END IF;
    RETURN format('%I %s %s', f, CASE op WHEN 'lt' THEN '<' WHEN 'le' THEN '<=' WHEN 'gt' THEN '>' ELSE '>=' END,
                  subscription_registry.literal(v, t, f));
  ELSIF op IN ('in', 'not_in') THEN
    IF t = 'timestamp' THEN RAISE EXCEPTION 'OPERATOR_NOT_ALLOWED: % on timestamp column %', op, f USING ERRCODE = '22023'; END IF;
    IF jsonb_typeof(v) <> 'array' OR jsonb_array_length(v) NOT BETWEEN 1 AND 1000 THEN
      RAISE EXCEPTION 'PREDICATE_INVALID: % takes a list of 1 to 1000 values', op USING ERRCODE = '22023';
    END IF;
    RETURN format('%I %sIN (%s)', f, CASE op WHEN 'not_in' THEN 'NOT ' ELSE '' END,
                  (SELECT string_agg(subscription_registry.literal(e, t, f), ', ') FROM jsonb_array_elements(v) e));
  ELSIF op IN ('contains', 'starts_with', 'ends_with') THEN
    IF t <> 'string' THEN RAISE EXCEPTION 'OPERATOR_NOT_ALLOWED: % on % column %', op, t, f USING ERRCODE = '22023'; END IF;
    IF jsonb_typeof(v) <> 'string' OR length(v #>> '{}') NOT BETWEEN 1 AND 200 THEN
      RAISE EXCEPTION 'TYPE_MISMATCH: % takes a string of 1 to 200 characters', op USING ERRCODE = '22023';
    END IF;
    RETURN format('%s(%I, %s)', replace(op, '_', ''), f, quote_literal(v #>> '{}'));
  END IF;
  RAISE EXCEPTION 'OPERATOR_NOT_ALLOWED: %', op USING ERRCODE = '22023';
END $$;

-- Every write to subscriptions goes through here, including direct SQL that skips the API: owner is the authenticated
-- role, the app must be onboarded and granted, the predicate must validate, and revision/generation are server-assigned.
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
    IF (SELECT count(*) FROM jsonb_path_query(NEW.predicate, 'lax $.**') n WHERE jsonb_typeof(n) = 'object') > 100 THEN
      RAISE EXCEPTION 'PREDICATE_TOO_COMPLEX: more than 100 nodes' USING ERRCODE = '22023';
    END IF;
    NEW.predicate_sql := subscription_registry.render(NEW.predicate, NEW.source, app.allowed_fields);
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
DROP POLICY IF EXISTS own ON subscription_registry.subscriptions;
CREATE POLICY own ON subscription_registry.subscriptions TO subscriber
  USING (owner = current_user) WITH CHECK (owner = current_user);
DROP POLICY IF EXISTS own ON subscription_registry.status;
CREATE POLICY own ON subscription_registry.status FOR SELECT TO subscriber
  USING (subscription_id IN (SELECT subscription_id FROM subscription_registry.subscriptions));

-- ---------------------------------------------------------------------------------------------------------------------
-- API. SECURITY INVOKER: RLS and the trigger see the caller's own role.
CREATE OR REPLACE FUNCTION subscription_api.subscribe(p_name text, p_source text, p_predicate jsonb, p_fields text[])
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER AS $$
DECLARE s subscription_registry.subscriptions; topic text;
BEGIN
  INSERT INTO subscription_registry.subscriptions AS t (owner, name, source, predicate, fields)
    VALUES (current_user, p_name, p_source, p_predicate, p_fields)
    ON CONFLICT (owner, name) DO UPDATE
      SET source = excluded.source, predicate = excluded.predicate, fields = excluded.fields, enabled = true
      WHERE (t.source, t.predicate, t.fields, t.enabled) IS DISTINCT FROM (excluded.source, excluded.predicate, excluded.fields, true)
    RETURNING * INTO s;
  IF s.subscription_id IS NULL THEN  -- identical definition: no new revision
    SELECT * INTO s FROM subscription_registry.subscriptions WHERE owner = current_user AND name = p_name;
  END IF;
  SELECT destination_topic INTO topic FROM subscription_registry.apps WHERE app_role = current_user;
  RETURN jsonb_build_object('subscriptionId', s.subscription_id, 'name', s.name, 'revision', s.revision,
    'state', 'ACCEPTED', 'predicateSql', s.predicate_sql, 'destination', topic);
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
        'source', s.source, 'predicateSql', s.predicate_sql, 'fields', s.fields,
        'state', CASE WHEN st.revision = s.revision THEN st.state ELSE 'PENDING' END,
        'error', CASE WHEN st.revision = s.revision THEN st.error END) ORDER BY s.name)
      FROM subscription_registry.subscriptions s LEFT JOIN subscription_registry.status st USING (subscription_id)), '[]'),
    'hub', coalesce((SELECT jsonb_object_agg(source, last_batch_at) FROM subscription_registry.hub_heartbeat), '{}'))
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Grants. Apps get the API plus the RLS-filtered table access its INVOKER functions need; nothing by default.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA subscription_api FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA subscription_registry FROM PUBLIC;
GRANT USAGE ON SCHEMA subscription_api, subscription_registry TO subscriber;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA subscription_api TO subscriber;
GRANT EXECUTE ON FUNCTION subscription_registry.render(jsonb, text, text[], int),
                          subscription_registry.literal(jsonb, text, text),
                          subscription_registry.stamp() TO subscriber;
GRANT SELECT ON subscription_registry.apps, subscription_registry.status, subscription_registry.source_columns,
                subscription_registry.hub_heartbeat TO subscriber;
GRANT SELECT, INSERT, UPDATE ON subscription_registry.subscriptions TO subscriber;
GRANT USAGE ON SEQUENCE subscription_registry.generation TO subscriber;
