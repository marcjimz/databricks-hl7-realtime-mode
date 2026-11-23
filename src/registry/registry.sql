-- Subscription registry (Lakebase Postgres). Idempotent: infra/registry.py applies it on every `make registry`.
--
-- Only the Subscription API writes subscriptions (src/subscription_api, a Databricks App, as role
-- `subscription_service`). For each call it switches to the calling app's role (SET LOCAL ROLE), so the owner trigger
-- and row-level security below see the app, not the service. App roles are NOLOGIN and granted only to
-- `subscription_service`: an app can't connect or use the Lakebase Data API, so the API is the only way in.
--
-- Hubs run the rules. Every app is assigned one by the platform (apps.hub): edp_shared, which the platform deploys, or a
-- team's own hub, deployed from the hub/ bundle and serving only the apps assigned to it. A rule runs on its app's hub,
-- except a deferred one: every app's deferred rules run on the hub with runs_deferred (edp_shared), so a team's hub is
-- one realtime pipeline. Each hub reads the rules it runs and writes its own rows (status, heartbeat, progress, delivery
-- state), all keyed by hub. edp_shared runs as the registry owner (it bypasses row-level security and filters by hub
-- itself); a team's hub runs as the team's service principal, a member of hub_<name>, which row-level security holds to
-- its own apps and rows.
--
-- A rule is Databricks SQL, compiled by Databricks against the app's own views when it is registered:
--   query: SELECT <fields> FROM <source> WHERE <where_sql>    (canonical; paste it into the SQL editor)
-- Domain errors raised here: APP_NOT_ONBOARDED, SOURCE_DENIED, FIELDS_DENIED, NOT_FOUND_OR_INACCESSIBLE, OWNERSHIP_DENIED,
-- TEMPLATE_DENIED, TIER_INVALID, RULE_LIMIT_REACHED.
--
-- Hub-written tables have no column defaults for hub or tier and may have been migrated (columns appended): name every
-- column when writing.

DO $$ BEGIN CREATE ROLE subscriber NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE subscription_service NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
-- Hubs' identities: `hub` holds the grants, hub_<name> says which hub (a team's service principal is granted its
-- hub_<name>; see onboard_hub below).
DO $$ BEGIN CREATE ROLE hub NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE hub_edp_shared NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
GRANT hub TO hub_edp_shared;

CREATE SCHEMA IF NOT EXISTS subscription_registry;
CREATE SCHEMA IF NOT EXISTS subscription_api;  -- functions only

-- The hubs that run rules. edp_shared is the platform's, and the one that runs every app's deferred rules.
CREATE TABLE IF NOT EXISTS subscription_registry.hubs (
  name                 text PRIMARY KEY CHECK (name ~ '^[a-z][a-z0-9_]{1,30}$'),
  principal            text,               -- the identity its pipeline runs as (a service principal's application ID)
  eventhubs_credential text,               -- its Unity Catalog service credential (hub_<name>_eventhubs)
  max_enabled_rules    int  NOT NULL DEFAULT 2000,  -- per tier it runs (measured: docs/SCALABILITY.md)
  runs_deferred        bool NOT NULL DEFAULT false,
  created_at           timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS hubs_one_deferred ON subscription_registry.hubs ((true)) WHERE runs_deferred;
INSERT INTO subscription_registry.hubs (name, runs_deferred) VALUES ('edp_shared', true) ON CONFLICT (name) DO NOTHING;

-- Platform-owned onboarding: who each app is, what it may read, and where its notifications go.
CREATE TABLE IF NOT EXISTS subscription_registry.apps (
  app_role          text PRIMARY KEY,  -- the app's NOLOGIN Postgres role (app_<name>)
  app_name          text NOT NULL UNIQUE,
  destination_topic text NOT NULL,     -- the app's own Event Hub
  allowed_sources   text[] NOT NULL,
  allowed_fields    text[] NOT NULL    -- readable and filterable columns (also the columns of its UC views)
);
ALTER TABLE subscription_registry.apps ADD COLUMN IF NOT EXISTS service_principal text UNIQUE;  -- application ID
-- The app's Unity Catalog service credential for its hub (sub_<app>_eventhubs): the Subscription API hands it out in
-- GET /api/v1/connection, so apps don't configure it.
ALTER TABLE subscription_registry.apps ADD COLUMN IF NOT EXISTS eventhubs_credential text;
-- The hub that runs its realtime rules (the platform assigns it: infra/registry.py, from Terraform's subscriber_apps).
ALTER TABLE subscription_registry.apps ADD COLUMN IF NOT EXISTS hub text NOT NULL DEFAULT 'edp_shared'
  REFERENCES subscription_registry.hubs;

-- Data templates: the platform's named grants per workflow (a set of sources and columns within the app's grant). A
-- subscription that names one may read, filter on, and receive only those columns.
CREATE TABLE IF NOT EXISTS subscription_registry.templates (
  app_role text NOT NULL REFERENCES subscription_registry.apps ON DELETE CASCADE,
  name     text NOT NULL,
  sources  text[] NOT NULL,
  fields   text[] NOT NULL,
  PRIMARY KEY (app_role, name)
);

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
-- The data template it is held to (NULL: the app's whole grant), and its delivery tier: realtime (the clinical hub,
-- seconds) or deferred (a separate hub on a longer trigger, for work that can wait, e.g. billing).
ALTER TABLE subscription_registry.subscriptions ADD COLUMN IF NOT EXISTS template text;
ALTER TABLE subscription_registry.subscriptions ADD COLUMN IF NOT EXISTS tier text NOT NULL DEFAULT 'realtime';

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

-- Written by the hub that runs the rule: the revision it runs. PLACED: the revision is in its matcher, so it decides
-- every record from then on; ACTIVE: it has decided a batch with it. A status row from any other hub doesn't count.
CREATE TABLE IF NOT EXISTS subscription_registry.status (
  subscription_id uuid NOT NULL REFERENCES subscription_registry.subscriptions ON DELETE CASCADE,
  hub             text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  revision        int  NOT NULL,
  state           text NOT NULL CHECK (state IN ('PLACED', 'ACTIVE', 'CANCELLED', 'FAILED')),
  error           text,
  observed_at     timestamptz NOT NULL DEFAULT now(),
  placed_at       timestamptz,       -- when this revision was PLACED (kept when it becomes ACTIVE)
  PRIMARY KEY (subscription_id, hub)
);

-- Written by each hub every micro-batch, per tier and source, so apps can tell a quiet stream from a stopped hub.
CREATE TABLE IF NOT EXISTS subscription_registry.hub_heartbeat (
  hub           text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  tier          text NOT NULL,
  source        text NOT NULL,
  last_batch_at timestamptz NOT NULL,
  batch_id      bigint NOT NULL,
  rules         int NOT NULL,
  PRIMARY KEY (hub, tier, source)
);

-- Written by each hub per micro-batch: where the time goes (milliseconds per phase), for docs/SCALABILITY.md. A new
-- checkpoint numbers batches from 0 again, so a restarted hub's batches replace older rows.
CREATE TABLE IF NOT EXISTS subscription_registry.hub_progress (
  hub        text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  tier       text NOT NULL,
  source     text NOT NULL,
  batch_id   bigint NOT NULL,
  at         timestamptz NOT NULL DEFAULT now(),
  rules      int NOT NULL,
  refresh_ms int NOT NULL,  -- registry check and reload
  prepare_ms int,           -- (before the engine) time this batch waited for a compile
  compile_ms int NOT NULL DEFAULT 0,  -- what compiling the set in use cost (checks + expressions), wherever it ran
  write_ms   int,           -- (before the engine) Spark: plan, evaluate, write to Event Hubs
  report_ms  int,           -- (before the engine) status and heartbeat
  PRIMARY KEY (hub, tier, source, batch_id)
);
-- How old the batch's newest record was when the hub began on it (its broker time to the batch's start), and Spark's
-- own split of the previous batch (StreamingQueryProgress.durationMs): the time outside the phases above.
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS age_ms int;
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS spark_ms jsonb;
-- The engine's phases: read the batch (rendered fields and expression matches), match on the driver, bind the batch's
-- decisions, publish to each app's topic, and record status; then how many records, notifications, and parked ones.
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS collect_ms int;
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS match_ms int;
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS bind_ms int;
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS deliver_ms int;
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS status_ms int;
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS rows int;
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS delivered int;
ALTER TABLE subscription_registry.hub_progress ADD COLUMN IF NOT EXISTS parked int;

-- Migration to hubs (2026-11): the one shared hub's rows, keyed without a hub, become edp_shared's; its deferred tier's
-- heartbeat and progress rows (source 'deferred:<source>') get a tier of their own; the rule cap moves to hubs.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'subscription_registry' AND table_name = 'status' AND column_name = 'hub') THEN
    ALTER TABLE subscription_registry.status
      ADD COLUMN hub text NOT NULL DEFAULT 'edp_shared' REFERENCES subscription_registry.hubs ON DELETE CASCADE,
      ADD COLUMN placed_at timestamptz;
    ALTER TABLE subscription_registry.status ALTER COLUMN hub DROP DEFAULT, DROP CONSTRAINT status_pkey,
      ADD PRIMARY KEY (subscription_id, hub), DROP CONSTRAINT IF EXISTS status_state_check,
      ADD CONSTRAINT status_state_check CHECK (state IN ('PLACED', 'ACTIVE', 'CANCELLED', 'FAILED'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'subscription_registry' AND table_name = 'hub_heartbeat' AND column_name = 'hub') THEN
    ALTER TABLE subscription_registry.hub_heartbeat DROP CONSTRAINT hub_heartbeat_pkey,
      ADD COLUMN hub text NOT NULL DEFAULT 'edp_shared' REFERENCES subscription_registry.hubs ON DELETE CASCADE,
      ADD COLUMN tier text NOT NULL DEFAULT 'realtime';
    UPDATE subscription_registry.hub_heartbeat SET tier = split_part(source, ':', 1), source = split_part(source, ':', 2)
      WHERE source LIKE '%:%';
    ALTER TABLE subscription_registry.hub_heartbeat ALTER COLUMN hub DROP DEFAULT, ALTER COLUMN tier DROP DEFAULT,
      ADD PRIMARY KEY (hub, tier, source);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'subscription_registry' AND table_name = 'hub_progress' AND column_name = 'hub') THEN
    ALTER TABLE subscription_registry.hub_progress DROP CONSTRAINT hub_progress_pkey,
      ADD COLUMN hub text NOT NULL DEFAULT 'edp_shared' REFERENCES subscription_registry.hubs ON DELETE CASCADE,
      ADD COLUMN tier text NOT NULL DEFAULT 'realtime';
    UPDATE subscription_registry.hub_progress SET tier = split_part(source, ':', 1), source = split_part(source, ':', 2)
      WHERE source LIKE '%:%';
    ALTER TABLE subscription_registry.hub_progress ALTER COLUMN hub DROP DEFAULT, ALTER COLUMN tier DROP DEFAULT,
      ALTER COLUMN prepare_ms DROP NOT NULL, ALTER COLUMN write_ms DROP NOT NULL, ALTER COLUMN report_ms DROP NOT NULL,
      ADD PRIMARY KEY (hub, tier, source, batch_id);
  END IF;
END $$;
DROP FUNCTION IF EXISTS subscription_registry.enabled_rules();
DROP TABLE IF EXISTS subscription_registry.limits;

-- A revision's PLACED time survives its ACTIVE verdict, and a revision placed again (a restarted hub) stays ACTIVE.
CREATE OR REPLACE FUNCTION subscription_registry.status_stamp() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.revision = OLD.revision THEN
    IF NEW.state = 'PLACED' AND OLD.state = 'ACTIVE' THEN
      RETURN OLD;
    END IF;
    NEW.placed_at := coalesce(NEW.placed_at, OLD.placed_at);
  ELSIF TG_OP = 'UPDATE' THEN  -- a new revision: the old one's PLACED time isn't its own
    NEW.placed_at := NULLIF(NEW.placed_at, OLD.placed_at);
  END IF;
  IF NEW.state = 'PLACED' THEN
    NEW.placed_at := coalesce(NEW.placed_at, now());
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS status_stamp ON subscription_registry.status;
CREATE TRIGGER status_stamp BEFORE INSERT OR UPDATE ON subscription_registry.status
  FOR EACH ROW EXECUTE FUNCTION subscription_registry.status_stamp();

-- ---------------------------------------------------------------------------------------------------------------------
-- The hub engine's own state (src/hl7rt/engine.py, delivery.py). Each row belongs to one hub and, where a hub's
-- pipelines split the work, one lane: its tier, or '<tier>:<sources>' when a pipeline runs only some sources. `app` is
-- the app's registry role (apps.app_role).

-- Every rule revision a hub ran, as it ran it (the registry rewrites a rule in place): what a retried batch rebuilds
-- its decisions from.
CREATE TABLE IF NOT EXISTS subscription_registry.revisions (
  hub             text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  subscription_id uuid NOT NULL,
  revision        int  NOT NULL,
  owner           text NOT NULL,
  name            text NOT NULL,
  source          text NOT NULL,
  where_sql       text,
  fields          text[] NOT NULL,
  topic           text NOT NULL,
  granted         text[] NOT NULL,     -- the columns it was held to (its template's, or the app's grant)
  captured_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (hub, subscription_id, revision)
);

-- One row per (hub, lane, app, source, query, batch), written before anything is published: the revisions its
-- decisions came from and their digest. A retried batch decides from these, not from the latest rules.
CREATE TABLE IF NOT EXISTS subscription_registry.bindings (
  binding_id text PRIMARY KEY,  -- '<hub>:<lane>:<app>:<source>:<query_id>:<batch_id>'
  hub        text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  lane       text NOT NULL,
  app        text NOT NULL,
  source     text NOT NULL,
  query_id   text NOT NULL,
  batch_id   bigint NOT NULL,
  offsets    jsonb,
  revisions  jsonb NOT NULL,    -- {subscription_id: revision}
  n          int NOT NULL,      -- notifications decided
  digest     text NOT NULL,     -- of their sorted notification IDs
  bound_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS bindings_batch ON subscription_registry.bindings (hub, lane, source, query_id, batch_id);

-- Each retried batch: whether it ran the bound revisions as they are live, and whether its decisions matched.
CREATE TABLE IF NOT EXISTS subscription_registry.replays (
  binding_id   text NOT NULL,
  hub          text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  at           timestamptz NOT NULL DEFAULT clock_timestamp(),
  live_same    bool,
  digest_match bool,
  n            int
);

-- An app's notifications parked while its destination fails, in order (the drainer delivers and deletes them).
CREATE TABLE IF NOT EXISTS subscription_registry.pending (
  seq             bigserial PRIMARY KEY,
  hub             text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  lane            text NOT NULL,
  app             text NOT NULL,
  topic           text NOT NULL,
  key             text,
  value           text NOT NULL,
  binding_id      text,
  notification_id text,
  parked_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS pending_app ON subscription_registry.pending (hub, lane, app, seq);

-- Each app's delivery circuit per hub and lane (CLOSED: delivering; OPEN: parking, probing; DRAINING: replaying what's
-- parked; BLOCKED: a dedicated pipeline waiting for it), and every change.
CREATE TABLE IF NOT EXISTS subscription_registry.app_delivery (
  hub     text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  lane    text NOT NULL,
  app     text NOT NULL,
  state   text NOT NULL CHECK (state IN ('CLOSED', 'OPEN', 'DRAINING', 'BLOCKED')),
  pending int,
  since   timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (hub, lane, app)
);
CREATE TABLE IF NOT EXISTS subscription_registry.app_delivery_log (
  hub     text NOT NULL REFERENCES subscription_registry.hubs ON DELETE CASCADE,
  lane    text NOT NULL,
  app     text NOT NULL,
  state   text NOT NULL,
  pending int,
  at      timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS app_delivery_log_app ON subscription_registry.app_delivery_log (hub, lane, app, at);

-- ---------------------------------------------------------------------------------------------------------------------
-- Which hub runs what. SECURITY DEFINER: callers (apps, hubs) can't read every app's row, and row-level security below
-- calls these.

-- The hub that runs a rule of an app on `app_hub` in `tier`: its own hub, or for deferred rules the one that runs them.
CREATE OR REPLACE FUNCTION subscription_registry.runs_on(app_hub text, tier text) RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = subscription_registry AS $$
  SELECT CASE WHEN tier = 'deferred' THEN (SELECT name FROM subscription_registry.hubs WHERE runs_deferred) ELSE app_hub END
$$;

-- The hub that runs one app's rule in `tier`.
CREATE OR REPLACE FUNCTION subscription_registry.executing_hub(p_owner text, p_tier text) RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = subscription_registry AS $$
  SELECT subscription_registry.runs_on((SELECT hub FROM subscription_registry.apps WHERE app_role = p_owner), p_tier)
$$;

-- The hubs a role runs as: those whose hub_<name> it is a member of.
CREATE OR REPLACE FUNCTION subscription_registry.hubs_of(p_role name) RETURNS SETOF text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = subscription_registry AS $$
  SELECT h.name FROM subscription_registry.hubs h JOIN pg_catalog.pg_roles r ON r.rolname = 'hub_' || h.name
  WHERE pg_catalog.pg_has_role(p_role, r.oid, 'MEMBER')
$$;

-- The apps a role's hubs run rules for: those assigned to them, or every app for the hub that runs deferred rules.
CREATE OR REPLACE FUNCTION subscription_registry.hub_apps(p_role name) RETURNS SETOF text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = subscription_registry AS $$
  SELECT a.app_role FROM subscription_registry.apps a
  WHERE a.hub IN (SELECT subscription_registry.hubs_of(p_role))
     OR EXISTS (SELECT 1 FROM subscription_registry.hubs h
                WHERE h.runs_deferred AND h.name IN (SELECT subscription_registry.hubs_of(p_role)))
$$;

-- The enabled rules one hub runs in one tier, whoever asks (apps only see their own rows): for the cap in stamp().
CREATE OR REPLACE FUNCTION subscription_registry.enabled_rules(p_hub text, p_tier text) RETURNS bigint
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = subscription_registry AS $$
  SELECT count(*) FROM subscription_registry.subscriptions s JOIN subscription_registry.apps a ON a.app_role = s.owner
  WHERE s.enabled AND s.tier = p_tier AND subscription_registry.runs_on(a.hub, s.tier) = p_hub
$$;

-- A hub's rule cap (per tier it runs).
CREATE OR REPLACE FUNCTION subscription_registry.rule_cap(p_hub text) RETURNS int
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = subscription_registry AS $$
  SELECT max_enabled_rules FROM subscription_registry.hubs WHERE name = p_hub
$$;

-- Onboard a team's hub (infra/registry.py, as the registry owner): its row and its role. Then grant the role to the
-- identity its pipeline runs as:   GRANT hub_<name> TO "<application ID>";   (the role needs nothing else: `hub` holds
-- every grant, and row-level security keys on membership in hub_<name>). Off-boarding: reassign its apps, then
--   DELETE FROM subscription_registry.hubs WHERE name = '<name>';  DROP ROLE hub_<name>;
-- (deleting the row deletes the hub's own rows: status, heartbeat, progress, and its engine state).
CREATE OR REPLACE FUNCTION subscription_registry.onboard_hub(p_name text, p_principal text, p_credential text,
                                                             p_max_enabled_rules int DEFAULT 2000)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO subscription_registry.hubs (name, principal, eventhubs_credential, max_enabled_rules)
    VALUES (p_name, p_principal, p_credential, p_max_enabled_rules)
    ON CONFLICT (name) DO UPDATE SET principal = excluded.principal,
      eventhubs_credential = excluded.eventhubs_credential, max_enabled_rules = excluded.max_enabled_rules;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'hub_' || p_name) THEN
    EXECUTE format('CREATE ROLE %I NOLOGIN', 'hub_' || p_name);
  END IF;
  EXECUTE format('GRANT hub TO %I', 'hub_' || p_name);
END $$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Every write to subscriptions goes through here: the owner is the acting role (the app, never the service), the app
-- must be onboarded and granted the source and fields, and revision/generation are server-assigned.
CREATE OR REPLACE FUNCTION subscription_registry.stamp() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE app subscription_registry.apps; tpl subscription_registry.templates; new_rule bool; target text; cap int;
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
    IF NEW.template IS NOT NULL THEN
      SELECT * INTO tpl FROM subscription_registry.templates WHERE app_role = current_user AND name = NEW.template;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'TEMPLATE_DENIED: template % is not granted to this app', NEW.template USING ERRCODE = '42501';
      END IF;
      IF NOT NEW.source = ANY (tpl.sources) OR NOT NEW.fields <@ tpl.fields THEN
        RAISE EXCEPTION 'TEMPLATE_DENIED: template % allows % of %', NEW.template, tpl.fields, tpl.sources
          USING ERRCODE = '42501';
      END IF;
    END IF;
  END IF;
  IF NEW.tier NOT IN ('realtime', 'deferred') THEN
    RAISE EXCEPTION 'TIER_INVALID: tier is realtime or deferred, not %', NEW.tier USING ERRCODE = '22023';
  END IF;
  -- The rule cap of the hub that will run it, per tier, across every app on that hub. One writer at a time per hub and
  -- tier, so two apps can't both take the last slot. Moving an enabled rule to another tier takes a slot there.
  -- (An upsert fires the INSERT trigger even when it becomes an UPDATE: an already enabled name isn't a new rule.)
  IF TG_OP = 'UPDATE' THEN
    new_rule := NEW.enabled AND (NOT OLD.enabled OR NEW.tier <> OLD.tier);
  ELSE
    new_rule := NEW.enabled AND NOT EXISTS (SELECT 1 FROM subscription_registry.subscriptions x
                                            WHERE x.owner = current_user AND x.name = NEW.name AND x.enabled
                                              AND x.tier = NEW.tier);
  END IF;
  IF new_rule THEN
    target := subscription_registry.runs_on(app.hub, NEW.tier);
    IF target IS NULL THEN
      RAISE EXCEPTION 'TIER_INVALID: no hub runs % rules', NEW.tier USING ERRCODE = '22023';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext('subscription_registry.enabled_rules:' || target || ':' || NEW.tier));
    cap := subscription_registry.rule_cap(target);
    IF subscription_registry.enabled_rules(target, NEW.tier) >= cap THEN
      RAISE EXCEPTION 'RULE_LIMIT_REACHED: hub % runs at most % % rules; cancel one first', target, cap, NEW.tier
        USING ERRCODE = '54000';
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
-- Row-level security: an app sees and writes only its own rows; a team's hub reads only the rules it runs and writes
-- only its own rows. edp_shared runs as the registry owner (bypasses RLS) and filters by hub itself.
ALTER TABLE subscription_registry.hubs ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.apps ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.status ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.hub_heartbeat ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.hub_progress ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.revisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.replays ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.pending ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.app_delivery ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription_registry.app_delivery_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS own ON subscription_registry.templates;
CREATE POLICY own ON subscription_registry.templates FOR SELECT TO subscriber USING (app_role = current_user);
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
-- An app sees the heartbeat of the hubs that run its rules: its own hub's realtime pipeline, and the deferred one.
DROP POLICY IF EXISTS own ON subscription_registry.hub_heartbeat;
CREATE POLICY own ON subscription_registry.hub_heartbeat FOR SELECT TO subscriber
  USING (hub = subscription_registry.runs_on((SELECT a.hub FROM subscription_registry.apps a
                                              WHERE a.app_role = current_user), tier));

-- A hub reads its own row, its apps, their templates, and the rules it runs (its apps' realtime rules; every app's
-- deferred rules on the hub that runs them) ...
DROP POLICY IF EXISTS hub_reads ON subscription_registry.hubs;
CREATE POLICY hub_reads ON subscription_registry.hubs FOR SELECT TO hub
  USING (name IN (SELECT subscription_registry.hubs_of(current_user)));
DROP POLICY IF EXISTS hub_reads ON subscription_registry.apps;
CREATE POLICY hub_reads ON subscription_registry.apps FOR SELECT TO hub
  USING (app_role IN (SELECT subscription_registry.hub_apps(current_user)));
DROP POLICY IF EXISTS hub_reads ON subscription_registry.templates;
CREATE POLICY hub_reads ON subscription_registry.templates FOR SELECT TO hub
  USING (app_role IN (SELECT subscription_registry.hub_apps(current_user)));
DROP POLICY IF EXISTS hub_reads ON subscription_registry.subscriptions;
CREATE POLICY hub_reads ON subscription_registry.subscriptions FOR SELECT TO hub
  USING (subscription_registry.executing_hub(owner, tier) IN (SELECT subscription_registry.hubs_of(current_user)));
-- ... and reads and writes only its own rows.
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['status', 'hub_heartbeat', 'hub_progress', 'revisions', 'bindings', 'replays', 'pending',
                           'app_delivery', 'app_delivery_log'] LOOP
    EXECUTE format('DROP POLICY IF EXISTS hub_own ON subscription_registry.%I', t);
    EXECUTE format('CREATE POLICY hub_own ON subscription_registry.%I TO hub '
                   'USING (hub IN (SELECT subscription_registry.hubs_of(current_user))) '
                   'WITH CHECK (hub IN (SELECT subscription_registry.hubs_of(current_user)))', t);
  END LOOP;
END $$;

-- ---------------------------------------------------------------------------------------------------------------------
-- API, called by the Subscription API as the app's role. SECURITY INVOKER: RLS and the trigger see the app.
DROP FUNCTION IF EXISTS subscription_api.subscribe(text, text, text, text, text[]);  -- before template and tier
CREATE OR REPLACE FUNCTION subscription_api.subscribe(p_name text, p_source text, p_query text, p_where text,
                                                      p_fields text[], p_template text DEFAULT NULL,
                                                      p_tier text DEFAULT 'realtime')
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER AS $$
DECLARE s subscription_registry.subscriptions; topic text; app_hub text;
BEGIN
  INSERT INTO subscription_registry.subscriptions AS t (owner, name, source, query, where_sql, fields, template, tier)
    VALUES (current_user, p_name, p_source, p_query, p_where, p_fields, p_template, p_tier)
    ON CONFLICT (owner, name) DO UPDATE
      SET source = excluded.source, query = excluded.query, where_sql = excluded.where_sql, fields = excluded.fields,
          template = excluded.template, tier = excluded.tier, enabled = true
      WHERE (t.source, t.query, t.fields, t.template, t.tier, t.enabled)
            IS DISTINCT FROM (excluded.source, excluded.query, excluded.fields, excluded.template, excluded.tier, true)
    RETURNING * INTO s;
  IF s.subscription_id IS NULL THEN  -- identical definition: no new revision
    SELECT * INTO s FROM subscription_registry.subscriptions WHERE owner = current_user AND name = p_name;
  END IF;
  SELECT destination_topic, hub INTO topic, app_hub FROM subscription_registry.apps WHERE app_role = current_user;
  RETURN jsonb_build_object('subscriptionId', s.subscription_id, 'name', s.name, 'revision', s.revision,
    'state', 'ACCEPTED', 'query', s.query, 'destination', topic, 'template', s.template, 'tier', s.tier,
    'hub', subscription_registry.runs_on(app_hub, s.tier));
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

-- The caller's subscriptions with the hub that runs each and its verdict for the current revision (only that hub's
-- counts), and the heartbeats of the hubs that run the caller's rules: `hub` maps each source to its last batch time
-- (the deferred tier's sources prefixed 'deferred:'), `heartbeats` lists them with their hub and tier.
CREATE OR REPLACE FUNCTION subscription_api.list_subscriptions()
RETURNS jsonb LANGUAGE sql STABLE SECURITY INVOKER AS $$
  WITH me AS (SELECT hub FROM subscription_registry.apps WHERE app_role = current_user),
  beats AS (
    SELECT h.* FROM subscription_registry.hub_heartbeat h
    WHERE h.hub = subscription_registry.runs_on((SELECT hub FROM me), h.tier)),
  subs AS (
    SELECT s.*, subscription_registry.runs_on((SELECT hub FROM me), s.tier) AS runs_on
    FROM subscription_registry.subscriptions s WHERE s.owner = current_user)
  SELECT jsonb_build_object(
    'subscriptions', coalesce((SELECT jsonb_agg(jsonb_build_object(
        'subscriptionId', s.subscription_id, 'name', s.name, 'revision', s.revision, 'enabled', s.enabled,
        'source', s.source, 'query', s.query, 'fields', s.fields, 'template', s.template, 'tier', s.tier,
        'hub', s.runs_on,
        'state', CASE WHEN st.revision = s.revision THEN st.state ELSE 'PENDING' END,
        'error', CASE WHEN st.revision = s.revision THEN st.error END) ORDER BY s.name)
      FROM subs s LEFT JOIN subscription_registry.status st
        ON st.subscription_id = s.subscription_id AND st.hub = s.runs_on), '[]'),
    'hub', coalesce((SELECT jsonb_object_agg(CASE WHEN tier = 'realtime' THEN source ELSE tier || ':' || source END,
                                             last_batch_at) FROM beats), '{}'),
    'hubName', (SELECT hub FROM me),
    'heartbeats', coalesce((SELECT jsonb_agg(jsonb_build_object('hub', hub, 'tier', tier, 'source', source,
        'lastBatchAt', last_batch_at, 'batchId', batch_id, 'rules', rules) ORDER BY hub, tier, source)
      FROM beats), '[]'))
$$;

-- ---------------------------------------------------------------------------------------------------------------------
-- Platform monitoring (the "HL7 subscriptions" dashboard reads these through the registry's Unity Catalog catalog).
-- Every app's rules in one place, so it's for the platform team only: nothing here is granted to `subscriber`.
CREATE SCHEMA IF NOT EXISTS subscription_monitor;

-- (Columns are only appended: the dashboards and the Power BI views read these by name.)
DROP VIEW IF EXISTS subscription_monitor.subscriptions;  -- views only: dropped and recreated, so columns can change
CREATE VIEW subscription_monitor.subscriptions AS
SELECT a.app_name, a.service_principal, s.name AS subscription, s.revision, s.source,
       s.query AS rule, array_to_string(s.fields, ', ') AS fields, s.template, s.tier,
       CASE WHEN st.revision = s.revision THEN st.state WHEN s.enabled THEN 'PENDING' ELSE 'CANCELLING' END AS state,
       CASE WHEN st.revision = s.revision THEN st.error END AS error,
       s.updated_at AS registered_at,
       CASE WHEN st.revision = s.revision THEN st.observed_at END AS confirmed_at,
       CASE WHEN st.revision = s.revision
            THEN round(extract(epoch FROM st.observed_at - s.updated_at)::numeric, 1)::float8 END AS seconds_to_confirm,
       x.hub,  -- the hub that runs it
       CASE WHEN st.revision = s.revision THEN st.placed_at END AS placed_at,
       CASE WHEN st.revision = s.revision
            THEN round(extract(epoch FROM st.placed_at - s.updated_at)::numeric, 1)::float8 END AS seconds_to_place
FROM subscription_registry.subscriptions s
LEFT JOIN subscription_registry.apps a ON a.app_role = s.owner
CROSS JOIN LATERAL (SELECT subscription_registry.runs_on(a.hub, s.tier) AS hub) x
LEFT JOIN subscription_registry.status st ON st.subscription_id = s.subscription_id AND st.hub = x.hub;

-- One row per hub, tier, and source (`source` keeps its 'deferred:' prefix for the deferred tier).
DROP VIEW IF EXISTS subscription_monitor.hub;
CREATE VIEW subscription_monitor.hub AS
SELECT CASE WHEN tier = 'realtime' THEN source ELSE tier || ':' || source END AS source, last_batch_at,
       round(extract(epoch FROM now() - last_batch_at))::int AS seconds_since_last_batch, batch_id, rules, hub, tier
FROM subscription_registry.hub_heartbeat;

-- Each app's delivery circuit on each hub (OPEN or DRAINING: its notifications are parked; see pending).
DROP VIEW IF EXISTS subscription_monitor.delivery;
CREATE VIEW subscription_monitor.delivery AS
SELECT d.hub, d.lane, a.app_name, d.state, d.pending, d.since
FROM subscription_registry.app_delivery d
LEFT JOIN subscription_registry.apps a ON a.app_role = d.app;

REVOKE ALL ON SCHEMA subscription_monitor FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA subscription_monitor FROM PUBLIC;

-- ---------------------------------------------------------------------------------------------------------------------
-- Grants. App roles get the API plus the RLS-filtered table access its INVOKER functions need. The service gets no
-- grants of its own: membership in each app role (infra/registry.py) lets it SET ROLE to its caller, and its policy on
-- apps lets it map a verified caller to that role. A team's hub gets what `hub` has, through hub_<name>: read the rules
-- it runs, write its own rows (row-level security above). Nothing by default.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA subscription_api FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA subscription_registry FROM PUBLIC;
GRANT USAGE ON SCHEMA subscription_api, subscription_registry TO subscriber;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA subscription_api TO subscriber;
GRANT EXECUTE ON FUNCTION subscription_registry.stamp(), subscription_registry.enabled_rules(text, text),
                          subscription_registry.rule_cap(text), subscription_registry.runs_on(text, text) TO subscriber;
GRANT SELECT ON subscription_registry.apps, subscription_registry.status, subscription_registry.hub_heartbeat,
                subscription_registry.templates TO subscriber;
GRANT SELECT, INSERT, UPDATE ON subscription_registry.subscriptions TO subscriber;
GRANT USAGE ON SEQUENCE subscription_registry.generation TO subscriber;

GRANT USAGE ON SCHEMA subscription_registry TO hub;
GRANT EXECUTE ON FUNCTION subscription_registry.runs_on(text, text), subscription_registry.executing_hub(text, text),
                          subscription_registry.hubs_of(name), subscription_registry.hub_apps(name),
                          subscription_registry.status_stamp() TO hub;
GRANT SELECT ON subscription_registry.hubs, subscription_registry.apps, subscription_registry.templates,
                subscription_registry.subscriptions TO hub;
GRANT SELECT, INSERT, UPDATE, DELETE ON subscription_registry.status, subscription_registry.hub_heartbeat,
                subscription_registry.hub_progress, subscription_registry.revisions, subscription_registry.bindings,
                subscription_registry.replays, subscription_registry.pending, subscription_registry.app_delivery,
                subscription_registry.app_delivery_log TO hub;
GRANT USAGE ON SEQUENCE subscription_registry.pending_seq_seq TO hub;
