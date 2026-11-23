"""Benchmark: dynamic patient rules with large patient lists, at scale. Run from the repo root as the deployer:

    uv run python examples/benchmarks/patient_lists.py seed <rules> [--patients=2000] [--like=sp-hl7-ed-monitor]
    uv run python examples/benchmarks/patient_lists.py churn <edits-per-minute> [--minutes=10]
    uv run python examples/benchmarks/patient_lists.py status | clean | offboard

Each virtual app watches a census of `--patients` IDs with the ED monitor's five patient rules
(examples/subscribers/python/patient_rules.py), one per silver source, so `seed 1000` is 200 censuses. A census holds
--in-population IDs from the generator's background population (default 50 of its 50,000 patients: each rule matches
0.1% of patients, as in docs/SCALABILITY.md) and the rest are patients with no traffic. `churn` edits censuses as a
dynamic workflow does: each edit swaps one in-population patient and re-registers that census's five rules.

The rules belong to a virtual app of their own, `perf-bench` (a NOLOGIN registry role, onboarded with the template and
hub of the --like app, so its matches land on that app's topic): in the monitor's own app, its status would wait on
every benchmark rule. They're written as that role through subscription_api.subscribe, with the canonical query and
WHERE from the API's own parser (rules.parse): the real owner trigger, RLS, and rule cap, but no warehouse compile (the
rules are the monitor's, which Databricks compiles). Names are perf-<census>-<rule>. `clean` cancels them; `offboard`
then removes the virtual app (make registry also does: it isn't in Terraform). Measure with the ED monitor running
alongside (edit -> ACTIVE, delivery) and subscription_registry.hub_progress (each batch's time).
"""

import argparse
import random
import ssl
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "examples/subscribers/python")]

import patient_rules  # noqa: E402

from subscription_api.rules import SOURCES, parse  # noqa: E402

PROJECT = "hl7-subscriptions"  # the registry's Lakebase project (databricks.yml registry_project)
ENDPOINT, DATABASE = f"projects/{PROJECT}/branches/production/endpoints/primary", "databricks_postgres"
APP, ROLE = "perf-bench", "app_perf_bench"
POPULATION = 50_000  # the generator's background patients (MRN000001 ... MRN050000; hl7_generator --patients)
NO_TRAFFIC = 1_000_000  # IDs from here up never appear in the feed


def census(i: int, size: int, in_population: int) -> list[str]:
    """Census i: `in_population` IDs with traffic, the rest without (deterministic per census)."""
    rng = random.Random(i)
    live = rng.sample(range(1, POPULATION + 1), in_population)
    quiet = range(NO_TRAFFIC + i * size, NO_TRAFFIC + i * size + size - in_population)
    return [f"MRN{p:06d}" for p in live] + [f"MRN{p:07d}" for p in quiet]


def rules_for(i: int, patients: list[str], schema: str) -> dict[str, object]:
    """Census i's five rules: name -> parsed rule (canonical query, WHERE, fields), as the API would store them."""
    return {
        f"perf-{i:03d}-{name.removeprefix('ed-patient-')}": parse(sql, SOURCES, schema)
        for name, sql in patient_rules.patient_rules(patients).items()
    }


def swap(patients: list[str], rng: random.Random, in_population: int) -> list[str]:
    """One admission and one discharge: an in-population patient replaced by another."""
    out = list(patients)
    out[rng.randrange(in_population)] = f"MRN{rng.randint(1, POPULATION):06d}"
    return out


def _connect():
    import pg8000.native
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()  # DATABRICKS_HOST + DATABRICKS_AUTH_TYPE=azure-cli, as make sets them
    db = pg8000.native.Connection(
        w.current_user.me().user_name, host=w.postgres.get_endpoint(name=ENDPOINT).status.hosts.host,
        database=DATABASE, password=w.postgres.generate_database_credential(endpoint=ENDPOINT).token,
        ssl_context=ssl.create_default_context(),
    )  # fmt: skip
    return w, db


class Registry:
    """The registry as the virtual app. SET LOCAL ROLE needs SET on the role (Postgres 16+): granted here, revoked at
    close."""

    def __init__(self, like: str):
        self.w, self.db = _connect()
        self.me = self.w.current_user.me().user_name
        rows = self.db.run("SELECT app_role, destination_topic FROM subscription_registry.apps WHERE app_name = :n",
                           n=like)  # fmt: skip
        if not rows:
            sys.exit(f"error: app {like} isn't onboarded (make registry)")
        like_role, topic = rows[0]
        if not self.db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=ROLE):
            self.db.run(f'CREATE ROLE "{ROLE}" NOLOGIN IN ROLE subscriber')
        self.db.run(
            "INSERT INTO subscription_registry.apps (app_role, app_name, destination_topic, allowed_sources, "
            "allowed_fields, hub) SELECT :r, :n, :t, allowed_sources, allowed_fields, hub FROM "
            "subscription_registry.apps WHERE app_role = :like ON CONFLICT (app_role) DO NOTHING",
            r=ROLE, n=APP, t=topic, like=like_role,
        )  # fmt: skip
        self.db.run(
            "INSERT INTO subscription_registry.templates (app_role, name, sources, fields) SELECT :r, name, sources, "
            "fields FROM subscription_registry.templates WHERE app_role = :like ON CONFLICT DO NOTHING",
            r=ROLE, like=like_role,
        )  # fmt: skip
        self.role, self.schema = ROLE, f"{infra_catalog()}.sub_{like.replace('-', '_')}"
        self.db.run(f'GRANT "{ROLE}" TO "{self.me}" WITH SET TRUE')

    def put(self, rules: dict, template: str):
        self.db.run("BEGIN")
        self.db.run(f'SET LOCAL ROLE "{self.role}"')
        for name, r in rules.items():
            self.db.run("SELECT subscription_api.subscribe(:n, :s, :q, :w, CAST(:f AS text[]), :t)",
                        n=name, s=r.source, q=r.query, w=r.where, f=list(r.fields), t=template)  # fmt: skip
        self.db.run("COMMIT")

    def clean(self) -> int:
        self.db.run("BEGIN")
        self.db.run(f'SET LOCAL ROLE "{self.role}"')
        names = self.enabled()
        for (name,) in names:
            self.db.run("SELECT subscription_api.unsubscribe(:n)", n=name)
        self.db.run("COMMIT")
        return len(names)

    def offboard(self) -> int:
        """Remove the virtual app, its rules, and its role (after clean, once the hub reported them CANCELLED)."""
        n = self.db.run("DELETE FROM subscription_registry.subscriptions WHERE owner = :r RETURNING 1", r=ROLE)
        self.db.run("DELETE FROM subscription_registry.apps WHERE app_role = :r", r=ROLE)
        self.db.run(f'REVOKE "{ROLE}" FROM "{self.me}"')
        self.db.run(f'DROP ROLE "{ROLE}"')
        return len(n)

    def status(self):
        return self.db.run(
            "SELECT state, count(*) FROM subscription_monitor.subscriptions WHERE app_name = :n GROUP BY state "
            "ORDER BY state", n=APP,
        )  # fmt: skip

    def enabled(self):
        return self.db.run("SELECT name FROM subscription_registry.subscriptions WHERE owner = :r AND enabled", r=ROLE)

    def close(self):
        if self.db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=ROLE):
            self.db.run(f'REVOKE "{ROLE}" FROM "{self.me}"')
        self.db.close()


def infra_catalog() -> str:
    import subprocess

    return subprocess.run(["terraform", f"-chdir={ROOT / 'infra/azure'}", "output", "-raw", "catalog"],
                          capture_output=True, text=True, check=True).stdout.strip()  # fmt: skip


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("action", choices=["seed", "churn", "status", "clean", "offboard"])
    p.add_argument("n", type=int, nargs="?", default=0, help="seed: rules (5 per census); churn: edits per minute")
    p.add_argument("--like", default="sp-hl7-ed-monitor", help="the app whose template and hub the virtual app gets")
    p.add_argument("--patients", type=int, default=2_000, help="IDs per census")
    p.add_argument("--in-population", type=int, default=50, help="IDs per census with traffic")
    p.add_argument("--minutes", type=float, default=10, help="churn: how long")
    a = p.parse_args(argv)
    reg = Registry(a.like)
    try:
        censuses = a.n // len(patient_rules.PATIENT_RULES) if a.action == "seed" else None
        if a.action == "seed":
            started = time.monotonic()
            for i in range(censuses):
                reg.put(rules_for(i, census(i, a.patients, a.in_population), reg.schema), patient_rules.TEMPLATE)
            print(f"seeded      {censuses * 5} rules ({censuses} censuses x {a.patients} patients) in "
                  f"{time.monotonic() - started:.0f} s")  # fmt: skip
        elif a.action == "churn":
            names = reg.enabled()
            seeded = {int(n.split("-")[1]) for (n,) in names}
            rng, lists, edits = random.Random(7), {}, 0
            deadline = time.monotonic() + a.minutes * 60
            while time.monotonic() < deadline:
                i = rng.choice(sorted(seeded))
                lists[i] = swap(lists.get(i) or census(i, a.patients, a.in_population), rng, a.in_population)
                t = time.monotonic()
                reg.put(rules_for(i, lists[i], reg.schema), patient_rules.TEMPLATE)
                edits += 1
                time.sleep(max(0.0, 60 / a.n - (time.monotonic() - t)))
            print(f"churned     {edits} census edits ({edits * 5} rule revisions) over {a.minutes:g} min")
        elif a.action == "clean":
            print(f"cancelled   {reg.clean()} perf rules")
        elif a.action == "offboard":
            print(f"offboarded  {APP} ({reg.offboard()} subscription rows deleted)")
            return
        print("status     ", reg.status())
    finally:
        reg.close()


if __name__ == "__main__":
    main()
