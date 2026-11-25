"""An example subscriber app in Python, run from the repo root. Signs in as its own Databricks service principal,
declares my_subscriptions.py to the Subscription API (through the client generated from its contract,
docs/api/openapi.json), waits until the hub runs exactly those revisions, then prints the notifications that arrive on
its own Event Hub. The API does the rest: Databricks compiles each rule, the registry keeps it, the hub runs it.

    uv run python examples/subscribers/python/subscriber.py [file.properties] [key=value ...] [--duration=40] [--e2e]

Nothing is silent. Exit 2: a rule was refused (Databricks' error, or a shape the hub won't run). 3: the hub can't run
one. 4: the hub didn't confirm in time or isn't processing. 5: not signed in as the identity the settings name (a
service principal unless identity=user). 6: this identity isn't an onboarded subscriber app.

Settings: only databricks.profile (the service principal's Databricks CLI profile) and api.app (the Subscription API's
Databricks App name; its URL is looked up, so a rebuilt workspace needs no change; api.url overrides it). Its topic,
Kafka bootstrap servers, and Unity Catalog credential come from the API (get_connection). They're read from the file
(default subscriber.properties, else the one file in examples/subscribers/apps/, which `make registry` writes) plus
key=value arguments. --e2e runs the end-to-end checks instead. (= the Java example's App.)

A person runs it as themselves only explicitly, with identity=user (their own CLI profile, or none for the ambient
identity). From a Databricks notebook, notebook.py calls main(argv, rules) with its own rules; there the notebook's
token is exchanged for one the API's App accepts (api.Exchanged).
"""

import os
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import build  # noqa: E402

build.ensure()

import api  # noqa: E402
import eventhubs  # noqa: E402
import my_subscriptions  # noqa: E402
import notifications  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.errors import DatabricksError  # noqa: E402
from hl7_subscriptions_client.api.subscriptions import declare, get_connection, get_status  # noqa: E402
from hl7_subscriptions_client.models import StatusState  # noqa: E402

WAIT_SECONDS = 120
POLL_SECONDS = 25
HEALTH_CHECK_EVERY = 30
_SP = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
IDENTITIES = ("service_principal", "user")  # the identity setting; the first is the default


def main(argv: list[str], rules: dict[str, str] | None = None):
    """Run the app. `rules` (name -> SQL) is what to declare; by default my_subscriptions.all(). {} cancels them all."""
    cfg = settings(argv)
    workspace = sign_in(cfg)
    client = api_client(workspace, cfg)
    where = "my_subscriptions.py" if rules is None else "your rules"

    # 0. Where this app's notifications arrive and how to read them: the API knows, so the app configures neither.
    try:
        connection = api.call(get_connection, client)
    except api.ApiError as e:
        exit_with(6 if e.status in (403, 503) else 3,
                  f"{e.code}: {e.message}" + (" (make registry onboards apps)" if e.status == 403 else ""))  # fmt: skip
    if "--e2e" in argv:
        import e2e

        sys.exit(e2e.run(workspace, client, connection, cfg))
    run_for = duration(argv)

    # 1. Declare what the code says. Databricks compiles each rule in this call: ACCEPTED, or refused with its error.
    try:
        declared = api.call(declare, client, body=api.declared(my_subscriptions.all() if rules is None else rules))
    except api.ApiError as e:
        if e.status == 403:
            exit_with(6, f"{e.message} (make registry onboards apps)")
        if e.status != 422:
            exit_with(3, f"the Subscription API answered {e.status}: {e.message}")
        for r in e.refusals:
            print(f"refused     {r.name if isinstance(r.name, str) else '':<22} {r.code}: {first_line(r.message)}")
        exit_with(2, f"{len(e.refusals)} subscription(s) refused; fix {where}")
    current = {}  # subscriptionId -> revision
    for s in declared.accepted:
        current[s.subscription_id] = s.revision
        print(f"subscribed  {s.name:<22} revision {s.revision} ACCEPTED{' (unchanged)' if s.unchanged else ''}\n"
              f"            {s.query}")  # fmt: skip
    for s in declared.cancelled:
        print(f"cancelled   {s.name:<22} (no longer in {where})")
    if not current:
        exit_with(0, f"no subscriptions declared in {where}")

    # 2. Wait until the hub runs exactly these revisions: READY, FAILED, or not within WAIT_SECONDS. The API holds each
    #    call for up to POLL_SECONDS (short, so no proxy in between cuts it off).
    deadline = time.monotonic() + WAIT_SECONDS
    while True:
        left = max(1, int(deadline - time.monotonic()))
        try:
            status = api.call(get_status, client, wait=min(POLL_SECONDS, left))
        except api.ApiError as e:
            exit_with(4, f"the Subscription API answered {e.status}: {e.message}")
        if status.state in (StatusState.READY, StatusState.FAILED) or time.monotonic() >= deadline:
            break
    for s in status.subscriptions:
        if s.enabled and s.state != "PENDING":
            print(f"hub         {s.name:<22} revision {s.revision} {s.state}")
    problems = f"[{', '.join(status.problems)}]"
    if status.state == StatusState.FAILED:
        exit_with(3, f"the hub can't run {problems}")
    if status.state != StatusState.READY:
        exit_with(4, f"{status.state}: {problems} (are the subscription hub pipelines running? make demo dev)")

    # 3. Consume this app's topic with a token Unity Catalog issues from its service credential (no Azure secret here).
    from confluent_kafka import Consumer

    tokens = eventhubs.tokens_for(workspace, connection.credential, connection.token_scope)
    group = cfg.get("consumer.group") or f"{cfg.get('databricks.profile') or 'subscriber'}-python"
    consumer = Consumer(eventhubs.consumer_config(connection.bootstrap_servers, group, tokens))
    consumer.subscribe([connection.topic])
    print(f"listening   on {connection.topic} as consumer group {group} (Ctrl-C to stop)", flush=True)
    try:
        notifications.Listener(current, group, sys.stdout, time.time()).run(
            consumer, time.time() + run_for if run_for else float("inf"), lambda: health_check(client),
            HEALTH_CHECK_EVERY)  # fmt: skip
    except KeyboardInterrupt:
        consumer.close()
        exit_with(0, "stopped")
    consumer.close()
    exit_with(0, f"ran for {run_for} s")


def health_check(client):
    """While listening: warn (don't stop) if the hub has stopped, or the API can't be reached for a moment."""
    try:
        now = api.call(get_status, client, wait=0)
        if now.state == StatusState.HUB_STALE:
            print(f"WARNING     [{', '.join(now.problems)}]: are the subscription hub pipelines running?", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"WARNING     health check failed ({e}); still listening", flush=True)


def exit_with(code: int, message: str):
    print(("done        " if code == 0 else "ERROR       ") + message, flush=True)
    sys.exit(code)


def first_line(s: str) -> str:
    return (s or "").strip().splitlines()[0] if (s or "").strip() else ""


# ----------------------------------------------------------------------------------------------------------------------
def sign_in(cfg: dict) -> WorkspaceClient:
    """With a profile: from that profile only, OAuth M2M for a service principal (a user's profile signs in its own
    way, e.g. databricks-cli). Without one: the ambient identity, e.g. a workspace job's run-as service principal, or a
    notebook's user. It must be the identity the settings name: a service principal (the default), whose user name is
    its application ID, or with identity=user, a person."""
    who = identity(cfg)
    profile = cfg.get("databricks.profile", "").strip()
    if not profile:
        workspace = WorkspaceClient()
    elif who == "user":
        workspace = WorkspaceClient(profile=profile)
    else:
        workspace = WorkspaceClient(profile=profile, auth_type="oauth-m2m")
    me = workspace.current_user.me()
    problem = identity_problem(who, me.user_name, profile)
    if problem:
        exit_with(5, problem)
    print(f"signed in   as {me.display_name} ({me.user_name})" + (" as a user" if who == "user" else ""))
    return workspace


def identity(cfg: dict) -> str:
    """The identity setting: service_principal (the default) or user."""
    v = (cfg.get("identity") or "").strip() or IDENTITIES[0]
    if v not in IDENTITIES:
        raise ValueError(f"identity: {' or '.join(IDENTITIES)}, not {v}")
    return v


def identity_problem(who: str, user_name: str | None, profile: str) -> str | None:
    """Why this sign-in isn't the identity the settings name, or None. Explicit both ways."""
    check = f": check profile {profile}" if profile else ""
    if who == "user" and is_service_principal(user_name):
        return f"signed in as {user_name}, a service principal, but identity=user{check} (drop identity=user)"
    if who != "user" and not is_service_principal(user_name):
        return f"signed in as {user_name}, not a service principal{check} (to run as yourself, set identity=user)"
    return None


def is_service_principal(user_name: str | None) -> bool:
    return bool(user_name and _SP.fullmatch(user_name))


def bearer(workspace: WorkspaceClient) -> str:
    """The signed-in identity's Databricks OAuth token (the SDK refreshes it)."""
    return workspace.config.authenticate()["Authorization"].removeprefix("Bearer ")


def on_databricks(cfg: dict) -> bool:
    """In a Databricks notebook or job with its ambient identity (no profile)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ and not cfg.get("databricks.profile", "").strip()


def duration(argv: list[str]) -> int | None:
    """--duration=40 (seconds) or the Java example's --duration=PT1M30S."""
    for a in argv:
        if a.startswith("--duration="):
            v = a.split("=", 1)[1]
            if v.isdigit():
                return int(v)
            m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", v)
            if not m:
                raise ValueError(f"--duration: seconds or PT1M30S, not {v}")
            h, mi, s = (int(x or 0) for x in m.groups())
            return h * 3600 + mi * 60 + s
    return None


# ----------------------------------------------------------------------------------------------------------------------
def settings(argv: list[str]) -> dict[str, str]:
    """The settings file plus key=value arguments (which win). Refuses secrets: they belong in the CLI profile."""
    named = next((Path(a) for a in argv if a.endswith(".properties")), None)
    file = settings_file(named, [Path.cwd(), HERE, HERE.parent])
    cfg = {}
    if file is not None:
        if not file.exists():
            raise ValueError(f"{file} not found")
        for line in file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith(("#", "!")) and "=" in line:
                key, value = line.split("=", 1)
                cfg[key.strip()] = value.strip()
    for a in argv:
        if "=" in a and not a.startswith("--") and not a.endswith(".properties"):
            key, value = a.split("=", 1)
            cfg[key.strip()] = value.strip()
    for key in cfg:
        if "secret" in key.lower() or "password" in key.lower():
            raise ValueError(f"{key}: keep secrets in the Databricks CLI profile, not here")
    return cfg


def settings_file(named: Path | None, dirs: list[Path]) -> Path | None:
    """The named file, else the first directory with subscriber.properties or exactly one file in apps/; else None."""
    if named is not None:
        return named
    for d in dirs:
        if (d / "subscriber.properties").exists():
            return d / "subscriber.properties"
        found = sorted((d / "apps").glob("*.properties")) if (d / "apps").is_dir() else []
        if len(found) == 1:
            return found[0]
        if len(found) > 1:
            raise ValueError(f"several apps in {d / 'apps'}: name one, e.g. {found[0]}")
    return None


def required(cfg: dict, key: str) -> str:
    v = (cfg.get(key) or "").strip()
    if not v:
        raise ValueError(f"missing {key} (make registry writes it to examples/subscribers/apps/)")
    return v


def api_client(workspace, cfg: dict):
    """The Subscription API's client, sending the signed-in identity's token. On Databricks (no profile) that token is
    the notebook's own, which can't call a Databricks App: it's exchanged for one with the App as its audience, so
    there the App must be named (api.app), and one lookup gives both its URL and its OAuth client ID."""
    if not on_databricks(cfg):
        return api.connect(api_url(workspace, cfg), lambda: bearer(workspace))
    name = (cfg.get("api.app") or "").strip()
    if not name:
        raise ValueError("missing api.app: on Databricks the notebook's token is exchanged for one for the "
                         "Subscription API's App, which is found by name (api.url alone isn't enough)")  # fmt: skip
    app = find_app(workspace, name)
    exchanged = api.Exchanged(workspace.config.host, lambda: bearer(workspace), app.oauth2_app_client_id)
    return api.connect((cfg.get("api.url") or "").strip() or app.url, exchanged)


def api_url(workspace, cfg: dict) -> str:
    """The Subscription API's URL: api.url if set, else the URL of the Databricks App named api.app (CAN_USE can read
    it)."""
    url = (cfg.get("api.url") or "").strip()
    if url:
        return url
    return find_app(workspace, required(cfg, "api.app")).url


def find_app(workspace, name: str):
    """The Subscription API's Databricks App, deployed (it has a URL)."""
    try:
        app = workspace.apps.get(name)
    except DatabricksError as e:
        exit_with(6, f"can't find the Subscription API app {name}: {first_line(str(e))} (make registry grants CAN_USE)")
    if not app.url:
        exit_with(4, f"the Subscription API app {name} has no URL yet (not deployed?)")
    return app


if __name__ == "__main__":
    main(sys.argv[1:])
