"""The subscription hub's engine: one hub pipeline's live rules, each micro-batch of a silver source decided, bound, and
published to each app's own topic. One Engine per pipeline, shared by its flows (one per source), which run batches on
concurrent threads.

Rules (hl7rt.registry) are placed when they change, never per batch: a simple rule (an OR of AND groups of
column-literal comparisons) in the source's driver Matcher (hl7rt.matching), indexed on its most selective = / IN, and
anything else as a Spark expression. Placing a revision touches only that rule, so an edited patient list changes one
rule, not the set, and it's PLACED in the registry from then on. Each batch:

1. refresh: one registry check, shared by the flows (at most one per REFRESH_S);
2. read the batch once (it's bounded by maxOffsetsPerTrigger): each record's columns, the rendered JSON member of every
   field a rule here selects (hl7rt.render, so payloads are Spark's bytes), and per chunk of expression rules an array
   of the ones the record matches (subscriptions.matches_sql). A rule that fails on the data (a cast of bad data) is
   found alone, reported FAILED, and the batch read again without it;
3. match the simple rules on the driver;
4. bind, per app, before publishing: the revisions its decisions came from and a digest of their notification IDs. A
   batch that was bound before is a retry (a failed attempt, or a restart before the commit): it decides from its
   binding, rebuilding the decisions from the revisions as they ran (hl7rt.registry captures each), not from the
   latest rules, and records whether they matched (replays);
5. publish (hl7rt.delivery): a producer per app, so one app's broken destination never holds up another's; its
   notifications are parked in the registry and delivered in order once it's back;
6. mark the revisions it decided with ACTIVE, and write a heartbeat and the batch's phases (hub_progress).

Measured in the 50-app benchmark (docs/SCALABILITY.md): one pipeline with a flow per source delivered p99 2.9 s at 50
apps and 2,000 rules (matching p99 under 0.1 s); every slow batch was the silver read.
"""

import contextlib
import hashlib
import threading
import time
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

from hl7rt import delivery, matching, render, subscriptions
from hl7rt.registry import Registry
from hl7rt.subscriptions import Rule

REFRESH_S = 0.05  # the flows share one registry check per 50 ms
KEEP_LOCK_S = 30.0  # how often the session lock's connection is checked
PRUNE_REVISIONS_S = 600.0
WARM_TIMEOUT_S = 20


def digest(ids: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest()


class RuleSet:
    """One source's rules on this hub: simple ones in the driver's Matcher, the rest as Spark expressions."""

    def __init__(self, source: str):
        self.source = source
        self.matcher = matching.Matcher()
        self.expressions: dict[str, tuple[Rule, str]] = {}  # subscription id -> (rule, owner)
        self.fields_seen: set[str] = set()  # every field a rule here selected: what a batch renders

    def place(self, rule: Rule, owner: str) -> bool:
        """Add or replace one checked revision (subscriptions.runnable); True when it's simple (the driver's)."""
        self.remove(rule.subscription_id)
        self.fields_seen |= set(rule.fields)
        try:
            self.matcher.place(rule, owner)
            return True
        except matching.RuleNotSimple:
            self.expressions[rule.subscription_id] = (rule, owner)
            return False

    def remove(self, sid: str):
        self.matcher.remove(sid)
        self.expressions.pop(sid, None)

    def __len__(self):
        return len(self.matcher.entries) + len(self.expressions)


def frame(batch, source: str, fields, expressions: list[Rule], chunk: int):
    """What a batch is read as: each record's columns, the rendered member of each of `fields`, and per chunk of
    `expressions` (_x<k>) the indexes of the ones it matches. Simple rules aren't in the plan: the driver matches
    them."""
    hits = [
        F.expr(subscriptions.matches_sql(expressions[i : i + chunk], i)).alias(f"_x{k}")
        for k, i in enumerate(range(0, len(expressions), chunk))
    ]
    return subscriptions.events(batch, source).select("*", *render.members(source, fields), *hits)


def read(batch, source: str, fields, expressions: list[Rule], chunk: int) -> list[dict]:
    """The batch's records, collected once (frame)."""
    return [r.asDict() for r in frame(batch, source, fields, expressions, chunk).collect()]


def match(rows: list[dict], matcher: matching.Matcher, expressions: list[tuple[Rule, str]], rendered: set[str]):
    """[(rule, owner, record)] for each record and rule it matches, once per rule. `expressions` is what the batch was
    read with; the matcher is live, so a rule placed since the read is skipped when the read didn't render its fields,
    and an expression rule now in the matcher (its new revision is simple) is decided there, once."""
    keys = sorted(k for k in rows[0] if k.startswith("_x")) if rows and expressions else []
    out = []
    for rec in rows:
        hits = {e.rule.subscription_id: (e.rule, e.owner) for e in matcher.match(rec) if set(e.rule.fields) <= rendered}
        for k in keys:
            for i in rec[k] or ():
                rule, owner = expressions[i]
                if rule.subscription_id not in hits and rule.subscription_id not in matcher.entries:
                    hits[rule.subscription_id] = (rule, owner)
        out += [(rule, owner, rec) for _, (rule, owner) in sorted(hits.items())]
    return out


def notifications(batch, source: str, rules: list[Rule], owner: str = "app", chunk: int | None = None):
    """(topic, key, value) of every match of `rules` in a batch of `source`'s topic, as the hub decides it: for tests
    and benchmarks (the hub itself keeps its rules placed across batches)."""
    rs = RuleSet(source)
    for r in subscriptions.runnable(rules)[0]:
        rs.place(r, owner)
    expressions = list(rs.expressions.values())
    rows = read(batch, source, rs.fields_seen, [r for r, _ in expressions], chunk or subscriptions.CHUNK)
    return [(rule.topic, *render.notification(rule, source, rec))
            for rule, _, rec in match(rows, rs.matcher, expressions, rs.fields_seen)]  # fmt: skip


class Engine:
    def __init__(self, registry: Registry, producer, mode: str = "park", chunk: int = subscriptions.CHUNK,
                 lock_wait_s: float = 120.0, warm: bool = True):  # fmt: skip
        """registry: this pipeline's (hub, tier, sources). producer(app) -> a new confluent-kafka Producer for that
        app's topic (one per app, made once). mode: hl7.subscriptions.on_failure (park | block)."""
        self.registry, self._make, self.mode, self.chunk = registry, producer, mode, chunk
        self.lock_wait_s, self.warm = lock_wait_s, warm
        self.sets = {s: RuleSet(s) for s in registry.sources}
        self.lock = threading.RLock()  # held while rules change and while a batch is matched
        self._init = threading.Lock()  # one Delivery, one producer per app, however many flows start at once
        self._delivery: delivery.Delivery | None = None
        self._producers: dict[str, object] = {}
        self._topics: dict[str, str] = {}  # app -> its topic (apps with a placed rule)
        self._warmed: set[str] = set()
        self._where: dict[str, str] = {}  # subscription id -> the source it's placed in
        self._failed: dict[str, int] = {}  # subscription id -> its revision that failed on data (until a new one)
        self._last_refresh = self._last_lock = self._last_prune = 0.0
        self._compile_ms = 0
        self.closed = False

    # -- lifecycle -----------------------------------------------------------------------------------------------------
    def start(self):
        """Once per process, on the first batch: this hub runs this tier, this process alone (session lock), and what a
        previous process parked goes out first."""
        if self._delivery is not None:
            return
        with self._init:
            if self._delivery is None:
                self.registry.check()
                self.registry.lock(self.lock_wait_s)
                d = delivery.Delivery(self.producer, self.registry.store(), self.registry.drain_store(), self.mode,
                                      f"{self.registry.hub}:{self.registry.lane}")  # fmt: skip
                d.resume()
                self._delivery = d

    def close(self):
        """Stop (tests; a pipeline's process just ends): release the session lock and the registry's connections."""
        self.closed = True
        self.registry.close()

    def producer(self, app: str):
        with self._init:
            if app not in self._producers:
                self._producers[app] = self._make(app)
            return self._producers[app]

    def _warm_up(self, apps):
        """Each app's producer connected, with its topic's metadata, before its first notification: a cold producer
        (token, metadata, connection) took longer than a batch waits, so the 50-app benchmark parked every new app
        once."""
        if not self.warm:
            return
        with self._init:
            cold = [a for a in apps if a not in self._warmed]
            self._warmed.update(cold)
        for app in cold:

            def warm(a=app, topic=self._topics[app]):
                with contextlib.suppress(Exception):  # a missing topic etc.: its first delivery finds out
                    self.producer(a).list_topics(topic, timeout=WARM_TIMEOUT_S)

            threading.Thread(target=warm, daemon=True, name=f"warm-{app}").start()

    # -- rules ---------------------------------------------------------------------------------------------------------
    def refresh(self, force: bool = False):
        """Place what changed in the registry (at most once per REFRESH_S, whichever flow asks)."""
        with self.lock:
            now = time.monotonic()
            if not force and now - self._last_refresh < REFRESH_S:
                return
            if now - self._last_lock > KEEP_LOCK_S:
                self.registry.keep_lock()
                self._last_lock = now
            if now - self._last_prune > PRUNE_REVISIONS_S:
                with contextlib.suppress(Exception):  # housekeeping only
                    self.registry.prune_revisions()
                self._last_prune = now
            changed, removed = self.registry.refresh()
            t0 = time.monotonic()
            new_apps = self._apply(changed, removed) if changed or removed else []
            if changed:
                self._compile_ms = int((time.monotonic() - t0) * 1000)
            self._last_refresh = time.monotonic()
        self._warm_up(new_apps)

    def _apply(self, changed, removed) -> list[str]:
        for sid in removed:  # its app moved to another hub: that hub reports it
            source = self._where.pop(sid, None)
            if source is not None:
                self.sets[source].remove(sid)
        placed, cancelled, failed, errors = [], [], [], {}
        for row in changed:
            rule, sid = row.rule, row.rule.subscription_id
            source = self._where.pop(sid, None)
            if source is not None:
                self.sets[source].remove(sid)
            if not row.enabled:
                cancelled.append((sid, rule.revision))
                continue
            if self._failed.get(sid) == rule.revision or rule.source not in self.sets:
                continue
            good, refused = subscriptions.runnable([rule])
            if refused:  # the hub's own check of what it runs (written around the API, or a grant taken away)
                failed.append((sid, rule.revision))
                errors |= refused
                continue
            self.sets[rule.source].place(good[0], row.owner)
            self._where[sid] = rule.source
            placed.append((good[0], row.owner))
        self.registry.capture(placed)
        self.registry.report([(r.subscription_id, r.revision) for r, _ in placed], "PLACED")
        self.registry.report(cancelled, "CANCELLED")
        self.registry.report(failed, "FAILED", errors)
        new = []
        for rule, owner in placed:
            if owner not in self._topics:
                new.append(owner)
            self._topics[owner] = rule.topic
        return new

    def _fail(self, source: str, errors: dict[str, str], revisions: dict[str, int]):
        """Rules found failing on a batch's data leave the set (FAILED until a new revision); the others go on."""
        with self.lock:
            for sid in errors:
                self.sets[source].remove(sid)
                self._where.pop(sid, None)
                self._failed[sid] = revisions[sid]
        self.registry.report([(sid, revisions[sid]) for sid in errors], "FAILED", errors)

    # -- a batch -------------------------------------------------------------------------------------------------------
    def _read(self, batch, source, fields, expressions, report=True):
        try:
            return read(batch, source, fields, [r for r, _ in expressions], self.chunk), expressions
        except Exception:
            if not expressions:
                raise
            errors = subscriptions.failing(subscriptions.events(batch, source), [r for r, _ in expressions])
            if not errors:
                raise
            if report:
                self._fail(source, errors, {r.subscription_id: r.revision for r, _ in expressions})
            expressions = [(r, o) for r, o in expressions if r.subscription_id not in errors]
            return read(batch, source, fields, [r for r, _ in expressions], self.chunk), expressions

    def _decide(self, batch, source):
        """Read and match with the live rules: (records, app -> [(rule, record)], app -> {subscription id: revision},
        the read's milliseconds)."""
        rs = self.sets[source]
        with self.lock:
            fields, expressions = set(rs.fields_seen), list(rs.expressions.values())
            live = bool(len(rs))
        if not live:
            return [], {}, {}, 0
        t0 = time.monotonic()
        rows, expressions = self._read(batch, source, fields, expressions)
        t1 = time.monotonic()
        with self.lock:
            hits = match(rows, rs.matcher, expressions, fields)
            revisions: dict[str, dict[str, int]] = {}
            for e in rs.matcher.entries.values():
                if set(e.rule.fields) <= fields:
                    revisions.setdefault(e.owner, {})[e.rule.subscription_id] = e.rule.revision
            for rule, owner in expressions:
                if rule.subscription_id not in rs.matcher.entries:
                    revisions.setdefault(owner, {})[rule.subscription_id] = rule.revision
        decided: dict[str, list] = {app: [] for app in revisions}
        for rule, owner, rec in hits:
            decided[owner].append((rule, rec))
        return rows, decided, revisions, int((t1 - t0) * 1000)

    def _rebuild(self, batch, source, revisions: dict[str, int]) -> dict[str, list]:
        """A retried batch's decisions for apps whose rules changed since its first attempt: from the revisions as they
        ran then (captured), on the same records."""
        rs = RuleSet(source)
        for rule, owner in self.registry.captured(revisions):
            rs.place(rule, owner)
        expressions = list(rs.expressions.values())
        rows, expressions = self._read(batch, source, rs.fields_seen, expressions, report=False)
        decided: dict[str, list] = {}
        for rule, owner, rec in match(rows, rs.matcher, expressions, rs.fields_seen):
            decided.setdefault(owner, []).append((rule, rec))
        return decided

    @staticmethod
    def _ids(decisions) -> list[str]:
        return [render.notification_id(r.subscription_id, r.revision, rec["_partition"], rec["_offset"])
                for r, rec in decisions]  # fmt: skip

    def _binding_id(self, app, source, query_id, batch_id) -> str:
        return f"{self.registry.hub}:{self.registry.lane}:{app}:{source}:{query_id}:{batch_id}"

    def _bind(self, batch, batch_id, source, query_id, rows, decided, revisions):
        """Bind a first attempt's decisions; for a retry, decide as the first attempt did. Returns (app -> decisions,
        the apps whose live revisions decided them)."""
        existing = self.registry.bound(source, query_id, batch_id)
        if not existing:
            offsets: dict[str, list[int]] = {}
            for rec in rows:
                lo, hi = offsets.get(str(rec["_partition"]), (rec["_offset"], rec["_offset"]))
                offsets[str(rec["_partition"])] = [min(lo, rec["_offset"]), max(hi, rec["_offset"])]
            self.registry.bind([
                {"binding_id": self._binding_id(app, source, query_id, batch_id), "app": app, "source": source,
                 "query_id": query_id, "batch_id": batch_id, "offsets": offsets, "revisions": revisions[app],
                 "n": len(decided[app]), "digest": digest(self._ids(decided[app]))}
                for app in sorted(revisions)
            ])  # fmt: skip
            self.registry.prune(source, query_id, batch_id)
            return decided, set(revisions)
        changed = {app: b for app, b in existing.items() if b["revisions"] != revisions.get(app, {})}
        bound = {s: r for b in changed.values() for s, r in b["revisions"].items()}
        rebuilt = self._rebuild(batch, source, bound) if bound else {}
        out = {}
        for app, b in existing.items():
            d = rebuilt.get(app, []) if app in changed else decided.get(app, [])
            self.registry.replayed(b["binding_id"], app not in changed, digest(self._ids(d)) == b["digest"], len(d))
            out[app] = d
        return out, set(existing) - set(changed)

    def process(self, batch, batch_id: int, source: str, query_id: str = "unknown",
                spark_ms: dict | None = None) -> dict:  # fmt: skip
        """One micro-batch of `source`'s topic (its foreach_batch_sink function). Returns the delivery's counts."""
        started, t0 = time.time(), time.monotonic()
        self.start()
        self.refresh()
        t1 = time.monotonic()
        rows, decided, revisions, collect_ms = self._decide(batch, source)
        t2 = time.monotonic()
        decided, live = self._bind(batch, batch_id, source, query_id, rows, decided, revisions)
        t3 = time.monotonic()
        msgs = []
        for app in sorted(decided):
            bid = self._binding_id(app, source, query_id, batch_id)
            for rule, rec in decided[app]:
                key, value = render.notification(rule, source, rec)
                nid = render.notification_id(rule.subscription_id, rule.revision, rec["_partition"], rec["_offset"])
                msgs.append(delivery.Msg(app, rule.topic, key, value, None, nid, bid))
        stats = self._delivery.publish(msgs)
        t4 = time.monotonic()
        self.registry.report(sorted({(s, r) for app in live for s, r in revisions.get(app, {}).items()}), "ACTIVE")
        rules = len(self.sets[source])
        self.registry.heartbeat(source, batch_id, rules)
        t5 = time.monotonic()
        newest = max((r["_source_ts"] for r in rows if r["_source_ts"] is not None), default=None)
        if newest is not None and newest.tzinfo is None:  # collected in the session time zone
            newest = newest.replace(tzinfo=ZoneInfo(batch.sparkSession.conf.get("spark.sql.session.timeZone")))
        ms = {"refresh": t1 - t0, "collect": collect_ms / 1000, "match": (t2 - t1) - collect_ms / 1000,
              "bind": t3 - t2, "deliver": t4 - t3, "status": t5 - t4}  # fmt: skip
        self.registry.progress(
            source, batch_id, rules, {k: int(v * 1000) for k, v in ms.items()} | {"compile": self._compile_ms},
            rows=len(rows), delivered=stats.get("delivered", 0), parked=stats.get("parked", 0),
            age_ms=None if newest is None else int((started - newest.timestamp()) * 1000), spark_ms=spark_ms,
        )  # fmt: skip
        return stats
