"""What the app does with each notification on its hub (the contract's Notification, generated from
docs/api/openapi.json). It prints only the subscription revisions this run registered, once each (the transport is
at-least-once, so it deduplicates on the stable notificationId), and counts the rest as skipped. The consumer group
resumes where it last committed and never skips, so after time away it first works through the backlog: counted, with
progress, rather than printed line by line. Commits only after handling, so a crash replays. (= the Java Listener.)
"""

import json
import time
from datetime import datetime

from confluent_kafka import KafkaError, KafkaException
from hl7_subscriptions_client.models import Notification

BACKLOG_AFTER = 120  # seconds: a gold record older than this means the group is catching up
REPORT_EVERY = 10  # seconds
SEEN_LIMIT = 100_000


# The delivery rules, on the contract's Notification -------------------------------------------------------------------
def parse(value) -> Notification:
    """One message value as a Notification; rejects one that isn't the contract's shape."""
    try:
        n = Notification.from_dict(json.loads(value))
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        raise ValueError(f"not a notification: {e!r}") from e
    if not n.notification_id or not n.subscription_id:
        raise ValueError("notification without notificationId/subscriptionId")
    return n


def is_current(n: Notification, registered: dict[str, int]) -> bool:
    """True if it matches a subscription revision this run registered (subscriptionId -> revision). Anything else is
    backlog: an earlier revision's matches, or a replaced subscription that reused the name."""
    return registered.get(n.subscription_id) == n.revision


def is_backlog(n: Notification, now: float, after: float) -> bool:
    """True if its gold record is older than `after` seconds: the group is catching up, not reading live data."""
    try:
        return datetime.fromisoformat(n.gold_ts).timestamp() < now - after
    except (TypeError, ValueError):
        return False


def payload_json(n: Notification) -> str:
    """The payload as compact JSON, the way the hub wrote it."""
    return json.dumps(n.payload.to_dict(), separators=(",", ":"))


class Listener:
    def __init__(self, current: dict[str, int], group: str, out, now: float):
        self.current, self.group, self.out = dict(current), group, out
        self.seen: set[str] = set()
        self.skipped = self.replayed = 0
        self.live = False
        self.replay_from = self.replay_at = None
        self.next_report = now + REPORT_EVERY

    def _print(self, line: str):
        print(line, file=self.out, flush=True)

    def on_record(self, value, now: float):
        """Handle one record's value from the hub."""
        n = parse(value)
        if not is_current(n, self.current):
            self.skipped += 1
            return
        if n.notification_id in self.seen:
            return
        self.seen.add(n.notification_id)
        if len(self.seen) > SEEN_LIMIT:
            self.seen.clear()
        if not self.live and is_backlog(n, now, BACKLOG_AFTER):
            if self.replay_from is None:
                self.replay_from = n.gold_ts
                self._print(f"catching up from {self.replay_from}, where consumer group {self.group} last stopped")
            self.replayed += 1
            self.replay_at = n.gold_ts
            return
        if not self.live and self.replay_from is not None:
            self._print(f"caught up   {self.replayed} backlog notifications ({self.replay_from} to {self.replay_at}); "
                        "live from here")  # fmt: skip
        self.live = True
        self._print(f"{n.gold_ts}  {n.subscription:<22} rev {n.revision}  {payload_json(n)}")

    def on_tick(self, now: float):
        """Periodic progress: backlog catch-up and skipped counts, at most every REPORT_EVERY seconds."""
        if now < self.next_report:
            return
        if not self.live and self.replay_from is not None:
            self._print(f"catching up {self.replayed} so far, at {self.replay_at}")
        if self.skipped:
            self._print(f"skipped     {self.skipped} notifications for other subscriptions or earlier revisions")
            self.skipped = 0
        self.next_report = now + REPORT_EVERY

    def run(self, consumer, stop_at: float, health_check, health_every: float):
        """Consume until stop_at: handle, then commit (a crash replays, never skips); run health_check every
        health_every seconds."""
        next_check = time.time() + health_every
        while time.time() < stop_at:
            messages = consumer.consume(num_messages=500, timeout=1.0)
            for m in messages:
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(m.error())
                self.on_record(m.value(), time.time())
            self.on_tick(time.time())
            if messages:
                consumer.commit(asynchronous=False)
            if time.time() > next_check:
                health_check()
                next_check = time.time() + health_every
