"""Matching a source's simple rules against a collected micro-batch, on the driver, with Databricks SQL's semantics.

A simple rule is what rules.disjuncts returns: an OR of AND groups, each clause a column against a literal. This is
Spark SQL's three-valued logic in Python (tests/test_matching.py compares it with the Spark expression path):
- a comparison with NULL is unknown, so its group doesn't hold (NOT IN of NULL too);
- numeric columns compare as DOUBLE; NaN = NaN, and NaN sorts above every number (Spark's ordering);
- strings compare by code point (Spark's binary UTF-8 order).
A record is delivered once per rule, when any of the rule's groups holds.

Each group is indexed on its most selective string = or IN (rules.index_key), in a hash map: a record only meets the
groups that name one of its values (and the few groups with no such clause). Placing a revision touches only its own
entries, so an edited patient list changes one rule, not the set. Anything else (a function, a timestamp comparison,
more than MAX_DISJUNCTS groups) is refused with RuleNotSimple: the hub evaluates it as a Spark expression instead.
"""

import math
from dataclasses import dataclass

from hl7rt import subscriptions
from subscription_api.rules import index_key


class RuleNotSimple(ValueError):
    """The rule isn't an OR of simple AND groups: the Spark expression path's job, not the driver's."""


def _cmp_n(name: str, v: float, lit: float) -> bool:
    if math.isnan(v) or math.isnan(lit):  # Spark: NaN = NaN, and NaN is greater than any other value
        a, b = (1, 0.0) if math.isnan(v) else (0, v), (1, 0.0) if math.isnan(lit) else (0, lit)
    else:
        a, b = v, lit
    if name == "eq":
        return a == b
    if name == "ne":
        return a != b
    if name == "lt":
        return a < b
    if name == "le":
        return a <= b
    if name == "gt":
        return a > b
    return a >= b  # ge


def holds(c: dict, rec: dict) -> bool:
    """One clause on one record: True only when SQL would say TRUE (unknown is not a match)."""
    op, v = c["op"], rec.get(c["col"])
    if op == "null":
        return v is None
    if op == "not_null":
        return v is not None
    if v is None:
        return False
    name, kind = op.rsplit("_", 1)
    if kind == "n":
        v = float(v)
        if name in ("in", "nin"):
            hit = any(_cmp_n("eq", v, x) for x in c["nums"])
            return hit if name == "in" else not hit
        return _cmp_n(name, v, c["num"])
    if not isinstance(v, str):
        v = str(v)
    if name == "in":
        return v in c["strs"]
    if name == "nin":
        return v not in c["strs"]
    lit = c["str"]
    return {"eq": v == lit, "ne": v != lit, "lt": v < lit, "le": v <= lit, "gt": v > lit, "ge": v >= lit}[name]


@dataclass
class Entry:
    rule: subscriptions.Rule
    owner: str  # the app role whose rule it is
    groups: list[list[dict]]  # the rule's AND groups


class Matcher:
    """One source's simple rules, indexed by (column, value) of each group's most selective = / IN."""

    def __init__(self):
        self.entries: dict[str, Entry] = {}
        self._index: dict[tuple[str, str], set[tuple[str, int]]] = {}  # (column, value) -> {(sid, group)}
        self._scan: set[tuple[str, int]] = set()  # groups with no key: checked against every record
        self._keys: dict[str, list] = {}  # sid -> where its groups are filed, to take them out again
        self._columns: dict[str, int] = {}  # key column -> how many index entries use it

    def place(self, rule: subscriptions.Rule, owner: str) -> None:
        """Add or replace one rule revision (checked: subscriptions.runnable). A revision that isn't simple raises
        RuleNotSimple, and the rule's earlier revision is taken out first: it must not keep running here."""
        groups = subscriptions._groups(rule)
        self.remove(rule.subscription_id)
        if groups is None:
            raise RuleNotSimple(rule.subscription_id)
        sid, filed = rule.subscription_id, []
        for d, group in enumerate(groups):
            k = index_key(group)
            if k is None:
                self._scan.add((sid, d))
                filed.append((None, d))
                continue
            for v in [k["str"]] if k["op"] == "eq_s" else k["strs"]:
                key = (k["col"], v)
                bucket = self._index.setdefault(key, set())
                if (sid, d) not in bucket:
                    bucket.add((sid, d))
                    self._columns[k["col"]] = self._columns.get(k["col"], 0) + 1
                    filed.append((key, d))
        self.entries[sid] = Entry(rule, owner, groups)
        self._keys[sid] = filed

    def remove(self, sid: str) -> None:
        for key, d in self._keys.pop(sid, ()):
            if key is None:
                self._scan.discard((sid, d))
                continue
            bucket = self._index.get(key)
            if bucket is not None and (sid, d) in bucket:
                bucket.discard((sid, d))
                if not bucket:
                    del self._index[key]
                self._columns[key[0]] -= 1
                if not self._columns[key[0]]:
                    del self._columns[key[0]]
        self.entries.pop(sid, None)

    def match(self, rec: dict) -> list[Entry]:
        """The rules this record matches, each once (for the first of its groups that holds), by subscription ID."""
        cand = set(self._scan)
        for col in self._columns:
            v = rec.get(col)
            if v is not None:
                cand |= self._index.get((col, v), set())
        hits = {}
        for sid, d in sorted(cand):
            if sid in hits:
                continue
            e = self.entries[sid]
            if all(holds(c, rec) for c in e.groups[d]):
                hits[sid] = e
        return list(hits.values())

    def index_size(self) -> int:
        """Index entries: one per (group, key value)."""
        return sum(len(b) for b in self._index.values())

    def fields(self) -> set[str]:
        """The fields some placed rule selects: what the batch read renders (render.members)."""
        return {f for e in self.entries.values() for f in e.rule.fields}
