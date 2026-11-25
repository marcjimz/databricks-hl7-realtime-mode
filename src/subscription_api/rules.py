"""A subscription rule is Databricks SQL in one shape: SELECT <columns> FROM <one source> [WHERE <condition>], a source
being a silver entity topic or, where the hub reads Delta tables instead, one of those tables (source_tables).

Databricks compiles the SQL against the app's own views (the API runs DESCRIBE QUERY), so a misspelled or ungranted
column, a wrong source, or a syntax error comes back as Databricks' own error. This module is the only rule logic of our
own: the hub evaluates the WHERE with its own permissions, so the rule must be one filter over one record. No joins,
subqueries, CTEs, set operations, aggregation, sorting, parameters, or functions that reach outside the record (network,
secrets, JVM, AI, UDFs). Everything else is a Databricks built-in, with Databricks semantics: the same SQL in the SQL
editor returns the rows the hub delivers.
"""

from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

DIALECT = "databricks"
# = hl7rt.subscriptions.SILVER (tests/test_subscriptions.py keeps them in step)
SOURCES = frozenset(
    {"silver_encounters", "silver_observations", "silver_orders", "silver_diagnoses", "silver_documents"}
)
# Functions that reach outside the record. Unqualified names only: any qualified function (a UC UDF) is refused too.
DENIED_FUNCTIONS = {
    "http_request", "secret", "try_secret", "reflect", "try_reflect", "java_method", "remote_query", "vector_search",
    "current_user", "session_user", "user", "current_metastore", "is_member", "is_account_group_member",
    "rand", "random", "randn", "uuid", "shuffle", "monotonically_increasing_id", "spark_partition_id",
}  # fmt: skip
DENIED_PREFIXES = ("ai_", "read_")
MAX_SQL = 200_000  # characters: room for an IN list of ~10,000 patient IDs; parsing stays bounded
NOT_ALLOWED = {
    exp.Join: "a join", exp.Subquery: "a subquery", exp.Exists: "a subquery", exp.With: "a CTE", exp.Union: "UNION",
    exp.Intersect: "INTERSECT", exp.Except: "EXCEPT", exp.Group: "GROUP BY", exp.Having: "HAVING",
    exp.Order: "ORDER BY", exp.Limit: "LIMIT", exp.Distinct: "DISTINCT", exp.Window: "a window function",
    exp.Lateral: "LATERAL", exp.TableSample: "TABLESAMPLE", exp.Placeholder: "a parameter",
    exp.Parameter: "a parameter", exp.Star: "SELECT *", exp.AggFunc: "an aggregate",
}  # fmt: skip


def source_tables(spec: str) -> dict[str, str]:
    """The Delta tables a deployment subscribes over instead of the silver topics (a comma list of
    <catalog>.<schema>.<table>: the hub's hl7.subscriptions.tables, the API's HL7_SOURCE_TABLES), as source name ->
    table. A source is named after its table, lowercase as Databricks compares identifiers: rules say FROM <table>."""
    out: dict[str, str] = {}
    for table in (t.strip() for t in spec.split(",")):
        if not table:
            continue
        parts = table.split(".")
        if len(parts) != 3 or not all(p.strip() for p in parts):
            raise ValueError(f"{table!r}: a table source is <catalog>.<schema>.<table>")
        name = parts[-1].strip().lower()
        if name in out:
            raise ValueError(f"{table!r}: a second table named {name} ({out[name]}); a source names one table")
        out[name] = table
    return out


def sources(tables: str = "") -> frozenset[str]:
    """The sources a rule may name: the Delta tables in `tables` (source_tables) when there are any, else silver's."""
    return frozenset(source_tables(tables)) or SOURCES


class RuleError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code, self.message = code, message


@dataclass(frozen=True)
class Rule:
    source: str
    fields: tuple[str, ...]
    where: str | None  # canonical Databricks SQL; the hub evaluates it per record
    query: str  # canonical SELECT ... FROM <source> [WHERE ...]; paste it into the SQL editor against the app's view


def _refuse(what: str):
    raise RuleError("SQL_NOT_ALLOWED", f"{what} isn't allowed: a rule is SELECT <columns> FROM <source> WHERE <filter>")


def _check_functions(node: exp.Expression):
    for n in node.walk():
        if isinstance(n, exp.Dot) and isinstance(n.expression, exp.Anonymous):
            _refuse(f"user-defined function {n.sql(dialect=DIALECT)}")
        if isinstance(n, exp.Func) and not isinstance(n, exp.Connector):
            name = (n.name if isinstance(n, exp.Anonymous) else n.sql_name()).lower()
            if name in DENIED_FUNCTIONS or name.startswith(DENIED_PREFIXES):
                _refuse(f"{name}()")


def parse(sql: str, sources: set[str], schema: str) -> Rule:
    """The rule in `sql`, for an app whose views live in `schema` (<catalog>.sub_<app>). Raises RuleError:
    SQL_NOT_ALLOWED for a shape we won't run, PARSE when sqlglot can't parse it (ask Databricks for the real error)."""
    if len(sql) > MAX_SQL:
        _refuse(f"a rule longer than {MAX_SQL:,} characters")
    try:
        statements = [s for s in sqlglot.parse(sql, read=DIALECT) if s is not None]
    except sqlglot.errors.ParseError as e:
        raise RuleError("PARSE", str(e).splitlines()[0]) from None
    except RecursionError:
        _refuse("nesting this deep")
    if len(statements) != 1:
        _refuse("more than one statement")
    # Databricks identifiers are case-insensitive: normalize, so the stored rule and the hub's checks agree with it.
    q = normalize_identifiers(statements[0], dialect=DIALECT)
    if not isinstance(q, exp.Select):
        _refuse(f"a {q.key.upper()} statement")
    for node in q.walk():
        for cls, what in NOT_ALLOWED.items():
            if isinstance(node, cls):
                _refuse(what)
    _check_functions(q)

    tables = list(q.find_all(exp.Table))
    if len(tables) != 1 or not isinstance(tables[0].this, exp.Identifier) or tables[0].args.get("joins"):
        _refuse("anything but one source table")
    table = tables[0]
    if table.args.get("version"):
        _refuse("time travel (the hub evaluates the live stream)")
    qualifier = ".".join(p for p in (table.catalog, table.db) if p)
    if qualifier and qualifier != schema:
        _refuse(f"{table.sql(dialect=DIALECT)}: only your own views ({schema}.<source>)")
    if table.name not in sources:
        _refuse(f"{table.name}: not a subscription source ({', '.join(sorted(sources))})")

    fields = []
    for e in q.expressions:
        if not isinstance(e, exp.Column):
            _refuse(f"{e.sql(dialect=DIALECT)} in SELECT (list columns only)")
        if e.name in fields:
            _refuse(f"{e.name} twice in SELECT")
        fields.append(e.name)

    for col in q.find_all(exp.Column):  # o.value_num -> value_num: the hub evaluates per record, without a table name
        col.set("table", None)
    where = q.args.get("where")
    condition = where.this.sql(dialect=DIALECT, comments=False) if where else None
    canonical = exp.select(*fields).from_(table.name)
    if condition:
        canonical = canonical.where(where.this)
    return Rule(table.name, tuple(fields), condition, canonical.sql(dialect=DIALECT, comments=False))


def columns(condition: str | None) -> set[str]:
    """Columns a WHERE condition reads (the hub checks them against the app's granted fields before running it)."""
    if not condition:
        return set()
    return {c.name for c in sqlglot.parse_one(condition, read=DIALECT).find_all(exp.Column)}


def check_condition(condition: str | None, granted: set[str]) -> str | None:
    """The hub's own check before it evaluates a stored condition: same function rules, granted columns only. Returns
    the condition regenerated from its parsed tree (one expression, canonical), which is what the hub evaluates."""
    if not condition:
        return None
    try:
        if len(condition) > MAX_SQL:
            _refuse(f"a rule longer than {MAX_SQL:,} characters")
        statements = [s for s in sqlglot.parse(condition, read=DIALECT) if s is not None]
    except sqlglot.errors.ParseError as e:
        raise RuleError("SQL_NOT_ALLOWED", f"not one expression: {str(e).splitlines()[0]}") from None
    except RecursionError:
        _refuse("nesting this deep")
    if len(statements) != 1 or not isinstance(statements[0], exp.Condition):
        _refuse("anything but one filter expression")
    node = normalize_identifiers(statements[0], dialect=DIALECT)
    for cls, what in NOT_ALLOWED.items():
        if any(isinstance(n, cls) for n in node.walk()):
            _refuse(what)
    _check_functions(node)
    ungranted = {c.name for c in node.find_all(exp.Column)} - granted
    if ungranted:
        raise RuleError("FIELD_DENIED", f"{', '.join(sorted(ungranted))} not granted to this app")
    return node.sql(dialect=DIALECT, comments=False)


def shape(query: str) -> str:
    """The query with its literals blanked (a string is '', a number 0) and each IN list cut to one of each kind: what
    Databricks' compile depends on. Two rules of one shape compile the same way (an edited patient list, say)."""

    def blank(node):
        if isinstance(node, exp.Literal):
            return exp.Literal.string("") if node.is_string else exp.Literal.number(0)
        if isinstance(node, exp.In) and not node.args.get("query"):
            kinds = {}
            for v in node.expressions:
                kinds.setdefault(type(v).__name__ + str(getattr(v, "is_string", "")), v)
            node.set("expressions", list(kinds.values()))
        return node

    tree = sqlglot.parse_one(query, read=DIALECT)
    return tree.transform(blank).sql(dialect=DIALECT, comments=False)


def literal(value: str) -> str:
    """`value` as a Databricks SQL string literal, escaped."""
    return exp.Literal.string(value).sql(dialect=DIALECT)


def identifier(name: str) -> str:
    """`name` as a quoted Databricks SQL identifier."""
    return exp.to_identifier(name, quoted=True).sql(dialect=DIALECT)


# ---------------------------------------------------------------------------------------------------------------------
# The hub's fast path. Most rules are a conjunction of simple comparisons between a column and a literal, e.g.
# observation_cd_desc = 'Potassium' AND value_num >= 6.5. Those run as data (one row per rule, one fixed plan for any
# number of them) instead of as one more expression in the plan. Anything else runs as an expression. Either way the
# rule means what Databricks SQL says it means; this only picks how the hub evaluates it.
_COMPARE = {exp.EQ: "eq", exp.NEQ: "ne", exp.LT: "lt", exp.LTE: "le", exp.GT: "gt", exp.GTE: "ge"}
_FLIP = {"lt": "gt", "le": "ge", "gt": "lt", "ge": "le", "eq": "eq", "ne": "ne"}


def _literal_of(node, kind: str):
    """The Python value of a string or number literal matching the column's kind ('string' or 'number'), else None."""
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
        value = _literal_of(node.this, kind)
        return -value if value is not None else None
    if not isinstance(node, exp.Literal):
        return None
    if kind == "string" and node.is_string:
        return node.this
    if kind == "number" and not node.is_string:
        try:
            return float(node.this)
        except ValueError:
            return None
    return None


MAX_DISJUNCTS = 64  # branches of a rule's OR, flattened; past this the rule is evaluated as an expression


class _NotSimple(Exception):
    pass


def disjuncts(condition: str | None, kinds: dict[str, str]) -> list[list[dict]] | None:
    """The condition as an OR of AND groups of simple clauses, [[{col, op, str|num|strs|nums}, ...], ...], or None if
    it isn't one (or has more than MAX_DISJUNCTS groups). A record matches when every clause of some group holds.

    kinds: column -> 'string' or 'number' (other types, e.g. timestamps, keep the expression path). A clause compares
    one column with a literal of its own kind: =, <>, <, <=, >, >=, BETWEEN, [NOT] IN (literals), IS [NOT] NULL. AND
    and OR distribute in SQL's three-valued logic too, so the flattened form matches exactly the same records.
    """
    if not condition:
        return [[]]
    try:
        return _dnf(sqlglot.parse_one(condition, read=DIALECT), kinds)
    except _NotSimple:
        return None


def _dnf(n, kinds: dict[str, str]) -> list[list[dict]]:
    while isinstance(n, exp.Paren):
        n = n.this
    if isinstance(n, exp.And):
        out = [a + b for a in _dnf(n.this, kinds) for b in _dnf(n.expression, kinds)]
    elif isinstance(n, exp.Or):
        out = _dnf(n.this, kinds) + _dnf(n.expression, kinds)
    else:
        out = [_clause(n, kinds)]
    if len(out) > MAX_DISJUNCTS:
        raise _NotSimple
    return out


def _clause(c, kinds: dict[str, str]) -> list[dict]:
    """One comparison as clauses (BETWEEN is two), or _NotSimple."""
    negate = isinstance(c, exp.Not)
    inner = c.this if negate else c
    while isinstance(inner, exp.Paren):
        inner = inner.this
    if isinstance(inner, exp.Is) and isinstance(inner.this, exp.Column) and isinstance(inner.expression, exp.Null):
        if inner.this.name not in kinds:
            raise _NotSimple
        return [{"col": inner.this.name, "op": "not_null" if negate else "null"}]
    if isinstance(inner, exp.In) and isinstance(inner.this, exp.Column) and not inner.args.get("query"):
        col = inner.this.name
        kind = kinds.get(col)
        values = [_literal_of(v, kind) for v in inner.expressions] if kind else [None]
        if not values or any(v is None for v in values):
            raise _NotSimple
        key = "strs" if kind == "string" else "nums"
        return [{"col": col, "op": ("nin_" if negate else "in_") + kind[0], key: values}]
    if negate:
        raise _NotSimple  # NOT of anything else keeps the expression path
    if isinstance(inner, exp.Between) and isinstance(inner.this, exp.Column):
        col, kind = inner.this.name, kinds.get(inner.this.name)
        low, high = _literal_of(inner.args["low"], kind or ""), _literal_of(inner.args["high"], kind or "")
        if kind is None or low is None or high is None:
            raise _NotSimple
        key = "str" if kind == "string" else "num"
        return [{"col": col, "op": "ge_" + kind[0], key: low}, {"col": col, "op": "le_" + kind[0], key: high}]
    op = _COMPARE.get(type(inner))
    if op is None:
        raise _NotSimple
    left, right = inner.this, inner.expression
    if not isinstance(left, exp.Column):
        left, right, op = right, left, _FLIP[op]
    if not isinstance(left, exp.Column) or left.name not in kinds:
        raise _NotSimple
    kind = kinds[left.name]
    value = _literal_of(right, kind)
    if value is None:
        raise _NotSimple
    return [{"col": left.name, "op": f"{op}_{kind[0]}", ("str" if kind == "string" else "num"): value}]


# Index keys, most selective first: identifiers (one patient, visit, order), then codes, then locations, then any other
# string (message types, code systems). Ties go to the clause with fewer values.
_IDENTIFIERS = {"patient_id", "visit_id", "account_id", "msg_control_id", "order_id", "document_id"}


def index_key(group: list[dict]) -> dict | None:
    """The clause of an AND group to index it on: its most selective string = or IN, or None if it has none."""
    keys = [c for c in group if c["op"] in ("eq_s", "in_s")]

    def rank(c):
        col = c["col"]
        tier = 0 if col in _IDENTIFIERS else 1 if col.endswith("_cd") else 2 if "point_of_care" in col else 3
        return tier, len(c.get("strs") or [c.get("str")])

    return min(keys, key=rank, default=None)
