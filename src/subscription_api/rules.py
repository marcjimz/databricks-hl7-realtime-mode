"""A subscription rule is Databricks SQL in one shape: SELECT <columns> FROM <one gold source> [WHERE <condition>].

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

DIALECT = "databricks"
SOURCES = frozenset({"gold_encounters", "gold_observations", "gold_orders"})  # = hl7rt.subscriptions.SOURCES
# Functions that reach outside the record. Unqualified names only: any qualified function (a UC UDF) is refused too.
DENIED_FUNCTIONS = {
    "http_request", "secret", "try_secret", "reflect", "try_reflect", "java_method", "remote_query", "vector_search",
    "current_user", "session_user", "user", "current_metastore", "is_member", "is_account_group_member",
}  # fmt: skip
DENIED_PREFIXES = ("ai_", "read_")
NOT_ALLOWED = {
    exp.Join: "a join", exp.Subquery: "a subquery", exp.Exists: "a subquery", exp.With: "a CTE", exp.Union: "UNION",
    exp.Intersect: "INTERSECT", exp.Except: "EXCEPT", exp.Group: "GROUP BY", exp.Having: "HAVING",
    exp.Order: "ORDER BY", exp.Limit: "LIMIT", exp.Distinct: "DISTINCT", exp.Window: "a window function",
    exp.Lateral: "LATERAL", exp.TableSample: "TABLESAMPLE", exp.Placeholder: "a parameter",
    exp.Parameter: "a parameter", exp.Star: "SELECT *", exp.AggFunc: "an aggregate",
}  # fmt: skip


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
    try:
        statements = [s for s in sqlglot.parse(sql, read=DIALECT) if s is not None]
    except sqlglot.errors.ParseError as e:
        raise RuleError("PARSE", str(e).splitlines()[0]) from None
    if len(statements) != 1:
        _refuse("more than one statement")
    q = statements[0]
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
    qualifier = ".".join(p for p in (table.catalog, table.db) if p)
    if qualifier and qualifier != schema:
        _refuse(f"{table.sql(dialect=DIALECT)}: only your own views ({schema}.<source>)")
    if table.name not in sources:
        _refuse(f"{table.name}: not a subscription source ({', '.join(sorted(sources))})")

    fields = []
    for e in q.expressions:
        if not isinstance(e, exp.Column):
            _refuse(f"{e.sql(dialect=DIALECT)} in SELECT (list columns only)")
        fields.append(e.name)

    for col in q.find_all(exp.Column):  # o.value_num -> value_num: the hub evaluates per record, without a table name
        col.set("table", None)
    where = q.args.get("where")
    condition = where.this.sql(dialect=DIALECT) if where else None
    canonical = exp.select(*fields).from_(table.name)
    if condition:
        canonical = canonical.where(where.this)
    return Rule(table.name, tuple(fields), condition, canonical.sql(dialect=DIALECT))


def columns(condition: str | None) -> set[str]:
    """Columns a WHERE condition reads (the hub checks them against the app's granted fields before running it)."""
    if not condition:
        return set()
    return {c.name for c in sqlglot.parse_one(condition, read=DIALECT).find_all(exp.Column)}


def check_condition(condition: str | None, granted: set[str]):
    """The hub's own check before it evaluates a stored condition: same function rules, granted columns only."""
    if not condition:
        return
    node = sqlglot.parse_one(condition, read=DIALECT)
    for cls, what in NOT_ALLOWED.items():
        if any(isinstance(n, cls) for n in node.walk()):
            _refuse(what)
    _check_functions(node)
    ungranted = columns(condition) - granted
    if ungranted:
        raise RuleError("FIELD_DENIED", f"{', '.join(sorted(ungranted))} not granted to this app")
