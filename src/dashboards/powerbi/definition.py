"""The live Power BI dashboard, as data: a DirectQuery semantic model over the views in views.sql (one table per view)
and a three-page report (Latency, Clinical, Subscriptions) that refreshes itself whenever the newest gold record
changes. infra/powerbi.py (`make powerbi`) renders both into Fabric item definitions (TMDL and PBIR) and deploys them,
so nothing is built by hand in Power BI Desktop. Host, warehouse, catalog, and model ID are filled in at deploy time.
"""

import base64
import json

# view -> its columns and their Databricks types, in the view's order (make powerbi checks them against Unity Catalog)
TABLES = {
    "hop_latency": {"hop": "string", "messages": "bigint", "p50_ms": "double", "p95_ms": "double", "p99_ms": "double",
                    "max_ms": "double", "under_200ms": "decimal"},
    "hop_p99_by_minute": {"hop": "string", "minute": "timestamp", "p99_ms": "double", "messages": "bigint"},
    "headline": {"msgs_per_sec": "decimal", "raw_to_gold_p99_ms": "double", "gold_observations": "bigint",
                 "quarantined": "bigint"},
    "last_update": {"updated_at": "timestamp"},
    "messages_by_minute": {"minute": "timestamp", "msg_type": "string", "messages": "bigint"},
    "critical_labs_by_facility": {"facility": "string", "observation": "string", "abnormal_flag": "string",
                                  "results": "bigint"},
    "latest_critical_labs": {"gold_at": "timestamp", "patient_id": "string", "facility": "string",
                             "observation": "string", "value_num": "double", "units": "string",
                             "abnormal_flag": "string", "raw_to_gold_ms": "double"},
    "quarantine_reasons": {"reason": "string", "messages": "bigint"},
    "subscriptions": {"app_name": "string", "subscription": "string", "revision": "int", "source": "string",
                      "state": "string", "rule": "string", "registered_at": "timestamp", "confirmed_at": "timestamp",
                      "seconds_to_confirm": "double"},
    "hub": {"source": "string", "status": "string", "seconds_since_last_batch": "int", "last_batch_at": "timestamp",
            "rules": "int"},
}  # fmt: skip

# table -> {measure: (DAX, format)}
MEASURES = {
    "headline": {
        "Messages per second": ("MAX(headline[msgs_per_sec])", "0.0"),
        "Raw to gold p99 ms": ("MAX(headline[raw_to_gold_p99_ms])", "0"),
        "Gold observations 15 min": ("MAX(headline[gold_observations])", "#,0"),
        "Quarantined 15 min": ("MAX(headline[quarantined])", "#,0"),
    },
    "last_update": {"Last update": ("MAX(last_update[updated_at])", "General Date")},
    "hop_p99_by_minute": {"p99 ms": ("MAX(hop_p99_by_minute[p99_ms])", "0")},
    "messages_by_minute": {"Parsed messages": ("SUM(messages_by_minute[messages])", "#,0")},
    "critical_labs_by_facility": {"Critical results": ("SUM(critical_labs_by_facility[results])", "#,0")},
    "quarantine_reasons": {"Quarantined messages": ("SUM(quarantine_reasons[messages])", "#,0")},
    "subscriptions": {"Active subscriptions": ("COUNTROWS(subscriptions) + 0", "#,0")},
    "hub": {"Stale hub sources": ('CALCULATE(COUNTROWS(hub), hub[status] = "STALE") + 0', "0")},
}

# Every page refreshes when this measure changes (Power BI allows one change-detection measure per model). It moves with
# every new gold record, so with live data the pages refresh at the check interval.
CHANGE_DETECTION = ("last_update", "Last update")
CHECK_EVERY_SECONDS = 5

# page -> visuals: (type, title, (x, y, width, height), {role: [(table, field)]}). A field is a measure if it's in
# MEASURES, else a column. Roles: card Values; tableEx Values; lineChart / columnChart / clusteredBarChart Category, Y,
# Series.
PAGES = {
    "Latency": [
        ("card", "Messages per second", (10, 10, 305, 110), {"Values": [("headline", "Messages per second")]}),
        ("card", "Raw to gold p99 (ms)", (325, 10, 305, 110), {"Values": [("headline", "Raw to gold p99 ms")]}),
        ("card", "Gold observations, 15 min", (640, 10, 305, 110),
         {"Values": [("headline", "Gold observations 15 min")]}),
        ("card", "Quarantined, 15 min", (955, 10, 315, 110), {"Values": [("headline", "Quarantined 15 min")]}),
        ("tableEx", "Per-hop latency, last 15 minutes (target: p99 under 200 ms)", (10, 130, 1260, 170),
         {"Values": [("hop_latency", c) for c in TABLES["hop_latency"]]}),
        ("lineChart", "p99 by minute, last hour (ms)", (10, 310, 1260, 400),
         {"Category": [("hop_p99_by_minute", "minute")], "Y": [("hop_p99_by_minute", "p99 ms")],
          "Series": [("hop_p99_by_minute", "hop")]}),
    ],
    "Clinical": [
        ("columnChart", "Parsed messages per minute by type, last hour", (10, 10, 630, 340),
         {"Category": [("messages_by_minute", "minute")], "Y": [("messages_by_minute", "Parsed messages")],
          "Series": [("messages_by_minute", "msg_type")]}),
        ("clusteredBarChart", "Critical lab results (HH/LL) by facility, last hour", (650, 10, 620, 340),
         {"Category": [("critical_labs_by_facility", "facility")],
          "Y": [("critical_labs_by_facility", "Critical results")],
          "Series": [("critical_labs_by_facility", "abnormal_flag")]}),
        ("tableEx", "Newest critical lab results", (10, 360, 900, 350),
         {"Values": [("latest_critical_labs", c) for c in TABLES["latest_critical_labs"]]}),
        ("clusteredBarChart", "Quarantined by reason, last hour", (920, 360, 350, 350),
         {"Category": [("quarantine_reasons", "reason")], "Y": [("quarantine_reasons", "Quarantined messages")]}),
    ],
    "Subscriptions": [
        ("card", "Active subscriptions", (10, 10, 305, 110), {"Values": [("subscriptions", "Active subscriptions")]}),
        ("card", "Stale hub sources", (325, 10, 305, 110), {"Values": [("hub", "Stale hub sources")]}),
        ("tableEx", "Subscription hub heartbeat", (10, 130, 1260, 170),
         {"Values": [("hub", c) for c in TABLES["hub"]]}),
        ("tableEx", "Subscriptions by app (not cancelled)", (10, 310, 1260, 400),
         {"Values": [("subscriptions", c) for c in TABLES["subscriptions"]]}),
    ],
}  # fmt: skip

_TMDL_TYPES = {"string": "string", "bigint": "int64", "int": "int64", "double": "double", "decimal": "decimal",
               "timestamp": "dateTime"}  # fmt: skip
_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"


def _part(path: str, content) -> dict:
    text = content if isinstance(content, str) else json.dumps(content, indent=2, ensure_ascii=False)
    return {"path": path, "payload": base64.b64encode(text.encode()).decode(), "payloadType": "InlineBase64"}


def _name(n: str) -> str:
    """A TMDL object name: quoted unless it's a plain identifier."""
    return n if n.replace("_", "").isalnum() else "'" + n.replace("'", "''") + "'"


def _table(view: str) -> str:
    lines = [f"table {view}", ""]
    for measure, (dax, fmt) in MEASURES.get(view, {}).items():
        lines += [f"\tmeasure {_name(measure)} = {dax}", f"\t\tformatString: {fmt}", ""]
    for column, kind in TABLES[view].items():
        lines += [f"\tcolumn {column}", f"\t\tdataType: {_TMDL_TYPES[kind]}", f"\t\tsourceColumn: {column}",
                  "\t\tsummarizeBy: none", ""]  # fmt: skip
    lines += [
        f"\tpartition {view} = m",
        "\t\tmode: directQuery",
        "\t\tsource =",
        "\t\t\t\tlet",
        "\t\t\t\t    Source = Databricks.Catalogs(Host, HttpPath, [Catalog = null, Database = null, "
        'EnableAutomaticProxyDiscovery = null, Implementation = "2.0"]),',
        '\t\t\t\t    Views = Source{[Name = Catalog, Kind = "Database"]}[Data]'
        '{[Name = "powerbi", Kind = "Schema"]}[Data],',
        f'\t\t\t\t    View = Views{{[Name = "{view}", Kind = "View"]}}[Data]',
        "\t\t\t\tin",
        "\t\t\t\t    View",
        "",
    ]
    return "\n".join(lines)


def _parameter(name: str, value: str) -> str:
    return (f'expression {name} = "{value}" meta [IsParameterQuery = true, Type = "Text", '
            "IsParameterQueryRequired = true]\n")  # fmt: skip


def model_parts(host: str, http_path: str, catalog: str) -> list[dict]:
    """The semantic model's TMDL definition: one DirectQuery table per view, read through the Azure Databricks
    connector from the SQL warehouse at host + http_path."""
    model = "model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n"
    model += "\tdiscourageImplicitMeasures\n\n"
    model += "".join(f"ref table {v}\n" for v in TABLES)
    return [
        _part("definition.pbism", {"version": "4.2", "settings": {}}),
        _part("definition/database.tmdl", "database\n\tcompatibilityLevel: 1604\n"),
        _part("definition/model.tmdl", model),
        _part("definition/expressions.tmdl", "\n".join(
            _parameter(n, v) for n, v in (("Host", host), ("HttpPath", http_path), ("Catalog", catalog)))),
        *(_part(f"definition/tables/{v}.tmdl", _table(v)) for v in TABLES),
    ]  # fmt: skip


def _literal(v: str) -> dict:
    return {"expr": {"Literal": {"Value": v}}}


def _field(table: str, name: str) -> dict:
    kind = "Measure" if name in MEASURES.get(table, {}) else "Column"
    return {kind: {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}}


def _visual(name: str, kind: str, title: str, box, roles) -> dict:
    x, y, w, h = box
    state = {role: {"projections": [{"field": _field(t, f), "queryRef": f"{t}.{f}", "nativeQueryRef": f}
                                    for t, f in fields]} for role, fields in roles.items()}  # fmt: skip
    return {
        "$schema": f"{_SCHEMA}/visualContainer/2.0.0/schema.json",
        "name": name,
        "position": {"x": x, "y": y, "z": 0, "width": w, "height": h},
        "visual": {
            "visualType": kind,
            "query": {"queryState": state},
            "visualContainerObjects": {"title": [{"properties": {
                "show": _literal("true"), "text": _literal("'" + title.replace("'", "''") + "'")}}]},
        },
    }  # fmt: skip


def _page_id(page: str) -> str:
    return page.lower()


def report_parts(model_id: str) -> list[dict]:
    """The report's PBIR definition, bound to the semantic model model_id."""
    table, measure = CHANGE_DETECTION
    refresh = {"pageRefresh": [{"properties": {
        "show": _literal("true"),
        "refreshType": _literal("'ChangeDetection'"),
        "measure": {"expr": _field(table, measure)},
        "checkEvery": _literal(f"{CHECK_EVERY_SECONDS}D"),
    }}]}  # fmt: skip
    parts = [
        _part("definition.pbir", {
            "$schema": f"{_SCHEMA}Properties/2.0.0/schema.json", "version": "4.0",
            "datasetReference": {"byConnection": {"connectionString": f"semanticmodelid={model_id}"}}}),
        _part("definition/version.json", {
            "$schema": f"{_SCHEMA}/versionMetadata/1.0.0/schema.json", "version": "2.0.0"}),
        _part("definition/report.json", {
            "$schema": f"{_SCHEMA}/report/3.1.0/schema.json",
            "themeCollection": {"baseTheme": {"name": "CY25SU12", "type": "SharedResources", "reportVersionAtImport": {
                "visual": "2.5.0", "report": "3.1.0", "page": "2.3.0"}}},
            "resourcePackages": [{"name": "SharedResources", "type": "SharedResources", "items": [
                {"name": "CY25SU12", "path": "BaseThemes/CY25SU12.json", "type": "BaseTheme"}]}]}),
        _part("definition/pages/pages.json", {
            "$schema": f"{_SCHEMA}/pagesMetadata/1.0.0/schema.json",
            "pageOrder": [_page_id(p) for p in PAGES], "activePageName": _page_id(next(iter(PAGES)))}),
    ]  # fmt: skip
    for page, visuals in PAGES.items():
        pid = _page_id(page)
        parts.append(_part(f"definition/pages/{pid}/page.json", {
            "$schema": f"{_SCHEMA}/page/2.0.0/schema.json", "name": pid, "displayName": page,
            "displayOption": "FitToPage", "width": 1280, "height": 720, "objects": refresh}))  # fmt: skip
        for i, (kind, title, box, roles) in enumerate(visuals):
            name = f"{pid}_{i:02d}"
            parts.append(_part(f"definition/pages/{pid}/visuals/{name}/visual.json",
                               _visual(name, kind, title, box, roles)))  # fmt: skip
    return parts
