"""A connection to the Lakebase registry as the current Databricks identity: a short-lived database credential, no
password. Shared by the Subscription API (as its app's service principal) and the hub (as the pipeline's identity).

pg8000 is pure Python; psycopg[binary] aborted the serverless pipeline process (native library clash).
"""

import ssl

TOKEN_SECONDS = 2700  # database credentials last an hour; reconnect well before


def connect(endpoint: str, database: str):
    import pg8000.native
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.postgres.get_endpoint(name=endpoint).status.hosts.host
    token = w.postgres.generate_database_credential(endpoint=endpoint).token
    return pg8000.native.Connection(w.current_user.me().user_name, host=host, database=database, password=token,
                                    ssl_context=ssl.create_default_context())  # fmt: skip
