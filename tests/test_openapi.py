"""The Subscription API's published contract (docs/api/openapi.json) is exactly what the API serves.

Any language can generate a client from that file, so it must not drift. After changing the API, regenerate it:
    uv run python tests/test_openapi.py
"""

import json
import pathlib

from subscription_api.app import app

CONTRACT = pathlib.Path(__file__).resolve().parent.parent / "docs" / "api" / "openapi.json"


def _spec() -> dict:
    return json.loads(json.dumps(app.openapi()))


def test_the_published_contract_is_what_the_api_serves():
    assert CONTRACT.exists(), "docs/api/openapi.json is missing: run `uv run python tests/test_openapi.py`"
    assert json.loads(CONTRACT.read_text()) == _spec(), (
        "docs/api/openapi.json is out of date: run `uv run python tests/test_openapi.py`"
    )


def test_every_operation_documents_its_response_and_errors():
    for path, operations in _spec()["paths"].items():
        for method, op in operations.items():
            ok = op["responses"]["200"]["content"]["application/json"]["schema"]
            assert ok, f"{method.upper()} {path}: no response schema"
            if path != "/api/v1/health":
                assert {"401", "403"} <= set(op["responses"]), f"{method.upper()} {path}: auth errors undocumented"


def test_clients_never_send_the_identity_header():
    """The Apps proxy sets the caller's identity and overwrites a forged copy, so it isn't a client parameter."""
    for path, operations in _spec()["paths"].items():
        for method, op in operations.items():
            names = {p["name"].lower() for p in op.get("parameters", [])}
            assert "x-forwarded-email" not in names, f"{method.upper()} {path} exposes the proxy's identity header"


if __name__ == "__main__":
    CONTRACT.parent.mkdir(parents=True, exist_ok=True)
    CONTRACT.write_text(json.dumps(_spec(), indent=2) + "\n")
    print(f"wrote {CONTRACT}")
