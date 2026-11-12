"""The API client is generated from the contract (docs/api/openapi.json), never hand-written: openapi-python-client
writes one function per operation (get_connection, declare, get_status, put_subscription, cancel, validate, health) and
one model per schema, including the Kafka message (Notification), into generated/ (gitignored). It runs whenever the
client is missing or the contract is newer, the way Maven's openapi-generator does for the Java example.
"""

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONTRACT = HERE.parents[2] / "docs/api/openapi.json"
OUT = HERE / "generated"
PACKAGE = "hl7_subscriptions_client"


def ensure() -> bool:
    """Generate the client if it's missing or older than the contract; make it importable. True if it was generated."""
    marker = OUT / PACKAGE / "__init__.py"
    stale = not marker.exists() or CONTRACT.stat().st_mtime > marker.stat().st_mtime
    if stale:
        generator = Path(sys.executable).with_name("openapi-python-client")
        if not generator.exists():
            sys.exit("openapi-python-client isn't installed: run `uv sync` in the repo root")
        OUT.mkdir(parents=True, exist_ok=True)
        r = subprocess.run([str(generator), "generate", "--path", str(CONTRACT), "--meta", "none", "--overwrite",
                            "--output-path", str(OUT / PACKAGE)], capture_output=True, text=True)  # fmt: skip
        if r.returncode != 0:
            sys.exit(f"generating the API client from {CONTRACT} failed:\n{r.stdout}{r.stderr}")
        marker.touch()
    if str(OUT) not in sys.path:
        sys.path.insert(0, str(OUT))
    return stale
