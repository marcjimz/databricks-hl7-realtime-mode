"""Durable guard: no Event Hubs connection strings or SAS keys are ever committed."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = [
    re.compile(r"Endpoint=sb://[^\s\"']+;SharedAccessKeyName=[^\s\"';]+;SharedAccessKey=[A-Za-z0-9+/=]{20,}"),
    re.compile(r"SharedAccessKey=[A-Za-z0-9+/=]{20,}"),
]


def _tracked_files():
    out = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=ROOT,
                         capture_output=True, text=True, check=True).stdout  # fmt: skip
    return [ROOT / p for p in out.splitlines() if (ROOT / p).is_file()]


def test_no_connection_strings_in_repo():
    hits = []
    for f in _tracked_files():
        try:
            text = f.read_text(errors="ignore")
        except OSError:
            continue
        hits += [f"{f.relative_to(ROOT)}" for rx in FORBIDDEN if rx.search(text)]
    assert not hits, f"secret-looking values found in: {sorted(set(hits))}"
