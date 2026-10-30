"""Durable guard: infra/azure keeps Event Hubs key-free and exposes every output the Makefile and scripts/uc.sh read."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STACK = ROOT / "infra/azure"


def _outputs() -> set[str]:
    return set(re.findall(r'^output "(\w+)"', (STACK / "outputs.tf").read_text(), re.M))


def _read_outputs(text: str) -> set[str]:
    return set(re.findall(r"tf_out,(\w+)\)", text)) | set(re.findall(r"\btf (\w+)\)", text))


def test_event_hubs_has_no_keys():
    tf = (STACK / "eventhubs.tf").read_text()
    assert re.search(r"local_authentication_enabled\s*=\s*false", tf), "SAS keys must stay disabled"
    assert "authorization_rule" not in tf, "no SAS rules: auth is the UC service credential (Entra ID)"


def test_access_connector_can_send_and_receive():
    tf = (STACK / "eventhubs.tf").read_text()
    assert "Azure Event Hubs Data Sender" in tf and "Azure Event Hubs Data Receiver" in tf


def test_makefile_and_uc_script_read_existing_outputs():
    used = _read_outputs((ROOT / "Makefile").read_text()) | _read_outputs((ROOT / "scripts/uc.sh").read_text())
    assert used, "expected Terraform outputs to be read"
    assert used <= _outputs(), f"reads missing outputs: {used - _outputs()}"
