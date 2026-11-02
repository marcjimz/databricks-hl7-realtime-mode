"""Durable guard: infra/azure keeps Event Hubs key-free and exposes every output the Makefile and infra/uc.sh read."""

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
    used = _read_outputs((ROOT / "Makefile").read_text()) | _read_outputs((ROOT / "infra/uc.sh").read_text())
    assert used, "expected Terraform outputs to be read"
    assert used <= _outputs(), f"reads missing outputs: {used - _outputs()}"


# Lessons from replaying a real sandbox wipe (resource group deleted, then `make up`). See AGENTS.md.
UC = (ROOT / "infra/uc.sh").read_text()


def test_workspace_managed_resource_group_name_is_not_fixed():
    """The wipe can leave the old managed RG behind; a fixed name would block the rebuild."""
    assert not re.search(r"^\s*managed_resource_group_name\s*=", (STACK / "databricks.tf").read_text(), re.M)


def test_uc_rebinds_a_catalog_left_bound_to_a_deleted_workspace():
    assert "not accessible in current workspace" in UC and "update-bindings" in UC


def test_uc_refreshes_credentials_when_the_connector_is_recreated():
    """A recreated connector keeps its resource ID but gets a new identity, which UC picks up only on update."""
    assert "access_connector_principal_id" in UC and "update-credential" in UC
