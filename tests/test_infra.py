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


def test_workspace_managed_resource_group_is_named_per_region():
    """A deleted workspace's managed RG can outlive it. Azure's default name ignores the region, so a rebuild in another
    region collides with the leftover."""
    m = re.search(r"^\s*managed_resource_group_name\s*=\s*(.+)$", (STACK / "databricks.tf").read_text(), re.M)
    assert m and "var.location" in m.group(1)


def test_uc_rebinds_a_catalog_left_bound_to_a_deleted_workspace():
    assert "not accessible in current workspace" in UC and "update-bindings" in UC


def test_uc_refreshes_credentials_when_the_connector_is_recreated():
    """A recreated connector keeps its resource ID but gets a new identity, which UC picks up only on update."""
    assert "access_connector_principal_id" in UC and "update-credential" in UC


# The live Power BI dashboard's Fabric capacity (infra/fabric): its own tenant and subscription, so nothing is fixed.
FABRIC = ROOT / "infra/fabric"


def test_fabric_stack_is_tagged_and_hardcodes_no_tenant_or_subscription():
    tf = "".join(p.read_text() for p in FABRIC.glob("*.tf"))
    assert "owner = var.owner" in tf, "sandbox policy requires an owner tag"
    assert "RemoveAfter = var.remove_after" in tf and "YYYY-MM-DD" in tf, "the subscription deletes untagged resources"
    assert "var.subscription_id" in tf and "var.tenant_id" in tf
    assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", tf), "no hardcoded IDs"
    assert "role_assignment" not in tf, "a capacity names its admins; no role assignments needed"


def test_makefile_and_powerbi_script_read_existing_fabric_outputs():
    outputs = set(re.findall(r'^output "(\w+)"', (FABRIC / "outputs.tf").read_text(), re.M))
    used = set(re.findall(r"\$\(FAB\) output -raw (\w+)", (ROOT / "Makefile").read_text()))
    used |= set(re.findall(r'tf_output\("fabric", "(\w+)"\)', (ROOT / "infra/powerbi.py").read_text()))
    assert used and used <= outputs, f"reads missing outputs: {used - outputs}"
