"""Durable guard: infra/azure keeps Event Hubs key-free and exposes every output the Makefile and infra/uc.sh read."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STACK = ROOT / "infra/azure"


def _outputs() -> set[str]:
    return set(re.findall(r'^output "(\w+)"', (STACK / "outputs.tf").read_text(), re.M))


def _read_outputs(text: str) -> set[str]:
    return (
        set(re.findall(r"tf_out,(\w+)\)", text))
        | set(re.findall(r"\btf (\w+)\)", text))
        | set(re.findall(r"output -json (\w+)", text))
        | set(re.findall(r'tf_output\("(\w+)"\)', text))
    )


def test_event_hubs_has_no_keys():
    tf = (STACK / "eventhubs.tf").read_text()
    assert re.search(r"local_authentication_enabled\s*=\s*false", tf), "SAS keys must stay disabled"
    assert "authorization_rule" not in tf, "no SAS rules: auth is the UC service credential (Entra ID)"


def test_access_connector_can_send_and_receive():
    tf = (STACK / "eventhubs.tf").read_text()
    assert "Azure Event Hubs Data Sender" in tf and "Azure Event Hubs Data Receiver" in tf


def test_makefile_uc_and_registry_scripts_read_existing_outputs():
    used = set()
    for f in ("Makefile", "infra/uc.sh", "infra/registry.py"):
        used |= _read_outputs((ROOT / f).read_text())
    assert {"subscriber_apps", "subscriber_hubs"} <= used, "expected the subscriber outputs to be read"
    assert used <= _outputs(), f"reads missing outputs: {used - _outputs()}"


# Team subscription hubs (bring your own hub, hubs.tf): each hub's identity reads silver and writes only its apps' hubs.
HUBS_TF = (STACK / "hubs.tf").read_text() if (STACK / "hubs.tf").exists() else ""


def _role_assignments(tf: str) -> dict[str, str]:
    """resource name -> its body, for every azurerm_role_assignment in `tf`."""
    return dict(re.findall(r'resource "azurerm_role_assignment" "(\w+)" \{(.*?)\n\}', tf, re.S))


def test_a_team_hub_reads_only_silver_and_writes_only_its_apps_hubs():
    roles = _role_assignments(HUBS_TF)
    assert roles, "hubs.tf grants each team hub's connector its roles"
    for name, body in roles.items():
        scope = re.search(r"^\s*scope\s*=\s*([^#\n]+)", body, re.M).group(1).strip()
        assert "namespace" not in scope, f"{name}: scoped to one Event Hub, never the namespace"
        assert "access_connector.hub[" in body, f"{name}: granted to a team hub's own connector"
        if "Data Receiver" in body:
            assert scope.startswith("azurerm_eventhub.topics[") and "source" in scope, f"{name}: reads silver only"
        else:
            assert "Data Sender" in body, name
            assert scope.startswith("azurerm_eventhub.subscriber["), f"{name}: writes its apps' hubs only"


def test_team_hubs_read_exactly_the_subscription_sources():
    from hl7rt import subscriptions

    block = re.search(r"subscription_sources\s*=\s*\[(.*?)\]", HUBS_TF, re.S).group(1)
    sources = re.findall(r'"(\w+)"', block)
    assert sorted(sources) == sorted(subscriptions.SOURCES), "the hub's sources (hl7rt.subscriptions.SOURCES)"
    tables = re.search(r'variable "tables" \{.*?default\s*=\s*\[(.*?)\]', (STACK / "variables.tf").read_text(), re.S)
    assert set(sources) <= set(re.findall(r'"(\w+)"', tables.group(1))), "each source is one of the hubs"


def test_edp_shared_is_reserved_and_the_default_hub():
    tf = (STACK / "variables.tf").read_text()
    hubs = re.search(r'variable "subscriber_hubs" \{(.*?)\n\}', tf, re.S).group(1)
    assert '"edp_shared"' in hubs and "^[a-z][a-z0-9_]{1,30}$" in hubs, "names: a-z, 0-9, _; edp_shared reserved"
    apps = re.search(r'variable "subscriber_apps" \{(.*?)\n\}', tf, re.S).group(1)
    assert re.search(r'subscription_hub\s*=\s*optional\(string, "edp_shared"\)', apps), "apps default to edp_shared"
    assert "var.subscriber_hubs" in apps, "an app's hub must exist"


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


def test_uc_gives_each_team_hub_a_credential_only_its_service_principal_may_use():
    loop = UC.split("output -json subscriber_hubs")[1]
    assert 'name="hub_${hub}_eventhubs"' in loop and 'credential "$name" SERVICE "$connector" "$principal"' in loop
    assert '\\"add\\":[\\"ACCESS\\"]' in loop and '\\"principal\\":\\"$sp\\"' in loop


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
