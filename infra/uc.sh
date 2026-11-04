#!/usr/bin/env bash
# Idempotent Unity Catalog bootstrap for the workspace in infra/azure (`make uc`). Safe to re-run.
#
# Unity Catalog objects live in the shared regional metastore and outlive the weekly sandbox wipe, while the Azure
# resources behind them (access connector, storage) are recreated. So each run creates what's missing and repoints
# what's stale:
#   storage credential <catalog>_storage   -> the access connector (ADLS: Storage Blob Data Contributor)
#   external location  <catalog>_root      -> the ADLS container
#   catalog            <catalog>           -> managed storage under that location, bound to this workspace only
#   service credential <catalog>_eventhubs -> the same connector (Event Hubs Data Sender + Receiver)
#   service credential sub_<app>_eventhubs -> each subscriber app's own connector (Data Receiver on its hub only),
#                                             ACCESS granted to that app's service principal only
# A catalog built for an earlier workspace (property hl7.workspace_id) is dropped and recreated: its tables belonged
# to that workspace's pipelines and their storage is gone.
set -euo pipefail

tf() { terraform -chdir=infra/azure output -raw "$1"; }
WORKSPACE_ID=$(tf workspace_id)
CONNECTOR=$(tf access_connector_id)
PRINCIPAL=$(tf access_connector_principal_id)
STORAGE_URL=${STORAGE_URL:-$(tf storage_url)}
CATALOG=${CATALOG:-$(tf catalog)}  # override only for a scratch test
DBX=${DBX:-databricks}

json() { python3 -c "import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1]) or '')" "$1" 2>/dev/null; }

# credential NAME PURPOSE [CONNECTOR PRINCIPAL]: create, or repoint it (default: the main connector). A rebuilt connector
# keeps its resource ID but gets a new identity, which Unity Catalog only picks up on update. So the comment records the
# identity, and a mismatch triggers the update.
credential() {
  local connector=${3:-$CONNECTOR} principal=${4:-$PRINCIPAL}
  local identity="{\"azure_managed_identity\":{\"access_connector_id\":\"$connector\"}}"
  local note="access connector identity $principal" current
  if current=$($DBX credentials get-credential "$1" -o json 2>/dev/null | json "d.get('comment')"); then
    if [ "$current" = "$note" ]; then
      echo "ok       $2 credential $1"
    else
      $DBX credentials update-credential "$1" --comment "$note" --json "$identity" --skip-validation --force >/dev/null
      echo "updated  $2 credential $1 (the access connector was recreated)"
    fi
  else
    $DBX credentials create-credential --skip-validation \
      --json "{\"name\":\"$1\",\"purpose\":\"$2\",\"comment\":\"$note\",${identity:1:${#identity}-2}}" >/dev/null
    echo "created  $2 credential $1"
  fi
}

credential "${CATALOG}_storage" STORAGE

LOCATION="${CATALOG}_root"
if url=$($DBX external-locations get "$LOCATION" -o json 2>/dev/null | json "d['url']"); then
  if [ "${url%/}" = "${STORAGE_URL%/}" ]; then echo "ok       external location $LOCATION"; else
    $DBX external-locations update "$LOCATION" --url "$STORAGE_URL" --credential-name "${CATALOG}_storage" \
      --skip-validation --force >/dev/null
    echo "updated  external location $LOCATION -> $STORAGE_URL"
  fi
else
  # Validation would race the storage role assignment's propagation; the catalog's first table exercises access.
  $DBX external-locations create "$LOCATION" "$STORAGE_URL" "${CATALOG}_storage" --skip-validation >/dev/null
  echo "created  external location $LOCATION"
fi

bind="{\"add\":[{\"workspace_id\":$WORKSPACE_ID,\"binding_type\":\"BINDING_TYPE_READ_WRITE\"}]}"
# After a rebuild the catalog is still bound (ISOLATED) to the deleted workspace, so this one can't see it yet: bind it.
if [[ "$($DBX catalogs get "$CATALOG" 2>&1 || true)" == *"not accessible in current workspace"* ]]; then
  $DBX workspace-bindings update-bindings catalog "$CATALOG" --json "$bind" >/dev/null
  echo "bound    catalog $CATALOG to this workspace (it was bound to a deleted one)"
fi
if built_for=$($DBX catalogs get "$CATALOG" -o json 2>/dev/null | json "d.get('properties',{}).get('hl7.workspace_id','none')"); then
  if [ "$built_for" = "none" ]; then
    echo "error: catalog $CATALOG exists but wasn't created by make uc; set another catalog in infra/azure/terraform.tfvars" >&2
    exit 1
  elif [ "$built_for" != "$WORKSPACE_ID" ]; then
    echo "catalog $CATALOG was built for workspace $built_for (this one is $WORKSPACE_ID): dropping and recreating it"
    $DBX catalogs delete "$CATALOG" --force
    built_for=""
  else
    echo "ok       catalog $CATALOG"
  fi
fi
if [ -z "${built_for:-}" ]; then
  $DBX catalogs create --json "{\"name\":\"$CATALOG\",\"storage_root\":\"${STORAGE_URL%/}/$CATALOG\",
    \"comment\":\"HL7 on Spark Real-Time Mode (make uc)\",\"properties\":{\"hl7.workspace_id\":\"$WORKSPACE_ID\"}}" >/dev/null
  # Shared metastore: keep the catalog visible to this workspace only.
  $DBX workspace-bindings update-bindings catalog "$CATALOG" --json "$bind" >/dev/null
  $DBX catalogs update "$CATALOG" --isolation-mode ISOLATED >/dev/null
  echo "created  catalog $CATALOG (bound to workspace $WORKSPACE_ID)"
  # The bundle's local deployment cache points at the old workspace's resources.
  rm -rf .databricks/bundle
fi

credential "${CATALOG}_eventhubs" SERVICE
echo "pipelines and generators reach Event Hubs with service credential ${CATALOG}_eventhubs (you own it)"

# Subscriber apps (terraform var subscriber_apps): one credential per app, usable only by that app's service principal.
terraform -chdir=infra/azure output -json subscriber_apps | python3 -c '
import sys, json
for app, a in json.load(sys.stdin).items():
    print(app, a["application_id"], a["connector_id"], a["connector_principal_id"])' |
while read -r app sp connector principal; do
  name="sub_${app//-/_}_eventhubs"
  credential "$name" SERVICE "$connector" "$principal"
  $DBX grants update credential "$name" --json "{\"changes\":[{\"principal\":\"$sp\",\"add\":[\"ACCESS\"]}]}" >/dev/null
  echo "ok       ACCESS on $name for $app ($sp)"
done
