# HL7 on Spark Real-Time Mode.
#
#   make install          local Python env (uv)
#   make test | lint      unit tests (local Spark) | ruff + terraform fmt
#
#   make up               build or rebuild Azure (Terraform plan -> you confirm -> apply), then make uc + registry
#   make uc               create or repair the Unity Catalog catalog and credentials (idempotent)
#   make registry         create or repair the Lakebase subscription registry and onboard subscriber apps (idempotent)
#
#   make demo dev         deploy, start every pipeline, both feeds, and the Subscription API; print the URLs
#   make clear-data dev   empty the Delta tables and refill from live data (~2 min; RTM keeps running)
#   make stop dev         stop the feeds, every pipeline, and the Subscription API (stops Databricks billing)
#
# The Java subscriber app runs on its own (java -jar; its workspace E2E job is its own bundle): src/subscriber/README.md.
#
# Your environment lives in infra/azure/terraform.tfvars (gitignored). Auth is the Azure CLI (`az login`) against the
# workspace in Terraform's outputs, so a rebuilt workspace needs no login or config.

ENVS   := dev
TARGET := $(filter $(ENVS),$(MAKECMDGOALS))
DBX    := databricks
BUNDLE := $(DBX) bundle
TF     := terraform -chdir=infra/azure

# Workspace, catalog, and namespace come from Terraform outputs (they change when the sandbox is rebuilt).
tf_out = $(shell $(TF) output -raw $(1) 2>/dev/null)
export DATABRICKS_HOST         ?= $(call tf_out,workspace_url)
export DATABRICKS_AUTH_TYPE    ?= azure-cli
export BUNDLE_VAR_catalog      ?= $(call tf_out,catalog)
export BUNDLE_VAR_eh_namespace ?= $(call tf_out,eh_namespace)

PIPELINES  := hl7_bronze hl7_silver hl7_gold lakehouse epic_json subscription_hub
GENERATORS := hl7_generator epic_json_generator
active_runs = $(DBX) jobs list-runs --job-id $$id --active-only -o json | python3 -c 'import sys,json;print(len(json.load(sys.stdin) or []))'
resource_id = $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['resources']['$(1)']['$(2)']['id'])")

.PHONY: help install test lint up uc registry demo clear-data stop guard-target $(ENVS)

help:
	@sed -n '3,17p' $(MAKEFILE_LIST) | sed 's/^# \{0,1\}//'

guard-target:
	@test -n "$(TARGET)" || { echo "error: name the environment, e.g. 'make demo dev' (one of: $(ENVS))"; exit 2; }

install:
	uv sync

test:
	uv run pytest
	mvn -q -f src/subscriber/pom.xml test

lint:
	uv run ruff check src tests infra
	uv run ruff format --check src tests infra
	terraform fmt -check -recursive infra

# The plan is applied only after you confirm it (never auto-approved).
up:
	@test -f infra/azure/terraform.tfvars || { echo "error: cp infra/azure/terraform.tfvars.example infra/azure/terraform.tfvars"; exit 2; }
	@$(TF) init -input=false >/dev/null
	@$(TF) plan -input=false -out=tfplan
	@read -p "Apply this plan? [y/N] " ok && [ "$$ok" = y ] || { echo "not applied"; rm -f infra/azure/tfplan; exit 1; }
	@$(TF) apply -input=false tfplan && rm -f infra/azure/tfplan
	@$(MAKE) --no-print-directory uc registry

uc:
	@DBX="$(DBX)" infra/uc.sh

registry:
	@uv run python infra/registry.py

# Continuous pipelines start on deploy; `run` starts any that are stopped (running ones are left alone).
demo: guard-target
	$(BUNDLE) deploy -t $(TARGET)
	@for p in $(PIPELINES); do $(BUNDLE) run -t $(TARGET) --no-wait $$p >/dev/null 2>&1 && echo "started $$p" || echo "running $$p"; done
	@for j in $(GENERATORS); do id=$(call resource_id,jobs,$$j); \
	  if [ "$$($(active_runs))" = "0" ]; then $(BUNDLE) run -t $(TARGET) --no-wait $$j >/dev/null && echo "started $$j"; else echo "running $$j"; fi; done
	@$(BUNDLE) run -t $(TARGET) subscription_api >/dev/null && echo "started subscription_api"
	@HL7_API_APP=hl7-subscriptions-$(TARGET) uv run python infra/registry.py | grep -v '^ok'
	@$(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json; d=json.load(sys.stdin)['resources']['dashboards']; \
	  print('workspace:               $(DATABRICKS_HOST)'); print('latency dashboard:       ' + d['hl7_latency']['url']); \
	  print('subscriptions dashboard: ' + d['hl7_subscriptions']['url'])"
	@printf 'subscription API:        %s (contract: /docs)\n' "$$($(DBX) apps get hl7-subscriptions-$(TARGET) -o json | python3 -c 'import sys,json;print(json.load(sys.stdin)["url"])')"

# A full refresh of the mirror empties the Delta tables the dashboard reads and restarts them at the live edge.
clear-data: guard-target
	@$(DBX) pipelines stop $(call resource_id,pipelines,lakehouse) >/dev/null
	@$(BUNDLE) run -t $(TARGET) --no-wait --full-refresh-all lakehouse >/dev/null && echo "cleared; tables refill within ~2 minutes"

# Waits for the feeds to stop, so a following `make demo` doesn't queue behind them.
stop: guard-target
	@for j in $(GENERATORS); do id=$(call resource_id,jobs,$$j); $(DBX) jobs cancel-all-runs --job-id $$id >/dev/null; \
	  while [ "$$($(active_runs))" != "0" ]; do sleep 5; done; \
	  echo "stopped $$j"; done
	@for p in $(PIPELINES); do $(DBX) pipelines stop $(call resource_id,pipelines,$$p) >/dev/null && echo "stopped $$p"; done
	@$(DBX) apps stop hl7-subscriptions-$(TARGET) >/dev/null 2>&1 && echo "stopped subscription_api" || true

# Environment words are goals too; make them no-ops.
$(ENVS):
	@:
