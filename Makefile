# HL7 on Spark Real-Time Mode — bundle, infra, and local dev helpers.
#
# Usage (env is a positional word, e.g. `make deploy dev`):
#   make install                 create .venv with dev dependencies (uv)
#   make test | lint | fmt       local checks (pytest with local Spark, ruff)
#
#   make infra-plan              terraform plan  -> infra/tfplan (Event Hubs namespace + hubs)
#   make infra-apply             terraform apply infra/tfplan (only the reviewed plan)
#   make secrets dev             write the Event Hubs connection string into the bundle's secret scope
#
#   make validate dev            databricks bundle validate
#   make deploy dev              validate + deploy (continuous pipelines start automatically)
#   make generate dev            start the continuous synthetic feed (runs until generate-stop)
#   make generate-stop dev       cancel the synthetic feed
#   make reset dev               full refresh: new checkpoints, start at the live edge (DEMO ONLY)
#   make stop dev                stop all pipelines + generators (stops compute billing)
#   make destroy dev             databricks bundle destroy
#
# Auth: the Databricks CLI picks a profile by host; if several match, set
# DATABRICKS_CONFIG_PROFILE=<profile> (or pass PROFILE=<profile>).

# Bundle targets treated as an env word. Kept in sync with databricks.yml `targets:`, so a
# typo like `make deploy devv` fails loudly instead of silently doing nothing.
ENVS := dev

TARGET  := $(filter $(ENVS),$(MAKECMDGOALS))
PROFILE ?= $(DATABRICKS_CONFIG_PROFILE)
DBX     := databricks $(if $(PROFILE),-p $(PROFILE))
BUNDLE  := $(DBX) bundle
TF      := terraform -chdir=infra

PIPELINES  := epic_json
GENERATORS := epic_json_generator

# Resolve a deployed resource id from the bundle summary: $(call resource_id,pipelines,epic_json)
resource_id = $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['resources']['$(1)']['$(2)']['id'])")
bundle_var  = $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['variables']['$(1)']['value'])")

.PHONY: help install test lint fmt infra-plan infra-apply secrets validate deploy generate generate-stop \
        reset stop destroy guard-target $(ENVS)

help:
	@sed -n '3,20p' $(MAKEFILE_LIST) | sed 's/^# \{0,1\}//'
	@echo "  <env> is one of: $(ENVS)"

guard-target:
	@if [ -z "$(TARGET)" ]; then echo "error: specify a target env, e.g. 'make deploy dev' (valid: $(ENVS))"; exit 2; fi
	@if [ $(words $(TARGET)) -gt 1 ]; then echo "error: pick ONE env, got: $(TARGET)"; exit 2; fi

# --- Local --------------------------------------------------------------------
install:
	uv sync

test:
	uv run pytest

lint:
	uv run ruff check src tests
	uv run ruff format --check src tests
	$(TF) fmt -check

fmt:
	uv run ruff check --fix src tests
	uv run ruff format src tests
	$(TF) fmt

# --- Infra (Azure Event Hubs) -------------------------------------------------
# Two steps on purpose: review the plan, then apply exactly that plan.
infra-plan:
	@test -f infra/terraform.tfvars || { echo "error: copy infra/terraform.tfvars.example to infra/terraform.tfvars"; exit 2; }
	$(TF) init -input=false -upgrade=false >/dev/null
	$(TF) plan -input=false -out=tfplan

infra-apply:
	@test -f infra/tfplan || { echo "error: run 'make infra-plan' first"; exit 2; }
	$(TF) apply -input=false tfplan
	@rm -f infra/tfplan

# Pipes the sensitive terraform output straight into the secret scope; never printed.
secrets: guard-target
	@set -e; SCOPE=$(call bundle_var,eh_secret_scope); KEY=$(call bundle_var,eh_secret_key); \
	echo "secret scope=$$SCOPE key=$$KEY"; \
	$(DBX) secrets list-scopes -o json | python3 -c "import sys,json;sys.exit(0 if any(s['name']=='$$SCOPE' for s in json.load(sys.stdin)) else 1)" \
	  || $(DBX) secrets create-scope $$SCOPE; \
	$(TF) output -raw connection_string | $(DBX) secrets put-secret $$SCOPE $$KEY; \
	echo "  stored"

# --- Bundle -------------------------------------------------------------------
validate: guard-target
	$(BUNDLE) validate -t $(TARGET)

deploy: guard-target
	$(BUNDLE) validate -t $(TARGET)
	$(BUNDLE) deploy   -t $(TARGET)

generate: guard-target
	@for j in $(GENERATORS); do $(BUNDLE) run -t $(TARGET) --no-wait $$j; done

generate-stop: guard-target
	@for j in $(GENERATORS); do $(DBX) jobs cancel-all-runs --job-id $(call resource_id,jobs,$$j) >/dev/null && echo "cancelled $$j"; done

# Demo-only: discards checkpoints so streams start at kafka_starting_offsets (latest) instead of
# resuming. Production should always resume from the checkpoint so no message is skipped.
reset: guard-target
	@for p in $(PIPELINES); do $(BUNDLE) run -t $(TARGET) --no-wait --full-refresh-all $$p; done

stop: guard-target generate-stop
	@for p in $(PIPELINES); do $(DBX) pipelines stop $(call resource_id,pipelines,$$p) >/dev/null && echo "stopped $$p"; done

destroy: guard-target
	$(BUNDLE) destroy -t $(TARGET)

# Env words are goals too; make them no-ops.
$(ENVS):
	@:
