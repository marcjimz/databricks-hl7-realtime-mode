# HL7 on Spark Real-Time Mode — bundle, infra, and local dev helpers.
#
# Usage (env is a positional word, e.g. `make deploy dev`):
#   make install                 create .venv with dev dependencies (uv)
#   make test | lint | fmt       local checks (pytest with local Spark, ruff, terraform fmt)
#   make clean                   remove local build/test artifacts (.build/, caches)
#
#   make up                      build or rebuild Azure (plan, confirm, apply) + Unity Catalog. Rerun after the weekly wipe
#   make infra-plan [STACK=]     terraform plan for infra/<STACK>/ (default: azure) -> tfplan
#   make infra-apply [STACK=]    terraform apply that saved plan (only the reviewed plan)
#   make uc                      create or repair the catalog, storage + Event Hubs service credentials (idempotent)
#
#   make validate dev            databricks bundle validate
#   make deploy dev              validate + deploy (continuous pipelines start automatically)
#   make start dev               start every pipeline (resumes checkpoints; already-running ones are left alone)
#   make demo dev                deploy + start + generate, then print the dashboard URL
#   make generate dev            start the continuous synthetic feed (runs until generate-stop)
#   make generate-stop dev       cancel the synthetic feed
#   make clear-data dev          empty the Delta tables and refill from live data (~2 min; hot path keeps running)
#   make reset dev               full refresh: new checkpoints, start at the live edge (DEMO ONLY)
#   make stop dev                stop all pipelines + generators (stops compute billing)
#   make destroy dev             databricks bundle destroy
#
# Auth: Azure CLI (`az login`) against the workspace in infra/azure's Terraform outputs, so a rebuilt workspace needs
# no login or config. To use a Databricks CLI profile instead, put PROFILE=<profile> in local.mk (see local.mk.example).

# Bundle targets treated as an env word. Kept in sync with databricks.yml `targets:`, so a
# typo like `make deploy devv` fails loudly instead of silently doing nothing.
ENVS := dev

TARGET  := $(filter $(ENVS),$(MAKECMDGOALS))
-include local.mk   # optional, gitignored: your PROFILE=<databricks cli profile> (see local.mk.example)
PROFILE ?= $(DATABRICKS_CONFIG_PROFILE)
DBX     := databricks $(if $(PROFILE),-p $(PROFILE))
BUNDLE  := $(DBX) bundle
STACK   ?= azure
TF      := terraform -chdir=infra/$(STACK)

# Workspace, catalog, and namespace come from Terraform outputs (they change when the sandbox is rebuilt).
tf_out = $(shell terraform -chdir=infra/azure output -raw $(1) 2>/dev/null)
ifeq ($(PROFILE),)
  export DATABRICKS_HOST      ?= $(call tf_out,workspace_url)
  export DATABRICKS_AUTH_TYPE ?= azure-cli
endif
export BUNDLE_VAR_catalog      ?= $(call tf_out,catalog)
export BUNDLE_VAR_eh_namespace ?= $(call tf_out,eh_namespace)

# Hot-path RTM pipelines first, then the mirror and Epic JSON.
PIPELINES  := hl7_bronze hl7_silver hl7_gold lakehouse epic_json
GENERATORS := hl7_generator epic_json_generator

# Resolve a deployed resource id from the bundle summary: $(call resource_id,pipelines,epic_json)
resource_id = $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['resources']['$(1)']['$(2)']['id'])")
bundle_var  = $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['variables']['$(1)']['value'])")

.PHONY: help install test lint fmt clean up infra-plan infra-apply uc validate deploy start demo generate generate-stop \
        clear-data \
        reset stop destroy guard-target $(ENVS)

help:
	@sed -n '3,25p' $(MAKEFILE_LIST) | sed 's/^# \{0,1\}//'
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
	terraform fmt -check -recursive infra

fmt:
	uv run ruff check --fix src tests
	uv run ruff format src tests
	terraform fmt -recursive infra

clean:
	rm -rf .build dist .pytest_cache .ruff_cache
	find src tests -type d -name __pycache__ -prune -exec rm -rf {} +

# --- Infra (Terraform, one stack per folder under infra/) ---------------------
# Two steps on purpose: review the plan, then apply exactly that plan.
infra-plan:
	@test -f infra/$(STACK)/terraform.tfvars || { echo "error: copy infra/$(STACK)/terraform.tfvars.example to terraform.tfvars"; exit 2; }
	$(TF) init -input=false -upgrade=false >/dev/null
	$(TF) plan -input=false -out=tfplan

infra-apply:
	@test -f infra/$(STACK)/tfplan || { echo "error: run 'make infra-plan STACK=$(STACK)' first"; exit 2; }
	$(TF) apply -input=false tfplan
	@rm -f infra/$(STACK)/tfplan

# Build or rebuild everything outside the bundle. The plan is shown and applied only after you confirm it.
up:
	@$(MAKE) --no-print-directory infra-plan STACK=azure
	@read -p "Apply this plan? [y/N] " ok && [ "$$ok" = y ] || { echo "not applied"; rm -f infra/azure/tfplan; exit 1; }
	@$(MAKE) --no-print-directory infra-apply STACK=azure
	@$(MAKE) --no-print-directory uc

uc:
	@DBX="$(DBX)" scripts/uc.sh

# --- Bundle -------------------------------------------------------------------
validate: guard-target
	$(BUNDLE) validate -t $(TARGET)

deploy: guard-target
	$(BUNDLE) validate -t $(TARGET)
	$(BUNDLE) deploy   -t $(TARGET)

start: guard-target
	@for p in $(PIPELINES); do $(BUNDLE) run -t $(TARGET) --no-wait $$p >/dev/null 2>&1 && echo "started $$p" || echo "running $$p"; done

demo: deploy start generate
	@echo "dashboard: $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['resources']['dashboards']['hl7_latency']['url'])")"
	@echo "classic RTM clusters take ~9 min to start; latency appears once they're RUNNING. Stop with: make stop $(TARGET)"

generate: guard-target
	@for j in $(GENERATORS); do $(BUNDLE) run -t $(TARGET) --no-wait $$j; done

# Waits until the runs have actually stopped, so a following `make generate` doesn't queue behind them.
generate-stop: guard-target
	@for j in $(GENERATORS); do id=$(call resource_id,jobs,$$j); $(DBX) jobs cancel-all-runs --job-id $$id >/dev/null; \
	  while [ "$$($(DBX) jobs list-runs --job-id $$id --active-only -o json | python3 -c 'import sys,json;print(len(json.load(sys.stdin) or []))')" != "0" ]; do sleep 5; done; \
	  echo "stopped $$j"; done

# The lakehouse mirror writes the Delta tables the dashboard reads. A full refresh empties them and restarts at the live
# edge in ~2 minutes; RTM pipelines keep running.
DELTA_PIPELINES := lakehouse
clear-data: guard-target
	@for p in $(DELTA_PIPELINES); do $(DBX) pipelines stop $(call resource_id,pipelines,$$p) >/dev/null; \
	  $(BUNDLE) run -t $(TARGET) --no-wait --full-refresh-all $$p >/dev/null && echo "cleared $$p"; done
	@echo "tables refill from live data within ~2 minutes; refresh the dashboard (its own refresh button)"

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
