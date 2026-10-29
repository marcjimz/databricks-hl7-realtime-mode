# HL7 on Spark Real-Time Mode — bundle, infra, and local dev helpers.
#
# Usage (env is a positional word, e.g. `make deploy dev`):
#   make install                 create .venv with dev dependencies (uv)
#   make test | lint | fmt       local checks (pytest with local Spark, ruff, terraform fmt)
#   make clean                   remove local build/test artifacts (.build/, caches)
#
#   make infra-plan [STACK=]     terraform plan for infra/<STACK>/ (default: eventhubs) -> tfplan
#   make infra-apply [STACK=]    terraform apply that saved plan (only the reviewed plan)
#   make secrets dev             write the Event Hubs connection string into the bundle's secret scope
#
#   make validate dev            databricks bundle validate
#   make deploy dev              validate + deploy (continuous pipelines start automatically)
#   make start dev               start every pipeline (resumes checkpoints; already-running ones are left alone)
#   make demo dev                deploy + start + generate, then print the dashboard URL
#   make generate dev            start the continuous synthetic feed (runs until generate-stop)
#   make generate-stop dev       cancel the synthetic feed (and subscription jobs)
#   make sub-new NAME=x [MODE=rtm|microbatch] [JOIN=1]   new subscription from the templates
#   make reset dev               full refresh: new checkpoints, start at the live edge (DEMO ONLY)
#   make stop dev                stop all pipelines + generators (stops compute billing)
#   make destroy dev             databricks bundle destroy
#
# Auth: the Databricks CLI picks a profile by host; if several match, put PROFILE=<profile> in local.mk
# (cp local.mk.example local.mk), or pass PROFILE=<profile>, or set DATABRICKS_CONFIG_PROFILE.

# Bundle targets treated as an env word. Kept in sync with databricks.yml `targets:`, so a
# typo like `make deploy devv` fails loudly instead of silently doing nothing.
ENVS := dev

TARGET  := $(filter $(ENVS),$(MAKECMDGOALS))
-include local.mk   # optional, gitignored: your PROFILE=<databricks cli profile> (see local.mk.example)
PROFILE ?= $(DATABRICKS_CONFIG_PROFILE)
DBX     := databricks $(if $(PROFILE),-p $(PROFILE))
BUNDLE  := $(DBX) bundle
STACK   ?= eventhubs
TF      := terraform -chdir=infra/$(STACK)
TF_EH   := terraform -chdir=infra/eventhubs

# Hot-path RTM pipelines first, then subscription pipelines, the mirror, and Epic JSON. RTM join subscriptions are
# Structured Streaming jobs (resources/jobs/sub_*.yml) because pipelines don't support stream-stream joins in RTM.
SUB_PIPELINES := $(patsubst resources/pipelines/%.yml,%,$(wildcard resources/pipelines/sub_*.yml))
SUB_JOBS      := $(patsubst resources/jobs/%.yml,%,$(wildcard resources/jobs/sub_*.yml))
PIPELINES  := hl7_bronze hl7_silver hl7_gold $(SUB_PIPELINES) lakehouse epic_json
GENERATORS := hl7_generator epic_json_generator

# Resolve a deployed resource id from the bundle summary: $(call resource_id,pipelines,epic_json)
resource_id = $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['resources']['$(1)']['$(2)']['id'])")
bundle_var  = $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['variables']['$(1)']['value'])")

.PHONY: help install test lint fmt clean infra-plan infra-apply secrets validate deploy start demo generate generate-stop sub-new \
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

# Pipes the sensitive terraform output straight into the secret scope; never printed.
secrets: guard-target
	@set -e; SCOPE=$(call bundle_var,eh_secret_scope); KEY=$(call bundle_var,eh_secret_key); \
	echo "secret scope=$$SCOPE key=$$KEY"; \
	$(DBX) secrets list-scopes -o json | python3 -c "import sys,json;sys.exit(0 if any(s['name']=='$$SCOPE' for s in json.load(sys.stdin)) else 1)" \
	  || $(DBX) secrets create-scope $$SCOPE; \
	$(TF_EH) output -raw connection_string | $(DBX) secrets put-secret $$SCOPE $$KEY; \
	echo "  stored"

# --- Bundle -------------------------------------------------------------------
validate: guard-target
	$(BUNDLE) validate -t $(TARGET)

deploy: guard-target
	$(BUNDLE) validate -t $(TARGET)
	$(BUNDLE) deploy   -t $(TARGET)

start: guard-target
	@for p in $(PIPELINES); do $(BUNDLE) run -t $(TARGET) --no-wait $$p >/dev/null 2>&1 && echo "started $$p" || echo "running $$p"; done
	@for j in $(SUB_JOBS); do id=$(call resource_id,jobs,$$j); \
	  if [ "$$($(DBX) jobs list-runs --job-id $$id --active-only -o json | python3 -c 'import sys,json;print(len(json.load(sys.stdin) or []))')" = "0" ]; \
	  then $(BUNDLE) run -t $(TARGET) --no-wait $$j >/dev/null && echo "started $$j"; else echo "running $$j"; fi; done

demo: deploy start generate
	@echo "dashboard: $$($(BUNDLE) summary -t $(TARGET) -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['resources']['dashboards']['hl7_latency']['url'])")"
	@echo "classic RTM clusters take ~9 min to start; latency appears once they're RUNNING. Stop with: make stop $(TARGET)"

generate: guard-target
	@for j in $(GENERATORS); do $(BUNDLE) run -t $(TARGET) --no-wait $$j; done

# Waits until the runs have actually stopped, so a following `make generate` doesn't queue behind them.
generate-stop: guard-target
	@for j in $(GENERATORS) $(SUB_JOBS); do id=$(call resource_id,jobs,$$j); $(DBX) jobs cancel-all-runs --job-id $$id >/dev/null; \
	  while [ "$$($(DBX) jobs list-runs --job-id $$id --active-only -o json | python3 -c 'import sys,json;print(len(json.load(sys.stdin) or []))')" != "0" ]; do sleep 5; done; \
	  echo "stopped $$j"; done

# New subscription: a rule file from src/subscriptions/_template.yml + a pipeline resource from the mode's template.
MODE ?= rtm
# An rtm JOIN rule runs as a Structured Streaming job (pipelines don't support RTM stream-stream joins); everything
# else is a pipeline. Micro-batch joins need nothing extra.
RTM_JOIN := $(and $(JOIN),$(filter rtm,$(MODE)))
SUB_KIND := $(if $(RTM_JOIN),jobs,pipelines)
SUB_TEMPLATE := resources/$(SUB_KIND)/_templates/sub_$(MODE)$(if $(RTM_JOIN),_join).yml.tmpl
sub-new:
	@test -n "$(NAME)" || { echo "usage: make sub-new NAME=<name> [MODE=rtm|microbatch] [JOIN=1]"; exit 2; }
	@test -f $(SUB_TEMPLATE) || { echo "error: no template $(SUB_TEMPLATE)"; exit 2; }
	@test ! -f src/subscriptions/$(NAME).yml || { echo "error: src/subscriptions/$(NAME).yml already exists"; exit 2; }
	@sed -e 's/^name: _template/name: $(NAME)/' -e 's/^mode: rtm /mode: $(MODE) /' \
	  $(if $(JOIN),-e 's/^# then: /then: /' -e 's/^#   source:/  source:/' -e 's/^#   filter:/  filter:/' -e 's/^# within:/within:/') \
	  src/subscriptions/_template.yml > src/subscriptions/$(NAME).yml
	@sed 's/__NAME__/$(NAME)/g' $(SUB_TEMPLATE) > resources/$(SUB_KIND)/sub_$(NAME).yml
	@echo "created src/subscriptions/$(NAME).yml and resources/$(SUB_KIND)/sub_$(NAME).yml"
	@echo "next: edit the rule, make infra-plan && make infra-apply (adds hub sub_$(NAME)), make deploy <env>"

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
