SHELL := /bin/bash

LOCAL_UID ?= $(shell id -u)
LOCAL_GID ?= $(shell id -g)
COMPOSE := LOCAL_UID=$(LOCAL_UID) LOCAL_GID=$(LOCAL_GID) docker compose

.PHONY: install test lint security secrets-detect secrets-gitleaks pipeline pipeline-minio dashboard dagster seed-data dagster-home observability minio minio-seed down

install:
	$(COMPOSE) build pipeline

seed-data:
	mkdir -p data/raw
	cp seed_data/raw/*.csv data/raw/

dagster-home:
	mkdir -p .dagster

test: install
	$(COMPOSE) run --rm pipeline python -m pytest -q

lint: install
	$(COMPOSE) run --rm pipeline ruff check .

security: secrets-detect secrets-gitleaks

secrets-detect:
	$(COMPOSE) run --rm pipeline detect-secrets scan --all-files --exclude-files '(^data/|^metrics/|^\.git/)'

secrets-gitleaks:
	@if command -v gitleaks >/dev/null 2>&1; then \
		gitleaks detect --config .gitleaks.toml --source . --no-git --redact; \
	else \
		echo "gitleaks is not installed. Install it locally, then rerun make security."; \
		exit 1; \
	fi

pipeline: install seed-data
	$(COMPOSE) run --rm pipeline python run_pipeline.py

pipeline-minio: install minio-seed
	APP_ENV=minio STORAGE_MODE=minio $(COMPOSE) --profile storage run --rm pipeline python run_pipeline.py

dashboard: install
	$(COMPOSE) up dashboard

dagster: install dagster-home
	$(COMPOSE) up dagster

observability: install
	$(COMPOSE) --profile observability up metrics-exporter prometheus grafana

minio:
	$(COMPOSE) --profile storage up minio

minio-seed:
	$(COMPOSE) --profile storage up -d minio
	$(COMPOSE) --profile storage run --rm minio-setup

down:
	$(COMPOSE) --profile observability --profile storage down
