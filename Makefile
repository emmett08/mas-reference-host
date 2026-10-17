SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
CARGO ?= cargo
MANIFEST := .protocol/rust-protocol/Cargo.toml
TARGET := $(CURDIR)/rust-protocol/target

.PHONY: protocol build test verify containers
protocol:
	bash scripts/prepare-protocol.sh

build: protocol
	$(CARGO) build --locked --release --manifest-path $(MANIFEST) --target-dir $(TARGET) --features postgres-repository --bin mas --bin mas-gateway --bin mas-protocol-codec --bin mas-shared --bin verify

test:
	python -m unittest discover -s host/tests -v
	python -m unittest discover -s scripts/tests -v

verify: build
	$(TARGET)/release/verify

containers: protocol
	docker compose build
	docker build -f Dockerfile.cursor -t mas-mesh-cursor:local .
