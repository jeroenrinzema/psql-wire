include .bingo/Variables.mk

SHELL := /bin/bash

GO	?= GOGC=off $(shell which go)

# Printing
V = 0
Q = $(if $(filter 1,$V),,@)
M = $(shell printf "\033[34;1m▶\033[0m")

# Targets
.PHONY: lint
lint: | $(GOLANGCI_LINT) ; $(info $(M) running golint…) @ ## Run the project linters
	$Q $(GOLANGCI_LINT) run --max-issues-per-linter 10

.PHONY: test
test: ## Run all tests
	$Q $(GO) test ./...

.PHONY: fmt
fmt: ; $(info $(M) running gofmt…) @ ## Run gofmt on all source files
	$Q $(GO) fmt $(PKGS)

.PHONY: build
build: test lint

.PHONY: help
help:
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
