SHELL := /bin/bash

BIN      	= $(CURDIR)/bin
BUILD_DIR   = $(CURDIR)/build

GOPATH		= $(HOME)/go
GOBIN		= $(GOPATH)/bin
GO			?= GOGC=off $(shell which go)

# Printing
V = 0
Q = $(if $(filter 1,$V),,@)
M = $(shell printf "\033[34;1m▶\033[0m")

# Tools
$(BUILD_DIR):
	@mkdir -p $@

$(BIN):
	@mkdir -p $@
$(BIN)/%: | $(BIN) ; $(info $(M) building $(@F)…)
	$Q GOBIN=$(BIN) $(GO) install $(shell $(GO) list -tags=tools -f '{{ join .Imports "\n" }}' ./tools | grep $(@F))

GOLANGCI_LINT = $(BIN)/golangci-lint
STRINGER = $(BIN)/stringer
GOIMPORTS = $(BIN)/goimports

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

.PHONY: clean
clean: ; $(info $(M) cleaning…)	@ ## Cleanup everything
	@rm -rf $(BIN)
	@rm -rf $(BUILD)

.PHONY: help
help:
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'