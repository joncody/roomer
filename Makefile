.DEFAULT_GOAL := help

.PHONY: help test test-go test-rust test-node test-python check tla tla-download redis redis-up redis-down cluster cluster-up cluster-down loadtest cluster-test clean

# Auto-detect local virtualenv pytest using absolute paths to avoid 'cd' concatenation bugs
PYTEST ?= $(if $(wildcard client/python/.venv/bin/pytest),$(CURDIR)/client/python/.venv/bin/pytest,$(if $(wildcard client/python/.venv/bin/python),$(CURDIR)/client/python/.venv/bin/python -m pytest,pytest))

# Auto-detect TLA+ runner: system 'tlc' binary, or 'tla2tools.jar' (repo root, spec/, ~/.tla/, or system paths)
TLA2TOOLS_JAR ?= $(firstword $(wildcard \
	tla2tools.jar \
	spec/tla2tools.jar \
	$(HOME)/.tla/tla2tools.jar \
	$(HOME)/.tla/lib/tla2tools.jar \
	$(HOME)/.local/share/tla/tla2tools.jar \
	$(HOME)/bin/tla2tools.jar \
	/usr/local/lib/tla2tools.jar \
	/usr/local/share/tla/tla2tools.jar \
	/usr/share/java/tla2tools.jar \
	/opt/tla/tla2tools.jar \
))

TLC ?= $(if $(shell command -v tlc 2>/dev/null),tlc,$(if $(TLA2TOOLS_JAR),java -cp $(TLA2TOOLS_JAR) tlc2.TLC,))

# Default cluster pair: go on 8080, rust on 8081
PAIR ?= go-rust

# Native Make string parsing (replaces shell 'cut')
NODE1 ?= $(word 1,$(subst -, ,$(PAIR)))
NODE2 ?= $(word 2,$(subst -, ,$(PAIR)))

# Configurable Load Test Parameters
CLIENTS ?= 50
MESSAGES ?= 500
DELAY ?= 0
NODE1_URL ?= ws://localhost:8080/ws
NODE2_URL ?= ws://localhost:8081/ws
NODES ?=
ROOM ?=
EXTRA_ARGS ?=

help: ## Display this help guide with available targets
	@echo ""
	@echo "Roomer Development & Testing Automation:"
	@echo ""
	@echo "  \033[1;37mTesting & Quality Assurance\033[0m"
	@grep -E '^(test|test-[a-z]+|check|tla):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  \033[1;37mClustering & Load Testing\033[0m"
	@grep -E '^(cluster|cluster-[a-z]+|loadtest):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  \033[1;37mInfrastructure & Utilities\033[0m"
	@grep -E '^(redis|redis-[a-z]+|tla-download|tla2tools.jar|clean|help):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  \033[1;37mConfigurable Variables\033[0m"
	@echo "    \033[33mPAIR\033[0m            Cluster server pair (default: go-rust; options: rust-go, go-node, rust-node)"
	@echo "    \033[33mCLIENTS\033[0m         Clients per cluster node for loadtest (default: 50)"
	@echo "    \033[33mMESSAGES\033[0m        Broadcast messages to send in loadtest (default: 500)"
	@echo "    \033[33mDELAY\033[0m           Microseconds between messages in loadtest (default: 0)"
	@echo "    \033[33mNODE1_URL\033[0m       WebSocket URL for Node 1 (default: ws://localhost:8080/ws)"
	@echo "    \033[33mNODE2_URL\033[0m       WebSocket URL for Node 2 (default: ws://localhost:8081/ws)"
	@echo "    \033[33mNODES\033[0m           Comma-separated node URLs (overrides NODE1_URL and NODE2_URL)"
	@echo "    \033[33mROOM\033[0m            Target room name for loadtest (default: unique timestamped room)"
	@echo "    \033[33mPYTEST\033[0m          Python test runner path (auto-detects client/python/.venv)"
	@echo ""

test: test-go test-rust test-node test-python ## Run all unit test suites across Go, Rust, Node, and Python

test-go: ## Run Go unit tests with race detector
	cd server/go && go test -v -race ./...

test-rust: ## Run Rust server unit and integration tests
	cargo test --manifest-path server/rust/Cargo.toml --features redis-adapter

test-node: ## Run Node.js server test suite
	cd server/node && ([ -d node_modules ] || npm install) && npm test

test-python: ## Run Python client SDK test suite
	cd client/python && $(PYTEST) -v

check: tla ## Run TLA+ formal specification model checking suite

tla: ## Verify formal state invariants in spec/roomer.tla with TLC
	@if [ -n "$(TLC)" ]; then \
		echo "Running TLA+ Model Checker on spec/roomer.tla ($(TLC))..."; \
		$(TLC) -config spec/roomer.cfg spec/roomer.tla; \
	elif command -v tlc >/dev/null 2>&1; then \
		echo "Running TLA+ Model Checker (tlc)..."; \
		tlc -config spec/roomer.cfg spec/roomer.tla; \
	elif [ -n "$(TLA2TOOLS_JAR)" ]; then \
		echo "Running TLA+ Model Checker (java -cp $(TLA2TOOLS_JAR) tlc2.TLC)..."; \
		java -cp $(TLA2TOOLS_JAR) tlc2.TLC -config spec/roomer.cfg spec/roomer.tla; \
	elif bash -i -c 'type -t tlc' 2>/dev/null | grep -Eq '^(alias|function)'; then \
		echo "Running TLA+ Model Checker via interactive shell 'tlc'..."; \
		bash -i -c 'tlc -config spec/roomer.cfg spec/roomer.tla'; \
	else \
		echo "Error: TLA+ model checker not found."; \
		echo "Please install 'tlc', configure ~/.tla/tla2tools.jar, or download tla2tools.jar:"; \
		echo "  make tla-download"; \
		echo "  make check"; \
		exit 1; \
	fi

tla-download: tla2tools.jar ## Download tla2tools.jar into repository root

tla2tools.jar:
	curl -fSL -o $@ https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar

redis: ## Start standalone Redis container on port 6379
	docker compose -f docker-compose.cluster.yml up -d redis

redis-up: redis ## Alias for 'redis'

redis-down: ## Stop standalone Redis container
	docker compose -f docker-compose.cluster.yml stop redis

cluster: ## Start multi-node Redis cluster with PAIR (default: PAIR=go-rust)
	NODE1=$(NODE1) NODE2=$(NODE2) docker compose -f docker-compose.cluster.yml up --build -d

cluster-up: cluster ## Alias for 'cluster'

cluster-down: ## Stop and tear down multi-node cluster containers
	docker compose -f docker-compose.cluster.yml down

loadtest: ## Run cluster load test (CLIENTS=50 MESSAGES=500 DELAY=0)
	cd server/go && go run ./cmd/loadtest/main.go \
		-node1=$(NODE1_URL) \
		-node2=$(NODE2_URL) \
		$(if $(NODES),-nodes=$(NODES),) \
		$(if $(ROOM),-room=$(ROOM),) \
		-clients=$(CLIENTS) \
		-messages=$(MESSAGES) \
		-delay=$(DELAY) \
		$(EXTRA_ARGS)

cluster-test: cluster ## Orchestrate cluster spinup, readiness wait, load test, and teardown
	@echo "Waiting for cluster nodes to initialize and connect to Redis..."
	@sleep 5
	@$(MAKE) loadtest || (echo "Loadtest failed, leaving cluster running for debugging." && exit 1)
	@$(MAKE) cluster-down

clean: ## Remove build artifacts, caches, and test artifacts across all languages
	@echo "Cleaning Go artifacts and test caches..."
	@(cd server/go && go clean -cache -testcache) 2>/dev/null || true
	@(cd server/go && go clean -i -r) 2>/dev/null || true
	@rm -f server_bin server/go/server_bin server/go/cmd/loadtest/loadtest
	@echo "Cleaning Rust target directories..."
	@cargo clean --manifest-path server/rust/Cargo.toml 2>/dev/null || true
	@echo "Cleaning Python build and test caches..."
	@rm -rf client/python/build client/python/dist client/python/*.egg-info client/python/.pytest_cache
	@find client/python -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaning TLA+ state files..."
	@rm -rf states/ spec/states/ MC.out spec/MC.out
	@echo "Cleaning Docker containers, volumes, and Redis test data..."
	@docker compose -f docker-compose.cluster.yml down -v --remove-orphans 2>/dev/null || true
	@docker compose -f server/go/docker-compose.yml down -v --remove-orphans 2>/dev/null || true
	@docker compose -f server/node/docker-compose.yml down -v --remove-orphans 2>/dev/null || true
	@docker compose -f server/rust/docker-compose.yml down -v --remove-orphans 2>/dev/null || true
	@redis-cli -p 6379 flushall 2>/dev/null || true
	@echo "Clean completed."
