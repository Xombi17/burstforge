.PHONY: build test race test-db target smoke burst fixed compare demo faults recovery tenants docker-demo clean

BIN := bin/burstforge
OUT := results

build:
	cd agent && go build -o ../$(BIN) ./cmd/burstforge

test:
	cd agent && go vet ./... && go test -count=1 ./...

race:
	cd agent && go test -race -count=1 ./...

# Store tests against a real Postgres. They are skipped by `make test`.
#   BURSTFORGE_TEST_DB=postgres://user@localhost:5432/burstforge_test make test-db
test-db:
	@test -n "$(BURSTFORGE_TEST_DB)" || { echo "set BURSTFORGE_TEST_DB to a throwaway Postgres database" >&2; exit 2; }
	cd agent && BURSTFORGE_TEST_DB="$(BURSTFORGE_TEST_DB)" go test -race -count=1 ./internal/store/ ./internal/runner/
	cd agent && go build -o ../$(BIN) ./cmd/burstforge
	cd control-plane && { [ -d node_modules ] || npm ci --silent; } && BURSTFORGE_TEST_DB="$(BURSTFORGE_TEST_DB)" npm test --silent

# Run the fragile target in the foreground (port 8080).
target:
	cd target && node server.js

smoke: build
	$(BIN) run scenarios/registration-smoke.yaml --out $(OUT)

burst: build
	$(BIN) run scenarios/registration-burst.yaml --out $(OUT)

fixed: build
	$(BIN) run scenarios/registration-burst-fixed.yaml --out $(OUT)

# Full before/after story. Starts and stops its own target.
demo: build
	./scripts/demo.sh

# Same proxy-injected faults against the fragile and hardened target.
faults: build
	./scripts/faults-demo.sh

# Kill an agent mid-run and watch its shard get taken over. Needs BURSTFORGE_DB.
recovery: build
	./scripts/recovery-demo.sh

# Week 6: three organizations run at once through the control plane, with
# their own quotas, targets and metrics. Needs Node and BURSTFORGE_DB.
tenants: build
	./scripts/tenants-demo.sh

# The demos in Docker Compose: before/after, a worker container killed under
# Postgres leases, and faults through the proxy container.
docker-demo:
	./scripts/docker-demo.sh

clean:
	rm -rf bin results burstforge-results
