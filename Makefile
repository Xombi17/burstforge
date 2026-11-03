.PHONY: build test race target smoke burst fixed compare demo clean

BIN := bin/burstforge
OUT := results

build:
	cd agent && go build -o ../$(BIN) ./cmd/burstforge

test:
	cd agent && go vet ./... && go test -count=1 ./...

race:
	cd agent && go test -race -count=1 ./...

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

clean:
	rm -rf bin results burstforge-results
