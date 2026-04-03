VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-X main.Version=$(VERSION)"
BINARY := folddb

.PHONY: build test test-unit test-integration test-all verify bench bench-full bench-full-kafka bench-compare stress stress-quick lint release clean docker-up docker-down testdata grammar

build:
	go build $(LDFLAGS) -o $(BINARY) ./cmd/folddb
	go build -o folddb-gen ./cmd/folddb-gen

test: test-unit

test-unit:
	go test ./...

test-integration: docker-up
	@echo "Waiting for Kafka to be healthy..."
	@timeout=60; \
	while [ $$timeout -gt 0 ]; do \
		if docker compose ps kafka | grep -q healthy; then \
			break; \
		fi; \
		sleep 2; \
		timeout=$$((timeout - 2)); \
	done
	@echo "Waiting for seed container to finish..."
	@docker compose logs seed --follow 2>/dev/null || true
	@sleep 2
	@echo "Running integration tests..."
	go test -tags integration -timeout 120s -v ./test/integration/
	@echo "Tearing down..."
	$(MAKE) docker-down

test-all: test-unit test-integration verify

verify: build
	./scripts/verify.sh 10000

bench: build
	@echo "Running benchmarks (this may take a few minutes)..."
	go test -tags bench -bench . -benchtime 1x -timeout 10m ./bench/ 2>&1 | tee bench/results.txt
	@echo "Results saved to bench/results.txt"

docker-up:
	docker compose up -d
	@echo "Infrastructure starting... use 'docker compose logs -f' to monitor."

docker-down:
	docker compose down -v --remove-orphans

lint:
	go vet ./...

release: clean
	GOOS=linux   GOARCH=amd64 go build $(LDFLAGS) -o dist/$(BINARY)-linux-amd64   ./cmd/folddb
	GOOS=linux   GOARCH=arm64 go build $(LDFLAGS) -o dist/$(BINARY)-linux-arm64   ./cmd/folddb
	GOOS=darwin  GOARCH=amd64 go build $(LDFLAGS) -o dist/$(BINARY)-darwin-amd64  ./cmd/folddb
	GOOS=darwin  GOARCH=arm64 go build $(LDFLAGS) -o dist/$(BINARY)-darwin-arm64  ./cmd/folddb

testdata:
	go run testdata/gen.go

grammar:
	pigeon -o internal/sql/grammar/folddb.go internal/sql/grammar/folddb.peg

bench-full: build
	./bench/harness/run.sh --output bench/harness/results/$$(date +%Y%m%d_%H%M%S).json

bench-full-kafka: build
	./bench/harness/run.sh --with-kafka --output bench/harness/results/$$(date +%Y%m%d_%H%M%S).json

bench-compare: build
	./bench/harness/run.sh --compare bench/harness/baseline.json

stress: build
	@echo "Running stress tests (this may take 15-30 minutes)..."
	./stress/run.sh --output stress/results/$$(date +%Y%m%d_%H%M%S).json

stress-quick: build
	@echo "Running quick stress tests (~5 minutes)..."
	./stress/run.sh --duration 1m --scenarios "sustained/passthrough|adversarial/high_card|adversarial/schema" --output stress/results/$$(date +%Y%m%d_%H%M%S).json

clean:
	rm -rf dist/ $(BINARY) folddb-gen
