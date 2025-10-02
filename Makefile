DEFAULT_PORT ?= 5673
DEFAULT_URL ?= amqp://admin:admin@127.0.0.1:$(DEFAULT_PORT)/
DEFAULT_UPSTREAM_PORT ?= 5673
UPSTREAM_URL ?= amqp://admin:admin@127.0.0.1:5672/

.PHONY: all build run test clean

all: build

build:
	go build ./cmd/server ./cmd/upstream

run:
	go run ./cmd/server -addr :$(DEFAULT_PORT)

run-upstream:
	go run ./cmd/upstream -addr :$(DEFAULT_UPSTREAM_PORT) -upstream $(UPSTREAM_URL)

test:
	go test ./... -v

.PHONY: itest
itest:
	@echo "Starting RabbitMQ container for integration tests (requires docker)..."
	@docker run -d --name amqp-it-test -p 5672:5672 rabbitmq:3-management >/dev/null || (echo "docker run failed"; exit 1)
	@echo "Waiting for RabbitMQ to start..."
	@sleep 8
	RABBIT_URL=amqp://guest:guest@127.0.0.1:5672/ go test -tags=integration ./pkg/amqp/upstream -v || true
	@echo "Stopping RabbitMQ container..."
	@docker stop amqp-it-test >/dev/null || true

.PHONY: rabbit-start rabbit-stop

rabbit-start:
	@echo "Starting RabbitMQ container (admin/admin)..."
	@docker run -d --name amqp-rabbit -p 5671:5671 -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin rabbitmq:3-management >/dev/null || (echo "docker run failed"; exit 1)
	@echo "Waiting for RabbitMQ to start..."
	@sleep 8

rabbit-stop:
	@echo "Stopping RabbitMQ container..."
	@docker stop amqp-rabbit >/dev/null || true
	@docker rm amqp-rabbit >/dev/null || true

publish:
	go run ./cmd/publish --addr $(DEFAULT_URL) --exchange "" --key test --body "hello" --exchange testx --queue testq --delete-exchange true

consume:
	go run ./cmd/consume --addr $(DEFAULT_URL) --queue test-queue

.PHONY: gen-certs

gen-certs:
	@mkdir -p tls
	@echo "Generating self-signed TLS certs into tls/ (CN=localhost)"
	@openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout tls/server.key -out tls/server.pem -subj "/CN=localhost" || (rm -f tls/server.key tls/server.pem; echo "openssl required"; false)

clean:
	rm -f server
