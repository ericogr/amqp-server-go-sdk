DEFAULT_PORT ?= 5672

.PHONY: all build run test clean

all: build

build:
	go build ./cmd/server

run:
	go run ./cmd/server -addr :$(DEFAULT_PORT)

test:
	go test ./... -v

publish:
	go run ./cmd/publish --addr amqp://guest:guest@127.0.0.1:$(DEFAULT_PORT)/ --exchange "" --key test --body "hello" --exchange testx --queue testq --delete-exchange true

consume:
	go run ./cmd/consume --addr amqps://guest:guest@127.0.0.1:5671/ --queue test-queue

.PHONY: gen-certs

gen-certs:
	@mkdir -p tls
	@echo "Generating self-signed TLS certs into tls/ (CN=localhost)"
	@openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout tls/server.key -out tls/server.pem -subj "/CN=localhost" || (rm -f tls/server.key tls/server.pem; echo "openssl required"; false)

clean:
	rm -f server
