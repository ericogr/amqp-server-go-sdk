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
	go run ./cmd/publish --addr amqp://guest:guest@127.0.0.1:$(DEFAULT_PORT)/ --exchange "" --key test --body "hello"

clean:
	rm -f server
