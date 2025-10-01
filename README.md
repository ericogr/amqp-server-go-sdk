# Minimal AMQP 0-9-1 Server and SDK (Go)

This repository contains a minimal, educational implementation of parts of the AMQP 0-9-1
wire protocol in Go and a tiny example server that accepts `basic.publish` messages.

Important: this SDK parses the AMQP wire protocol and provides a delegation model for
handling high level operations (exchanges, queues, publishes, consumes). It does NOT
implement a full AMQP broker. Instead, the SDK calls application-provided handlers
(`ServerHandlers`) through a connection context (`ConnContext`) so that the application
implements policy and behavior (e.g. queue management, routing, persistence).

This design keeps the SDK focused on protocol handling and lets applications (or the
example server in `cmd/server`) implement the broker semantics they need.

The project is intended as a learning/demo SDK — it is NOT a full AMQP broker and
should not be used in production.

Contents
- `pkg/amqp` - AMQP frame/method helpers and `Serve` helpers. The SDK focuses on parsing
  and producing frames and delegating behavioral decisions to handlers.
- `cmd/server` - example server that uses the SDK and provides a minimal default behavior
  implemented with `ServerHandlers` (in-memory queues) for demo and tests.
- `cmd/publish` - example publisher that uses the official RabbitMQ Go client
  (`github.com/rabbitmq/amqp091-go`) to publish a message to the local server.
- `Makefile` - convenience targets: `build`, `run`, `test`, `publish`, `clean`.

TLS support

- The server can accept TLS connections. If `tls/server.pem` and `tls/server.key`
  exist the example server will start a TLS listener on `:5671` in addition to the
  plain TCP listener on `:5672`.
- Use `make gen-certs` to generate a self-signed certificate pair for local testing.
- Handlers receive `ConnContext` which now includes `Vhost` and `TLSState` so
  authentication handlers can inspect the requested virtual-host and the
  client/server TLS connection state (e.g. client certificates).

Quick start

1. Build (optional):

   make build

2. Run the minimal server (default port `:5672`):

   make run

3. In another terminal, publish a test message (the example publisher connects to
   `amqp://guest:guest@127.0.0.1:5672/` by default):

   make publish

You should see the published message printed by the server and the publisher should
exit quickly after receiving the broker handshake responses.

Tests

Run unit tests with:

   make test

This runs `go test ./... -v` and includes tests that validate frame read/write and a
publish→ack roundtrip using `net.Pipe`.

Using the SDK

The main helper is `pkg/amqp.Serve(addr string, handler func(channel uint16, body []byte) error)`.
Example usage in `cmd/server`:

  - start the server: `amqp.Serve(":5672", handler)`
  - handler receives the channel id and the message body (raw bytes).

Limitations

- Implements only the minimal subset of AMQP 0-9-1 to accept `basic.publish`:
  handshake (Start/Tune/Open), channel open, basic.publish header+body, and basic.ack.
- No queues, no exchanges, no routing, no persistence, no real authentication/authorization.
- Limited property parsing and limited error handling. Frame sizes are capped (1MB).

Notes

- The `cmd/publish` example uses the official RabbitMQ client. It performs a graceful
  close handshake; the server implements `connection.close` and `channel.close` replies
  so the client can finish immediately.
- This project is for experimentation and demonstration only.

Contributing

Pull requests are welcome. Please keep changes small and focused.
