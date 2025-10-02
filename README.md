# AMQP 0-9-1 SDK (Go) — minimal, protocol-focused

This repository provides a minimal, protocol-focused AMQP 0-9-1 SDK in Go plus
example binaries. The SDK parses AMQP frames/methods, negotiates connection
parameters and delegates behavioral decisions (queues, routing, persistence)
to application-provided handlers. It is not a full broker — the SDK's job is
wire-protocol correctness and safe delegation.

## Key points
- Protocol-focused: parsing/serializing frames, content headers, field-tables.
- Delegation model: application supplies `ServerHandlers` (see API below).
- Structured logging: SDK uses `zerolog`; applications set the logger with
  `amqp.SetLogger(logger)`.

## Implemented features (high level)

 - ✅ Connection negotiation (Start/Tune/Open/Close)
 - ✅ Content header parsing (properties + `headers` field-table)
 - ✅ Basic.Publish parsing + delegation to `OnBasicPublish`
 - ✅ Basic.QoS (prefetch) negotiation + `qos-ok`
 - ✅ Channel Flow control (flow/flow-ok)
 - ✅ Heartbeat negotiation and sending
 - ✅ Basic.Return (publisher returns) and handler callback

## Project layout

 - `pkg/amqp` — SDK: frames, method parsing, ServeWithAuth, ConnContext, ServerHandlers.
- `cmd/server` — example server using `pkg/amqp` and a simple in-memory broker.
- `cmd/upstream` — example upstream proxy that forwards operations to a
  configured RabbitMQ instance (useful for development and integrating the
  SDK with a real broker).
- `cmd/publish` — example publisher (uses `github.com/rabbitmq/amqp091-go`) with
  flags for `--prefetch-count` and `--mandatory` to demonstrate QoS and returns.
- `cmd/consume` — example consumer (uses official client) for demo.

## Quick start

Prereqs: Go toolchain and (optionally) `openssl` for `make gen-certs`.

### Build server:

```bash
  make build
```

### Run server (plain TCP on 5672):

```bash
  make run
```

### Run an upstream proxy (delegates to RabbitMQ)

Start a proxy that accepts client connections and forwards operations to an
upstream RabbitMQ instance (default listens on :5673):

```bash
make run-upstream
```

The upstream proxy is implemented in `cmd/upstream` and accepts flags such as
`-upstream` (upstream URL), `-upstream-tls`, `-upstream-tls-skip-verify`,
`-failure-policy` and `-reconnect-delay`.

You can start a local RabbitMQ container (useful for development):

```bash
make rabbit-start   # starts rabbitmq:3-management with admin/admin
make rabbit-stop    # stop and remove it
```

### Generate self-signed certs (local TLS demo):

```bash
  make gen-certs
```

### Run TLS server (if certs generated):

### server will automatically start TLS listener on :5671 when certs exist

```bash
  make run
```

### Publish a message (example):

```bash
  make publish
```

Or run the example publisher with QoS/mandatory flags:

```go
  go run ./cmd/publish --addr amqp://guest:guest@127.0.0.1:5672/ --exchange "" --key test --body hello --prefetch-count 10 --mandatory=true
```

### Run unit tests:

```bash
  make test
```

## SDK usage (programmatic)

Set logger (recommended):

```go
logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
amqp.SetLogger(logger)
```

Provide handlers and start server:

```go
handlers := &amqp.ServerHandlers{
    OnBasicPublish: func(ctx amqp.ConnContext, channel uint16, exchange, rkey string, mandatory, immediate bool, props amqp.BasicProperties, body []byte) (bool, bool, error) {
        // Decide routing here (application code). Return (routed, nack, err).
        return false, false, nil
    },
    OnBasicQos: func(ctx amqp.ConnContext, channel uint16, prefetchSize uint32, prefetchCount uint16, global bool) error {
        // Application may adjust consumer behavior.
        return nil
    },
    OnChannelFlow: func(ctx amqp.ConnContext, channel uint16, active bool) (bool, error) {
        // Return the flow state the server should use.
        return active, nil
    },
    OnBasicReturn: func(ctx amqp.ConnContext, channel uint16, replyCode uint16, replyText, exchange, routingKey string, props amqp.BasicProperties, body []byte) error {
        // Notification that a published message was returned (mandatory/immediate)
        return nil
    },
}

if err := amqp.ServeWithAuth(":5672", nil, nil, handlers); err != nil {
    logger.Fatal().Err(err).Msg("server failed")
}
```

## Upstream adapter (integration with RabbitMQ)

The repository includes an `pkg/amqp/upstream` adapter that can be used to
bridge SDK clients to a real RabbitMQ broker. Key points:

- The adapter opens an upstream connection per client session and forwards
  exchanges, queues, publishes and consumer operations by default.
- It supports reconnection and a `failure-policy` (`reconnect`, `close`,
  `enqueue`) to control behavior when the upstream is unavailable.
- Consumers are restored on reconnect: subscriptions registered by clients are
  re-created on upstream reconnect so deliveries resume after RabbitMQ comes
  back.
- The adapter exposes an `AuthHandler` and a `Handlers()` helper so you can
  pass it directly to `amqp.ServeWithAuth`.
- The server SDK now calls an `OnConnClose` handler when a client connection
  ends; the upstream adapter uses this to close and clean up the upstream
  session and avoid leaking upstream connections.

Example: run a RabbitMQ container and the proxy, then run examples that point
to `amqp://...:5673` (the proxy) instead of directly to RabbitMQ.

If you restart RabbitMQ while the proxy is running the adapter will log the
disconnect, attempt reconnects, and restore consumers when the upstream is
available again (see proxy logs for reconnect messages).

## Notes and design

- The SDK purposely delegates all broker behavior to handlers so applications can
  implement storage, routing, and policies (or connect to a real RabbitMQ server).
- Field-table parsing supports common types; extend `pkg/amqp/encode.go` if you
  need more field-value types.
- The example `cmd/server` implements a tiny in-memory broker for demonstration
  and testing; real deployments should use RabbitMQ or an equivalent broker.

## Where to look next

- `pkg/amqp` — protocol implementation and handler APIs.
- `cmd/server` — example handlers wired to an in-memory broker.
- `cmd/publish` / `cmd/consume` — example publisher/consumer showing QoS and returns.

See `TODO.md` for a compatibility checklist and outstanding work.

## Contributing

Contributions welcome; please open issues for discussion before large changes.
