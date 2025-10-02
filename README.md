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
