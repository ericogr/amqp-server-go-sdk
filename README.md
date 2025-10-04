# AMQP 0-9-1 SDK (Go) — minimal, protocol-focused

This repository provides a minimal, protocol-focused AMQP 0-9-1 SDK in Go plus
example binaries. The SDK parses AMQP frames/methods, negotiates connection
parameters and delegates behavioral decisions (queues, routing, persistence)
to application-provided handlers. It is not a full broker — the SDK's job is
wire-protocol correctness and safe delegation.

![AMQP SDK logo](doc/logo.svg)

**SDK as Protocol Intermediary**

```mermaid
graph LR
  Pub(Publisher App)
  Con(Consumer App)
  SDK(AMQP SDK - protocol implementation)
  H(Handlers / ServerHandlers - customization)
  Up(Upstream broker or custom backend)

  Pub -->|AMQP frames| SDK
  Con -->|AMQP frames| SDK
  SDK -->|parses frames| H
  H -->|custom logic/routing| SDK
  SDK -->|optional upstream| Up
  Up -->|deliveries / confirms| SDK
```

The diagram shows the SDK sitting between AMQP clients and application logic.
It implements the AMQP 0-9-1 wire protocol, exposes `ServerHandlers` so
applications can inject custom behavior (routing, storage, validation), and
optionally forwards operations to an upstream broker like RabbitMQ.

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

### Build server

Builds the example server and upstream proxy binaries (`cmd/server` and `cmd/upstream`) using `go build`.
This produces compiled executables (named `server` and `upstream`) in the repository root so you can run them directly
without `go run`. Use `make build` when you want a local binary for testing, debugging, or packaging for deployment.

```bash
make build
```

### Run server (plain TCP on 5672)

Starts the example server — a minimal in-memory AMQP broker that accepts
client connections over plain TCP and delegates protocol handling to the
SDK handlers. The server listens on `:5672` by default and accepts a custom
address via the `-addr` flag.

Notes:

- `make run-basic-server` (Makefile) starts the server on `:5673` by default to avoid
  colliding with a locally installed RabbitMQ. Use `make run-basic-server` for a quick
  demo run.
- To run the server directly or specify a different address, use:

```bash
go run ./cmd/server -addr :5672    # run from source (default :5672)
  ./server -addr :5672               # run built binary
```

- TLS: the server will also start a TLS listener on `:5671` if certificate
  files exist at `tls/server.pem` and `tls/server.key`. Generate test certs
  with `make gen-certs`.
- Authentication: the example accepts the PLAIN mechanism with credentials
  `admin:admin` (see `cmd/server` for the simple auth handler). Replace or
  remove this in real deployments.

Use this server for local testing, debugging client behavior (QoS, basic.return,
consumer flows) or exercising the SDK without a full RabbitMQ broker.

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
make run-basic-server
```

### Publish a message (example):

```bash
make publish
```

Or run the example publisher with QoS/mandatory flags:

```go
  go run ./cmd/publish --addr amqp://admin:admin@127.0.0.1:5672/ --exchange "" --key test --body hello --prefetch-count 10 --mandatory=true
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

## Architecture

```mermaid
graph LR
  P(Publisher)
  S(SDK)
  UA(Upstream Adapter)
  R(RabbitMQ)
  C(Consumer)

  P -->|basic.publish| S
  S -->|onBasicPublish| UA
  UA -->|upstream publish| R
  R -->|deliver| UA
  UA -->|basic.deliver| S
  S -->|basic.deliver| C
  C -->|basic.ack / basic.nack| S
  S -->|map ack/nack| UA
  UA -->|upstream ack/nack| R
```

Explanation

The diagram shows a typical deployment where two AMQP clients (a publisher
and a consumer) connect to the AMQP SDK acting as a protocol-aware server.
The SDK implements frame parsing and exposes `ServerHandlers`. The
`UpstreamAdapter` implements those handlers and maintains a separate upstream
AMQP connection to a real RabbitMQ broker. Publish and consume operations are
forwarded: `Basic.Publish` from a client becomes an upstream publish; upstream
deliveries are translated into `Basic.Deliver` frames and written back to the
client. Acks, nacks and other control frames are mapped in both directions so
client acknowledgements affect the upstream broker and upstream confirms are
propagated back to publishers.

### Example scenario: publisher + consumer via the upstream proxy

Steps to run a minimal demo that shows how the SDK proxies messages to RabbitMQ:

- Start RabbitMQ (local container):

```bash
make rabbit-start
```

- Start the upstream proxy (SDK + UpstreamAdapter listening on :5673 by default):

```bash
make run-upstream
# or: go run ./cmd/upstream -addr :5673 -upstream amqp://admin:admin@127.0.0.1:5672/
```

- Start a consumer (connects to the proxy and issues `basic.consume`):

```bash
make consume
# or: go run ./cmd/consume --addr amqp://admin:admin@127.0.0.1:5673/ --queue test-queue
```

- Publish a message (connects to the proxy and issues `basic.publish`):

```bash
make publish
# or: go run ./cmd/publish --addr amqp://admin:admin@127.0.0.1:5673/ --exchange "" --key test --body "hello"
```

What happens (step-by-step):

1. The consumer declares a queue and calls `basic.consume` against the SDK.
   The `UpstreamAdapter.OnBasicConsume` creates a consumer on the real
   RabbitMQ instance and registers a mapping between upstream delivery-tags
   and the client-side delivery-tags.
2. The publisher sends `basic.publish` to the SDK. The SDK parses the frames
   and calls `UpstreamAdapter.OnBasicPublish`, which publishes the message on
   the upstream RabbitMQ connection. If confirm mode is enabled the adapter
   maps upstream confirms to `basic.ack`/`basic.nack` to the publisher.
3. RabbitMQ routes the message to the queue and delivers it to the upstream
   consumer. The adapter receives the upstream delivery and forwards it to
   the client by writing `basic.deliver` + content header + body frames via
   the SDK. The client receives the message and processes it.
4. When the client issues `basic.ack` / `basic.nack` / `basic.reject`, the
   SDK maps the client delivery-tag back to the upstream delivery-tag and the
   adapter calls the corresponding upstream method so the broker's state is
   updated.

Failure handling: the adapter supports configurable failure policies
(`FailReconnect`, `FailEnqueue`, `FailCloseClient`). For example, with
`FailEnqueue` the adapter will buffer publishes in memory while RabbitMQ is
down and drain them on reconnect; `FailReconnect` will try reconnect attempts
and restore consumers when the broker becomes available again.

Quick wiring example (programmatic)

```go
import (
  "log"
  "github.com/ericogr/go-amqp-server-sdk/pkg/amqp"
  "github.com/ericogr/go-amqp-server-sdk/pkg/amqp/upstream"
)

cfg := upstream.UpstreamConfig{URL: "amqp://admin:admin@127.0.0.1:5672/", FailurePolicy: upstream.FailReconnect}
adapter := upstream.NewUpstreamAdapter(cfg)
handlers := adapter.Handlers()

go func() {
  if err := amqp.ServeWithAuth(":5673", nil, adapter.AuthHandler, handlers); err != nil {
    log.Fatal(err)
  }
}()
```

The adapter and SDK together let application developers intercept, inspect
or modify AMQP operations while keeping compatibility with RabbitMQ and the
AMQP 0-9-1 protocol.

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

See [TODO.md](TODO.md) for a compatibility checklist and outstanding work.

## Contributing

Contributions welcome; please open issues for discussion before large changes.
