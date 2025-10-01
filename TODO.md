# TODO / Compatibility Checklist

This document lists features from the AMQP 0-9-1 specification (see `doc/amqp0-9-1.extended.xml`) and the current implementation status in `pkg/amqp` and the example server (`cmd/server`). The SDK intentionally parses the wire protocol and delegates behavioral decisions to application-provided handlers (`ServerHandlers`). Use this checklist to track work required for fuller spec compliance.

| Area (class) | Method(s) / Feature | Status (SDK) | Status (default server) | Notes / Action Required |
|---|---|---:|---|---|
| Connection (10) | `Start` / `Start-Ok` | Implemented (handshake & PLAIN parsing) | n/a | PLAIN auth delegated via AuthHandler; OK.
| Connection (10) | `Tune` / `Tune-Ok` | Implemented | n/a | Basic tune exchange implemented.
| Connection (10) | `Open` / `Open-Ok` | Implemented | n/a | OK.
| Connection (10) | `Close` / `Close-Ok` | Implemented | n/a | OK.
| Connection (10) | `Secure` / `Secure-Ok` (SASL challenge) | Missing | Missing | Add SASL challenge flow if needed.
| Connection (10) | `Blocked` / `Unblocked` | Missing | Missing | Implement notifications and allow handlers to react.
| Connection (10) | `Update-Secret` | Missing | Missing | Optional (OAuth-like) support.
| Channel (20) | `Open` / `Open-Ok` | Implemented | n/a | OK.
| Channel (20) | `Close` / `Close-Ok` | Implemented | n/a | OK.
| Channel (20) | `Flow` / `Flow-Ok` (flow control) | Missing | Missing | Requires handler support to pause/resume publishers/consumers.
| Exchange (40) | `Declare` / `Declare-Ok` | Delegated to handlers | Default handler: record existence (minimal) | Need full argument parsing, passive/if-unused, durable, auto-delete semantics.
| Exchange (40) | `Delete` / `Delete-Ok` | Implemented (delegated) | Default server: removes exchange from in-memory map; delegates to `ServerHandlers.OnExchangeDelete` | Tests added (pkg/amqp): `TestServerDelegatesExchangeQueueDelete`.
| Exchange (40) | `Bind` / `Bind-Ok` (exchange→exchange) | Missing / Delegated | Missing | Complex; implement binding storage and routing logic.
| Queue (50) | `Declare` / `Declare-Ok` | Delegated to handlers | Default handler: create minimal queue state | Implement passive, durable, exclusive, auto-delete, arguments.
| Queue (50) | `Bind` / `Bind-Ok` (queue→exchange) | Delegated | Default handler: noop-record | Add full binding semantics and argument handling.
| Queue (50) | `Purge` / `Purge-Ok` | Missing | Missing | Implement counts and reply.
| Queue (50) | `Delete` / `Delete-Ok` | Implemented (delegated) | Default server: removes queue from in-memory state; delegates to `ServerHandlers.OnQueueDelete` | Tests added (pkg/amqp): `TestServerDelegatesExchangeQueueDelete`.
| Basic (60) | `Publish` | Partially (parsing + delegation; handler now returns `(nack bool, error)`) | Default handler: basic routing for default exchange & queue | Add mandatory/immediate handling, properties parsing, and return behavior; handler can request server-side `basic.nack` in confirm mode.
| Basic (60) | `Deliver` (server→client) | SDK supports sending frames; delegated to handlers | Default handler: sends `basic.deliver` to first consumer, enqueues otherwise | Need to honor `redelivered`, `consumer-tag`, `multiple` semantics and consumer-selection rules.
| Basic (60) | `Consume` / `Consume-Ok` | Delegated | Default handler: registers consumer, delivers queued message | Implement flags (`no-local`, `exclusive`, `no-ack`) properly.
| Basic (60) | `Get` / `Get-Ok` / `Get-Empty` | Delegated | Default handler: supports simple get semantics | Implement `message-count`, no-ack behavior and edge cases.
| Basic (60) | `Ack` (publisher confirmations vs. consumer ack) | SDK sends `basic.ack` for publisher confirms (server-side ack) | Default handler acks publisher after delegation | Consumer acks (client→server consumption) not implemented.
| Basic (60) | `Nack` / `Reject` | Implemented (delegated) | Default server: delegates client Nack/Reject to `ServerHandlers.OnBasicNack` / `OnBasicReject`; server emits `basic.nack` when `OnBasicPublish` returns `nack==true` in confirm mode | Tests added (pkg/amqp): `TestServerDelegatesBasicNackReject`, `TestServePublishConfirmWithNack`.
| Basic (60) | `Return` (mandatory/immediate) | Missing | Missing | Implement returning unroutable messages to publisher.
| Basic (60) | `Qos` / `Qos-Ok` | Missing | Missing | Implement prefetch / windowing semantics.
| Confirm (85) | `Select` / `Select-Ok` | Implemented (SDK enables confirm mode and server replies) | Default server: supports acks for published seq | Missing: full confirm model (multiple, nacks, listeners, resequencing).
| Tx (90) | `Select` / `Commit` / `Rollback` | Missing | Missing | Transactions not supported.
| Content properties | Content header properties & field-table | Partial: only `body-size` parsed and encoded; field-table stubbed | Default server ignores properties/arguments | Implement full property flags and field-table (maps, types) parsing & encoding.
| Error handling | Specified reply-codes and exceptions | Partial | Partial | Many reply-codes and synchronous error rules not exhaustively implemented.
| Misc | Heartbeats | Frame type recognized; heartbeat handling minimal | Minimal | Implement proper heartbeat detection and peer liveness.

Notes
- "Delegated" means the SDK parses the wire protocol and calls the function in `ServerHandlers`.
- The default server (`cmd/server`) provides a minimal in-memory behavior for demos and tests; it is not a full broker.
- To reach full spec compliance, each row above marked Missing/Partial needs detailed sub‑tasks and tests driven by the spec.

Recent updates
- Added delegation for client `basic.nack` / `basic.reject` via `ServerHandlers.OnBasicNack` and `OnBasicReject`.
- Changed `ServerHandlers.OnBasicPublish` signature to return `(nack bool, error)` so handlers can request server-side `basic.nack` in confirm mode.
- Updated example server (`cmd/server/main.go`) to use the new signature and added no-op Nack/Reject handlers.
- Added tests in `pkg/amqp`: `TestServerDelegatesBasicNackReject` and `TestServePublishConfirmWithNack`.
