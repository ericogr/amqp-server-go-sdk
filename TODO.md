
## TODO / Compatibility Checklist

This document lists AMQP 0-9-1 areas and features and the current implementation
status in the SDK (`pkg/amqp`). The table below shows, for each feature, a simple
status value and a short inline note describing the current state when relevant.

| Area (class) | Feature | Status | Note |
|---|---|---:|---|
| Connection (10) | `Start` / `Start-Ok` | Implemented | Handshake and PLAIN auth supported; `Start-Ok` parsed.
| Connection (10) | `Tune` / `Tune-Ok` | Implemented | Basic Tune exchange implemented.
| Connection (10) | `Open` / `Open-Ok` (vhost) | Implemented | `vhost` is parsed and available on `ConnContext.Vhost`.
| Connection (10) | `Close` / `Close-Ok` | Implemented | Connection.Close/-Ok supported.
| Connection (10) | `Secure` / `Secure-Ok` (SASL challenge) | Not implemented | SASL challenge flow not supported.
| Connection (10) | `Blocked` / `Unblocked` | Not implemented | Connection.blocked/unblocked notifications not implemented.
| Connection (10) | `Update-Secret` | Not implemented | Optional feature not supported.
| Channel (20) | `Open` / `Open-Ok` | Implemented | Channel open/ok supported.
| Channel (20) | `Close` / `Close-Ok` | Implemented | Channel close/ok supported.
| Channel (20) | `Flow` / `Flow-Ok` | Not implemented | Flow control not implemented.
| Exchange (40) | `Declare` / `Declare-Ok` | Implemented | Delegated to `ServerHandlers.OnExchangeDeclare` (args passed to handler).
| Exchange (40) | `Delete` / `Delete-Ok` | Implemented | Delegated to `ServerHandlers.OnExchangeDelete`; flags (`if-unused`,`nowait`) parsed; `nowait` suppresses reply.
| Exchange (40) | `Bind` / `Bind-Ok` | Implemented | Delegated to `ServerHandlers.OnExchangeBind`; flags (`nowait`) parsed; `nowait` suppresses reply.
| Queue (50) | `Declare` / `Declare-Ok` | Implemented | Delegated to `ServerHandlers.OnQueueDeclare`; `declare-ok` includes name and counters (currently zeros).
| Queue (50) | `Bind` / `Bind-Ok` | Implemented | Delegated to `ServerHandlers.OnQueueBind`.
| Queue (50) | `Unbind` / `Unbind-Ok` | Implemented | Delegated to `ServerHandlers.OnQueueUnbind`; fields parsed: queue, exchange, routing-key, arguments; `unbind-ok` always sent on success.
| Queue (50) | `Purge` / `Purge-Ok` | Implemented | Delegated to `ServerHandlers.OnQueuePurge`; `purge-ok` includes `message-count` returned by handler.
| Queue (50) | `Delete` / `Delete-Ok` | Implemented | Delegated to `ServerHandlers.OnQueueDelete`; flags (`if-unused`,`if-empty`,`nowait`) parsed; handler returns deleted message-count which is included in `delete-ok`.
| Basic (60) | `Publish` | Partial | Parsing and delegation implemented; `OnBasicPublish` returns `(nack bool, error)` and confirm-mode ack/nack is sent; limited handling of properties/mandatory/return.
| Basic (60) | `Deliver` (server→client) | Implemented | SDK sends `basic.deliver` frames; delivery behavior is the responsibility of the handler.
| Basic (60) | `Consume` / `Consume-Ok` | Implemented | Delegated to `ServerHandlers.OnBasicConsume`; `consume-ok` is sent.
| Basic (60) | `Get` / `Get-Ok` / `Get-Empty` | Implemented | Delegated to `ServerHandlers.OnBasicGet`.
| Basic (60) | `Ack` (consumer→server ack) | Not implemented | Consumer acknowledgements (client→server) for consumption are not delegated; SDK uses acks for publisher confirms.
| Basic (60) | `Nack` / `Reject` | Implemented | Client notifications delegated via `OnBasicNack`/`OnBasicReject`; server can send `basic.nack` for publishes based on handler.
| Basic (60) | `Return` (mandatory/immediate) | Not implemented | Returning unroutable messages to publisher not implemented.
| Basic (60) | `Qos` / `Qos-Ok` | Not implemented | Prefetch/QoS not implemented.
| Confirm (85) | `Select` / `Select-Ok` | Partial | Basic confirm mode implemented (per-channel sequence, per-publish ack/nack); full confirm model not implemented.
| Tx (90) | `Select` / `Commit` / `Rollback` | Not implemented | Transactions not supported.
| Content properties | Content header properties & field-table | Partial | Only `body-size` parsed/serialized; property flags and field-table mostly stubbed.
| Error handling | Reply-codes & spec errors | Partial | Some reply-codes used; error handling per-spec not exhaustive.
| Misc | Heartbeats | Partial | Heartbeat frame type recognized; minimal handling.
| Transport | TLS support | Implemented | `ServeWithListener` accepts TLS listeners; `ConnContext.TLSState` filled after handshake; `make gen-certs` available for local testing.

Use this table to track compatibility progress; update the "Status" column to
`Implemented`, `Not implemented` or `Partial` and keep the note on the same row
with relevant details about the current implementation state.
