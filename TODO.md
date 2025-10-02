
## TODO / Compatibility Checklist

This document lists AMQP 0-9-1 areas and features and the current implementation
status in the SDK (`pkg/amqp`). The table below shows, for each feature, a simple
status value and a short inline note describing the current state when relevant.

Status icons: ✅ = Implemented, ❌ = Not implemented, ⚠️ = Partial — the icon is
shown next to the status word to make scanning the table easier.

| Area (class) | Feature | Status | Note |
|---|---|---:|---|
| Connection | `Start` / `Start-Ok` | ✅ Implemented | Handshake and PLAIN auth supported; `Start-Ok` parsed.
| Connection | `Tune` / `Tune-Ok` | ✅ Implemented | Basic Tune exchange implemented.
| Connection | `Open` / `Open-Ok` (vhost) | ✅ Implemented | `vhost` is parsed and available on `ConnContext.Vhost`.
| Connection | `Close` / `Close-Ok` | ✅ Implemented | Connection.Close/-Ok supported.
| Connection | `Secure` / `Secure-Ok` (SASL challenge) | ❌ Not implemented | SASL challenge flow not supported.
| Connection | `Blocked` / `Unblocked` | ❌ Not implemented | Connection.blocked/unblocked notifications not implemented.
| Connection | `Update-Secret` | ❌ Not implemented | Optional feature not supported.
| Channel | `Open` / `Open-Ok` | ✅ Implemented | Channel open/ok supported.
| Channel | `Close` / `Close-Ok` | ✅ Implemented | Channel close/ok supported.
| Channel | `Flow` / `Flow-Ok` | ✅ Implemented | Channel.flow parsed and delegated to `ServerHandlers.OnChannelFlow`; server responds with flow-ok.
| Exchange | `Declare` / `Declare-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnExchangeDeclare` (args passed to handler).
| Exchange | `Delete` / `Delete-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnExchangeDelete`; flags (`if-unused`,`nowait`) parsed; `nowait` suppresses reply.
| Exchange | `Bind` / `Bind-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnExchangeBind`; flags (`nowait`) parsed; `nowait` suppresses reply.
| Queue | `Declare` / `Declare-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnQueueDeclare`; `declare-ok` includes name and counters (currently zeros).
| Queue | `Bind` / `Bind-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnQueueBind`.
| Queue | `Unbind` / `Unbind-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnQueueUnbind`; fields parsed: queue, exchange, routing-key, arguments; `unbind-ok` always sent on success.
| Queue | `Purge` / `Purge-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnQueuePurge`; `purge-ok` includes `message-count` returned by handler.
| Queue | `Delete` / `Delete-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnQueueDelete`; flags (`if-unused`,`if-empty`,`nowait`) parsed; handler returns deleted message-count which is included in `delete-ok`.
| Basic | `Publish` | ✅ Implemented | Parsing and delegation implemented; SDK parses method flags (mandatory/immediate), content header properties (common properties + headers table) into `BasicProperties` and delegates to `ServerHandlers.OnBasicPublish(ctx, channel, exchange, rkey, mandatory, immediate, properties, body)`. Handler returns `(routed bool, nack bool, error)` — SDK will send `basic.return` when required and confirm ack/nack in confirm mode. Field-table parsing is basic (supports common types) and may be extended.
| Basic | `Deliver` (server→client) | ✅ Implemented | SDK sends `basic.deliver` frames; delivery behavior is the responsibility of the handler.
| Basic | `Consume` / `Consume-Ok` | ✅ Implemented | Delegated to `ServerHandlers.OnBasicConsume`; `consume-ok` is sent.
| Basic | `Get` / `Get-Ok` / `Get-Empty` | ✅ Implemented | Delegated to `ServerHandlers.OnBasicGet`.
| Basic | `Ack` (consumer→server ack) | ✅ Implemented | Delegated to `ServerHandlers.OnBasicAck(ctx, channel, deliveryTag, multiple)`; parses delivery-tag (longlong) and `multiple` flag.
| Basic | `Nack` / `Reject` | ✅ Implemented | Client notifications delegated via `OnBasicNack`/`OnBasicReject`; server can send `basic.nack` for publishes based on handler.
| Basic | `Return` (mandatory/immediate) | ✅ Implemented | Server sends `basic.return` when `mandatory`/`immediate` are set and handler indicates not routed; SDK notifies `ServerHandlers.OnBasicReturn`.
| Basic | `Qos` / `Qos-Ok` | ✅ Implemented | `basic.qos` parsed and delegated to `ServerHandlers.OnBasicQos`; SDK replies with `qos-ok`.
| Confirm | `Select` / `Select-Ok` | ⚠️ Partial | Basic confirm mode implemented (per-channel sequence, per-publish ack/nack); full confirm model not implemented.
| Tx | `Select` / `Commit` / `Rollback` | ❌ Not implemented | Transactions not supported.
| Content properties | Content header properties & field-table | ✅ Implemented | Content header properties parsed into `BasicProperties` including `headers` field-table (supporting common AMQP field-value types).
| Error handling | Reply-codes & spec errors | ⚠️ Partial | Some reply-codes used; error handling per-spec not exhaustive.
| Misc | Heartbeats | ✅ Implemented | Heartbeat negotiation and sending implemented; SDK sends heartbeats when negotiated and resets read deadlines on receive.
| Transport | TLS support | ✅ Implemented | `ServeWithListener` accepts TLS listeners; `ConnContext.TLSState` filled after handshake; `make gen-certs` available for local testing.

Use this table to track compatibility progress; update the "Status" column to
`Implemented`, `Not implemented` or `Partial` and keep the note on the same row
with relevant details about the current implementation state.
