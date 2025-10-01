- Connection (class 10)
    - Connection.Secure / Secure-Ok (SASL challenge/response)
    - Connection.Blocked / Connection.Unblocked
    - Connection.Update-Secret / Update-Secret-Ok
- Channel (class 20)
    - Channel.Flow / Flow-Ok (flow control)
- Exchange (class 40)
    - Exchange.Declare / Declare-Ok
    - Exchange.Delete / Delete-Ok
    - Exchange.Bind / Bind-Ok
    - Exchange.Unbind / Unbind-Ok
- Queue (class 50)
    - Queue.Declare / Declare-Ok
    - Queue.Bind / Bind-Ok
    - Queue.Unbind / Unbind-Ok
    - Queue.Purge / Purge-Ok
    - Queue.Delete / Delete-Ok
- Basic (class 60)
    - Basic.Qos / Qos-Ok
    - Basic.Consume / Consume-Ok (registro de consumidores)
    - Basic.Cancel / Cancel-Ok
    - Basic.Deliver (servidor -> cliente para entregar a consumidores)
    - Basic.Return (servidor -> cliente para mensagens unroutable)
    - Basic.Get / Get-Ok / Get-Empty
    - Basic.Nack / Basic.Reject
    - Basic.Recover / Recover-Ok / Recover-Async
    - (Nota: Basic.Ack já é enviado, mas falta suporte completo a flags
multiple, redelivery e políticas de requeue)
- Confirm (class 85)
    - Confirm.Select/Select-Ok já adicionados, porém falta:
        - comportamento completo de confirmações (suporte a multiple ack/
nack, resequencing/confirm listeners, nacks aos publishers)
- Tx (class 90)
    - Tx.Select / Select-Ok
    - Tx.Commit / Commit-Ok
    - Tx.Rollback / Rollback-Ok
- Infra/serialização faltantes
    - Codificação/decodificação completa de field-table (hoje só há helper
encodeFieldTableEmpty)
    - Parse/serialize completo de propriedades de header (content-
properties, flags)
    - Helpers para construir/parsear os argumentos de muitos métodos
(declare, bind, queue args, etc.)