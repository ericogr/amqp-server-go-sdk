package amqp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// handleMethodFrame processes a single method frame. It returns nil to continue,
// io.EOF or other error to stop the connection.
func handleMethodFrame(conn net.Conn, ctx ConnContext, f Frame, compatHandler func(ctx ConnContext, channel uint16, body []byte) error, handlers *ServerHandlers, channelStates map[uint16]*channelState) error {
	classID, methodID, args, err := ParseMethod(f.Payload)
	if err != nil {
		return err
	}

	// quick-path: handle Basic.Publish in a dedicated helper
	if classID == classBasic && methodID == methodBasicPublish {
		if err := handleBasicPublish(ctx, f, args, handlers, compatHandler, channelStates); err != nil {
			return err
		}
		return nil
	}

	// basic.qos (prefetch)
	if classID == classBasic && methodID == methodBasicQos {
		// args: prefetch-size (long), prefetch-count (short), global (bit)
		var prefetchSize uint32
		var prefetchCount uint16
		var global bool
		if len(args) >= 7 {
			prefetchSize = binary.BigEndian.Uint32(args[0:4])
			prefetchCount = binary.BigEndian.Uint16(args[4:6])
			if args[6]&1 == 1 {
				global = true
			}
		}
		if handlers != nil && handlers.OnBasicQos != nil {
			if err := handlers.OnBasicQos(ctx, f.Channel, prefetchSize, prefetchCount, global); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "basic.qos failed", classBasic, methodBasicQos); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
		}
		// respond with qos-ok
		if err := ctx.WriteMethod(f.Channel, classBasic, methodBasicQosOk, []byte{}); err != nil {
			logger.Error().Err(err).Msg("[server] write qos-ok error")
			return err
		}
		return nil
	}

	// handle incoming client Basic.Nack (class 60 method 120)
	if classID == classBasic && methodID == methodBasicNack {
		if len(args) < 9 {
			return nil
		}
		dtag := binary.BigEndian.Uint64(args[0:8])
		var multiple bool
		if len(args) >= 9 && args[8]&1 == 1 {
			multiple = true
		}
		var requeue bool
		if len(args) >= 9 && args[8]&2 == 2 {
			requeue = true
		}
		if handlers != nil && handlers.OnBasicNack != nil {
			if herr := handlers.OnBasicNack(ctx, f.Channel, dtag, multiple, requeue); herr != nil {
				logger.Error().Err(herr).Msg("[server] OnBasicNack handler error")
			}
		}
		return nil
	}

	// handle incoming client Basic.Reject (class 60 method 90)
	if classID == classBasic && methodID == methodBasicReject {
		if len(args) < 9 {
			return nil
		}
		dtag := binary.BigEndian.Uint64(args[0:8])
		var requeue bool
		if len(args) >= 9 && args[8]&1 == 1 {
			requeue = true
		}
		if handlers != nil && handlers.OnBasicReject != nil {
			if herr := handlers.OnBasicReject(ctx, f.Channel, dtag, requeue); herr != nil {
				logger.Error().Err(herr).Msg("[server] OnBasicReject handler error")
			}
		}
		return nil
	}

	// handle incoming client Basic.Ack (class 60 method 80)
	if classID == classBasic && methodID == methodBasicAck {
		if len(args) < 9 {
			// legacy servers may send just 8 bytes (delivery-tag) without flags
			if len(args) >= 8 {
				dtag := binary.BigEndian.Uint64(args[0:8])
				if handlers != nil && handlers.OnBasicAck != nil {
					if herr := handlers.OnBasicAck(ctx, f.Channel, dtag, false); herr != nil {
						logger.Error().Err(herr).Msg("[server] OnBasicAck handler error")
					}
				}
			}
			return nil
		}
		dtag := binary.BigEndian.Uint64(args[0:8])
		var multiple bool
		if len(args) >= 9 && args[8]&1 == 1 {
			multiple = true
		}
		if handlers != nil && handlers.OnBasicAck != nil {
			if herr := handlers.OnBasicAck(ctx, f.Channel, dtag, multiple); herr != nil {
				logger.Error().Err(herr).Msg("[server] OnBasicAck handler error")
			}
		}
		return nil
	}

	logger.Debug().Uint16("chan", f.Channel).Int("class", int(classID)).Int("method", int(methodID)).Int("args", len(args)).Msg("recv method")
	// handle Connection.Close (class 10 method 50)
	if classID == classConnection && methodID == methodConnClose {
		// reply-code (short) may be present in args
		var replyCode uint16
		if len(args) >= 2 {
			replyCode = binary.BigEndian.Uint16(args[0:2])
		}
		logger.Debug().Int("reply_code", int(replyCode)).Int("args_len", len(args)).Msg("recv connection.close")
		// respond with Connection.Close-Ok (class 10 method 51) and close connection
		if err := ctx.WriteMethod(0, classConnection, methodConnCloseOk, []byte{}); err != nil {
			logger.Error().Err(err).Msg("[server] write close-ok error")
		}
		logger.Debug().Uint16("chan", 0).Int("class", int(classConnection)).Int("method", int(methodConnCloseOk)).Msg("send method close-ok")
		return io.EOF
	}
	// channel open
	if classID == classChannel && methodID == methodChannelOpen {
		// create channel state
		channelStates[f.Channel] = &channelState{confirming: false, publishSeq: 0}
		// respond with channel.open-ok on same channel (reserved longstr)
		if err := ctx.WriteMethod(f.Channel, classChannel, methodChannelOpenOk, encodeLongStr("")); err != nil {
			logger.Error().Err(err).Msg("[server] write channel.open-ok error")
			return err
		}
		logger.Debug().Uint16("chan", f.Channel).Int("class", int(classChannel)).Int("method", int(methodChannelOpenOk)).Msg("channel.open-ok")
		return nil
	}
	// channel close: reply with channel.close-ok on same channel
	if classID == classChannel && methodID == methodChannelClose {
		if err := ctx.WriteMethod(f.Channel, classChannel, methodChannelCloseOk, []byte{}); err != nil {
			logger.Error().Err(err).Msg("[server] write channel.close-ok error")
			return err
		}
		logger.Debug().Uint16("chan", f.Channel).Int("class", int(classChannel)).Int("method", int(methodChannelCloseOk)).Msg("channel.close-ok")
		// notify optional handler so adapters can cleanup per-channel state
		if handlers != nil && handlers.OnChannelClose != nil {
			if cerr := handlers.OnChannelClose(ctx, f.Channel); cerr != nil {
				logger.Error().Err(cerr).Msg("[server] OnChannelClose handler error")
			}
		}
		return nil
	}

	// exchange.declare (delegate to handlers)
	if classID == classExchange && methodID == methodExchangeDeclare {
		// parse args: reserved-1 then exchange shortstr then type shortstr
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		exch := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				exch = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		kind := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				kind = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		if handlers != nil && handlers.OnExchangeDeclare != nil {
			if err := handlers.OnExchangeDeclare(ctx, f.Channel, exch, kind, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "exchange.declare failed", classExchange, methodExchangeDeclare); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
		}
		if err := ctx.WriteMethod(f.Channel, classExchange, methodExchangeDeclareOk, []byte{}); err != nil {
			logger.Error().Err(err).Msg("[server] write exchange.declare-ok error")
			return err
		}
		return nil
	}

	// exchange.bind (delegate)
	if classID == classExchange && methodID == methodExchangeBind {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		dest := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				dest = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		src := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				src = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		rkey := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				rkey = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		// nowait flag (optional)
		var nowait bool
		if idx < len(args) {
			flags := args[idx]
			if flags&1 == 1 {
				nowait = true
			}
		}
		if handlers != nil && handlers.OnExchangeBind != nil {
			if err := handlers.OnExchangeBind(ctx, f.Channel, dest, src, rkey, nowait, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "exchange.bind failed", classExchange, methodExchangeBind); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
		}
		if !nowait {
			if err := ctx.WriteMethod(f.Channel, classExchange, methodExchangeBindOk, []byte{}); err != nil {
				logger.Error().Err(err).Msg("[server] write exchange.bind-ok error")
				return err
			}
		}
		return nil
	}

	// exchange.unbind (delegate)
	if classID == classExchange && methodID == methodExchangeUnbind {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		dest := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				dest = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		src := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				src = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		rkey := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				rkey = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		// no-wait (single octet)
		var nowait bool
		if idx < len(args) {
			if args[idx] != 0 {
				nowait = true
			}
		}
		if handlers != nil && handlers.OnExchangeUnbind != nil {
			if err := handlers.OnExchangeUnbind(ctx, f.Channel, dest, src, rkey, nowait, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "exchange.unbind failed", classExchange, methodExchangeUnbind); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
		}
		if !nowait {
			if err := ctx.WriteMethod(f.Channel, classExchange, methodExchangeUnbindOk, []byte{}); err != nil {
				logger.Error().Err(err).Msg("[server] write exchange.unbind-ok error")
				return err
			}
		}
		return nil
	}

	// exchange.delete (delegate)
	if classID == classExchange && methodID == methodExchangeDelete {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		exch := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				exch = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		// parse flags: if-unused (bit1), nowait (bit2)
		var ifUnused bool
		var nowait bool
		if idx < len(args) {
			flags := args[idx]
			if flags&1 == 1 {
				ifUnused = true
			}
			if flags&2 == 2 {
				nowait = true
			}
		}
		if handlers != nil && handlers.OnExchangeDelete != nil {
			if err := handlers.OnExchangeDelete(ctx, f.Channel, exch, ifUnused, nowait, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "exchange.delete failed", classExchange, methodExchangeDelete); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
		}
		if !nowait {
			if err := ctx.WriteMethod(f.Channel, classExchange, methodExchangeDeleteOk, []byte{}); err != nil {
				logger.Error().Err(err).Msg("[server] write exchange.delete-ok error")
				return err
			}
		}
		return nil
	}

	// queue.declare (delegate)
	if classID == classQueue && methodID == methodQueueDeclare {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		qname := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				qname = string(args[idx+1 : idx+1+l])
			}
		}
		if handlers != nil && handlers.OnQueueDeclare != nil {
			if err := handlers.OnQueueDeclare(ctx, f.Channel, qname, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "queue.declare failed", classQueue, methodQueueDeclare); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
		}
		// queue.declare-ok: queue (shortstr), message-count (long), consumer-count (long)
		var dq bytes.Buffer
		dq.Write(encodeShortStr(qname))
		dq.Write(encodeLong(0))
		dq.Write(encodeLong(0))
		if err := ctx.WriteMethod(f.Channel, classQueue, methodQueueDeclareOk, dq.Bytes()); err != nil {
			logger.Error().Err(err).Msg("[server] write queue.declare-ok error")
			return err
		}
		return nil
	}

	// queue.bind (delegate)
	if classID == classQueue && methodID == methodQueueBind {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		qname := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				qname = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		exch := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				exch = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		rkey := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				rkey = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		if handlers != nil && handlers.OnQueueBind != nil {
			if err := handlers.OnQueueBind(ctx, f.Channel, qname, exch, rkey, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "queue.bind failed", classQueue, methodQueueBind); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
		}
		if err := ctx.WriteMethod(f.Channel, classQueue, methodQueueBindOk, []byte{}); err != nil {
			logger.Error().Err(err).Msg("[server] write queue.bind-ok error")
			return err
		}
		return nil
	}

	// queue.unbind (delegate)
	if classID == classQueue && methodID == methodQueueUnbind {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		qname := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				qname = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		exch := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				exch = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		rkey := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				rkey = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		// arguments (remaining bytes) passed to handler
		if handlers != nil && handlers.OnQueueUnbind != nil {
			if err := handlers.OnQueueUnbind(ctx, f.Channel, qname, exch, rkey, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "queue.unbind failed", classQueue, methodQueueUnbind); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
		}
		if err := ctx.WriteMethod(f.Channel, classQueue, methodQueueUnbindOk, []byte{}); err != nil {
			logger.Error().Err(err).Msg("[server] write queue.unbind-ok error")
			return err
		}
		return nil
	}

	// queue.purge (delegate)
	if classID == classQueue && methodID == methodQueuePurge {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		qname := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				qname = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		var msgCount int
		if handlers != nil && handlers.OnQueuePurge != nil {
			if c, err := handlers.OnQueuePurge(ctx, f.Channel, qname, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "queue.purge failed", classQueue, methodQueuePurge); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			} else {
				msgCount = c
			}
		}
		// queue.purge-ok: message-count (long)
		if err := ctx.WriteMethod(f.Channel, classQueue, methodQueuePurgeOk, encodeLong(uint32(msgCount))); err != nil {
			logger.Error().Err(err).Msg("[server] write queue.purge-ok error")
			return err
		}
		return nil
	}

	// queue.delete (delegate)
	if classID == classQueue && methodID == methodQueueDelete {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		qname := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				qname = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		// parse flags: if-unused (bit1), if-empty (bit2), nowait (bit4)
		var ifUnused bool
		var ifEmpty bool
		var nowait bool
		if idx < len(args) {
			flags := args[idx]
			if flags&1 == 1 {
				ifUnused = true
			}
			if flags&2 == 2 {
				ifEmpty = true
			}
			if flags&4 == 4 {
				nowait = true
			}
		}
		var delCount int
		if handlers != nil && handlers.OnQueueDelete != nil {
			c, err := handlers.OnQueueDelete(ctx, f.Channel, qname, ifUnused, ifEmpty, nowait, args)
			if err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "queue.delete failed", classQueue, methodQueueDelete); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
			delCount = c
		}
		// queue.delete-ok: message-count (long), unless nowait
		if !nowait {
			if err := ctx.WriteMethod(f.Channel, classQueue, methodQueueDeleteOk, encodeLong(uint32(delCount))); err != nil {
				logger.Error().Err(err).Msg("[server] write queue.delete-ok error")
				return err
			}
		}
		return nil
	}

	// basic.consume
	if classID == classBasic && methodID == methodBasicConsume {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		qname := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				qname = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		consumerTag := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				consumerTag = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		var flags byte
		if idx < len(args) {
			flags = args[idx]
			idx++
		}
		// delegate to handler if provided
		serverTag := consumerTag
		if handlers != nil && handlers.OnBasicConsume != nil {
			if st, err := handlers.OnBasicConsume(ctx, f.Channel, qname, consumerTag, flags, args); err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "basic.consume failed", classBasic, methodBasicConsume); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			} else if st != "" {
				serverTag = st
			}
		}
		if serverTag == "" {
			serverTag = fmt.Sprintf("ctag-%d", time.Now().UnixNano())
		}
		if err := ctx.WriteMethod(f.Channel, classBasic, methodBasicConsumeOk, encodeShortStr(serverTag)); err != nil {
			logger.Error().Err(err).Msg("[server] write basic.consume-ok error")
			return err
		}
		return nil
	}

	// confirm.select
	if classID == classConfirm && methodID == methodConfirmSelect {
		// Put channel into confirm mode. We ignore args (nowait) for simplicity.
		st, ok := channelStates[f.Channel]
		if !ok {
			st = &channelState{confirming: false, publishSeq: 0}
			channelStates[f.Channel] = st
		}
		st.confirming = true
		st.publishSeq = 0
		// respond with select-ok
		if err := ctx.WriteMethod(f.Channel, classConfirm, methodConfirmSelectOk, []byte{}); err != nil {
			logger.Error().Err(err).Msg("[server] write confirm.select-ok error")
			return err
		}
		logger.Debug().Uint16("chan", f.Channel).Int("class", int(classConfirm)).Int("method", int(methodConfirmSelectOk)).Msg("confirm.select-ok")
		return nil
	}

	// basic.get (delegate)
	if classID == classBasic && methodID == methodBasicGet {
		idx := 0
		if len(args) >= 2 {
			idx = 2
		}
		qname := ""
		if idx < len(args) {
			l := int(args[idx])
			if idx+1+l <= len(args) {
				qname = string(args[idx+1 : idx+1+l])
				idx = idx + 1 + l
			}
		}
		// delegate to handler
		if handlers != nil && handlers.OnBasicGet != nil {
			found, delTag, msg, err := handlers.OnBasicGet(ctx, f.Channel, qname, false)
			if err != nil {
				if werr := writeConnCloseAndWait(ctx, conn, 504, "basic.get failed", classBasic, methodBasicGet); werr != nil {
					logger.Error().Err(werr).Msg("[server] write connection close error")
				}
				return err
			}
			if !found {
				if err := ctx.WriteMethod(f.Channel, classBasic, methodBasicGetEmpty, []byte{}); err != nil {
					logger.Error().Err(err).Msg("[server] write get-empty error")
					return err
				}
				return nil
			}
			// send get-ok
			var payload bytes.Buffer
			payload.Write(encodeLongLong(delTag))
			payload.WriteByte(0)
			payload.Write(encodeShortStr(""))
			payload.Write(encodeShortStr(""))
			payload.Write(encodeLong(0))
			if err := ctx.WriteMethod(f.Channel, classBasic, methodBasicGetOk, payload.Bytes()); err != nil {
				logger.Error().Err(err).Msg("[server] write get-ok error")
				return err
			}
			if err := ctx.WriteFrame(Frame{Type: frameHeader, Channel: f.Channel, Payload: buildContentHeaderPayload(classBasic, uint64(len(msg)))}); err != nil {
				logger.Error().Err(err).Msg("[server] write header error")
				return err
			}
			if err := ctx.WriteFrame(Frame{Type: frameBody, Channel: f.Channel, Payload: msg}); err != nil {
				logger.Error().Err(err).Msg("[server] write body error")
				return err
			}
			return nil
		}

		// channel.flow (flow control)
	}

	return nil
}
