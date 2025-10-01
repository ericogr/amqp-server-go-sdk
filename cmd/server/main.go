package main

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ericogr/amqp-test/pkg/amqp"
	"github.com/rs/zerolog"
)

// minimal encoders used by the default server
func encodeShortStr(s string) []byte {
	if len(s) > 255 {
		s = s[:255]
	}
	b := make([]byte, 1+len(s))
	b[0] = byte(len(s))
	copy(b[1:], []byte(s))
	return b
}

func encodeLongLong(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func buildContentHeaderPayload(classID uint16, bodySize uint64) []byte {
	var buf bytes.Buffer
	buf.Write(encodeShort(classID))
	buf.Write(encodeShort(0))
	buf.Write(encodeLongLong(bodySize))
	buf.Write(encodeShort(0))
	return buf.Bytes()
}

func encodeShort(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func main() {
	addr := flag.String("addr", ":5672", "listen address")
	flag.Parse()

	// configure logger and pass to SDK
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	amqp.SetLogger(logger)

	logger.Info().Str("addr", *addr).Msg("starting minimal AMQP server")

	// minimal in-memory broker for default server behavior
	type consumer struct {
		tag       string
		channel   uint16
		writeMeth func(channel uint16, classID, methodID uint16, args []byte) error
		writeFrm  func(f amqp.Frame) error
	}
	type queueState struct {
		name            string
		messages        [][]byte
		consumers       []*consumer
		nextDeliveryTag uint64
	}

	var mu sync.Mutex
	exchanges := map[string]struct{}{}
	queues := map[string]*queueState{}

	// simple default handlers using the in-memory broker
	handlers := &amqp.ServerHandlers{}

	handlers.OnExchangeDeclare = func(ctx amqp.ConnContext, channel uint16, exchange, kind string, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		exchanges[exchange] = struct{}{}
		logger.Info().Str("exchange", exchange).Str("type", kind).Msg("exchange declared")
		return nil
	}

	handlers.OnExchangeDelete = func(ctx amqp.ConnContext, channel uint16, exchange string, ifUnused bool, nowait bool, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		delete(exchanges, exchange)
		logger.Info().Str("exchange", exchange).Bool("ifUnused", ifUnused).Bool("nowait", nowait).Msg("exchange deleted")
		return nil
	}

	handlers.OnExchangeBind = func(ctx amqp.ConnContext, channel uint16, destination, source, routingKey string, nowait bool, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		// for demo we don't implement routing here; just log the bind
		logger.Info().Str("dest", destination).Str("source", source).Str("key", routingKey).Bool("nowait", nowait).Str("vhost", ctx.Vhost).Bool("tls", ctx.TLSState != nil).Msg("exchange bind")
		return nil
	}

	handlers.OnExchangeUnbind = func(ctx amqp.ConnContext, channel uint16, destination, source, routingKey string, nowait bool, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		logger.Info().Str("dest", destination).Str("source", source).Str("key", routingKey).Bool("nowait", nowait).Str("vhost", ctx.Vhost).Bool("tls", ctx.TLSState != nil).Msg("exchange unbind")
		return nil
	}

	handlers.OnQueueDeclare = func(ctx amqp.ConnContext, channel uint16, queue string, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := queues[queue]; !ok {
			queues[queue] = &queueState{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
		}
		logger.Info().Str("queue", queue).Msg("queue declared")
		return nil
	}

	handlers.OnQueueDelete = func(ctx amqp.ConnContext, channel uint16, queue string, ifUnused bool, ifEmpty bool, nowait bool, args []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		cnt := 0
		if q, ok := queues[queue]; ok {
			cnt = len(q.messages)
			delete(queues, queue)
		}
		logger.Info().Str("queue", queue).Int("count", cnt).Bool("ifUnused", ifUnused).Bool("ifEmpty", ifEmpty).Bool("nowait", nowait).Msg("queue deleted")
		return cnt, nil
	}

	handlers.OnQueuePurge = func(ctx amqp.ConnContext, channel uint16, queue string, args []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		if q, ok := queues[queue]; ok {
			cnt := len(q.messages)
			q.messages = make([][]byte, 0)
			logger.Info().Str("queue", queue).Int("count", cnt).Msg("queue purged")
			return cnt, nil
		}
		return 0, nil
	}

	// periodic generator: push a message to the demo queue every 2 seconds
	// only generate a message when there is at least one consumer for the queue
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for t := range ticker.C {
			mu.Lock()
			qname := "test-queue"
			q, ok := queues[qname]
			if ok && len(q.consumers) > 0 {
				msg := []byte(fmt.Sprintf("auto-msg-%d", t.UnixNano()))
				delivered := false
				// try to deliver to the first available consumer; remove dead consumers on write errors
				for i := 0; i < len(q.consumers); {
					c := q.consumers[i]
					// prepare delivery args with a tentative delivery tag
					delTag := q.nextDeliveryTag + 1
					var dar bytes.Buffer
					dar.Write(encodeShortStr(c.tag))
					dar.Write(encodeLongLong(delTag))
					dar.WriteByte(0)
					dar.Write(encodeShortStr(""))
					dar.Write(encodeShortStr(""))

					if err := c.writeMeth(c.channel, 60, 60, dar.Bytes()); err != nil {
						// remove dead consumer
						q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
						continue
					}
					if err := c.writeFrm(amqp.Frame{Type: 2, Channel: c.channel, Payload: buildContentHeaderPayload(60, uint64(len(msg)))}); err != nil {
						q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
						continue
					}
					if err := c.writeFrm(amqp.Frame{Type: 3, Channel: c.channel, Payload: msg}); err != nil {
						q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
						continue
					}
					// success
					q.nextDeliveryTag = delTag
					logger.Info().Str("queue", qname).Uint64("tag", delTag).Msg("auto-delivered msg")
					delivered = true
					break
				}
				if !delivered {
					// no live consumers found; do nothing
				}
			}
			mu.Unlock()
		}
	}()

	handlers.OnQueueBind = func(ctx amqp.ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		// ensure queue exists
		if _, ok := queues[queue]; !ok {
			queues[queue] = &queueState{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
		}
		// ensure exchange exists
		exchanges[exchange] = struct{}{}
		logger.Info().Str("queue", queue).Str("exchange", exchange).Str("rkey", rkey).Msg("bind")
		return nil
	}

	handlers.OnQueueUnbind = func(ctx amqp.ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		logger.Info().Str("queue", queue).Str("exchange", exchange).Str("rkey", rkey).Str("vhost", ctx.Vhost).Bool("tls", ctx.TLSState != nil).Msg("queue unbind")
		return nil
	}

	handlers.OnBasicConsume = func(ctx amqp.ConnContext, channel uint16, queue, consumerTag string, flags byte, args []byte) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := queues[queue]; !ok {
			queues[queue] = &queueState{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
		}
		if consumerTag == "" {
			consumerTag = fmt.Sprintf("ctag-%d", time.Now().UnixNano())
		}
		c := &consumer{tag: consumerTag, channel: channel, writeMeth: ctx.WriteMethod, writeFrm: ctx.WriteFrame}
		queues[queue].consumers = append(queues[queue].consumers, c)
		// deliver any queued messages to this new consumer
		if len(queues[queue].messages) > 0 {
			msg := queues[queue].messages[0]
			queues[queue].messages = queues[queue].messages[1:]
			queues[queue].nextDeliveryTag++
			delTag := queues[queue].nextDeliveryTag
			var dar bytes.Buffer
			dar.Write(encodeShortStr(consumerTag))
			dar.Write(encodeLongLong(delTag))
			dar.WriteByte(0)
			dar.Write(encodeShortStr(""))
			dar.Write(encodeShortStr(""))
			_ = c.writeMeth(c.channel, 60, 60, dar.Bytes())
			_ = c.writeFrm(amqp.Frame{Type: 2, Channel: c.channel, Payload: buildContentHeaderPayload(60, uint64(len(msg)))})
			_ = c.writeFrm(amqp.Frame{Type: 3, Channel: c.channel, Payload: msg})
		}
		return consumerTag, nil
	}

	handlers.OnBasicPublish = func(ctx amqp.ConnContext, channel uint16, exchange, rkey string, mandatory bool, immediate bool, properties amqp.BasicProperties, body []byte) (bool, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		// default routing: if exchange empty, deliver to queue named rkey
		if exchange == "" {
			if q, ok := queues[rkey]; ok {
				if len(q.consumers) > 0 {
					c := q.consumers[0]
					q.nextDeliveryTag++
					delTag := q.nextDeliveryTag
					var dar bytes.Buffer
					dar.Write(encodeShortStr(c.tag))
					dar.Write(encodeLongLong(delTag))
					dar.WriteByte(0)
					dar.Write(encodeShortStr(exchange))
					dar.Write(encodeShortStr(rkey))
					_ = c.writeMeth(c.channel, 60, 60, dar.Bytes())
					_ = c.writeFrm(amqp.Frame{Type: 2, Channel: c.channel, Payload: buildContentHeaderPayload(60, uint64(len(body)))})
					_ = c.writeFrm(amqp.Frame{Type: 3, Channel: c.channel, Payload: body})
					return true, false, nil
				}
				// enqueue
				q.messages = append(q.messages, append([]byte(nil), body...))
				return true, false, nil
			}
			// no such queue -> drop
			return false, false, nil
		}
		// for non-empty exchange default do nothing
		return false, false, nil
	}

	// optional: log client-side nacks/rejects
	handlers.OnBasicNack = func(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, multiple bool, requeue bool) error {
		logger.Info().Uint16("chan", channel).Uint64("tag", deliveryTag).Bool("multiple", multiple).Bool("requeue", requeue).Msg("client basic.nack")
		return nil
	}
	handlers.OnBasicReject = func(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, requeue bool) error {
		logger.Info().Uint16("chan", channel).Uint64("tag", deliveryTag).Bool("requeue", requeue).Msg("client basic.reject")
		return nil
	}

	handlers.OnBasicAck = func(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, multiple bool) error {
		logger.Info().Uint16("chan", channel).Uint64("tag", deliveryTag).Bool("multiple", multiple).Str("vhost", ctx.Vhost).Bool("tls", ctx.TLSState != nil).Msg("client basic.ack")
		return nil
	}

	handlers.OnBasicGet = func(ctx amqp.ConnContext, channel uint16, queue string, noAck bool) (bool, uint64, []byte, error) {
		mu.Lock()
		defer mu.Unlock()
		q, ok := queues[queue]
		if !ok || len(q.messages) == 0 {
			return false, 0, nil, nil
		}
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.nextDeliveryTag++
		delTag := q.nextDeliveryTag
		return true, delTag, msg, nil
	}

	// simple auth handler: accept PLAIN guest:guest
	// receives the connection context so it can inspect `Vhost` and TLS state
	auth := func(ctx amqp.ConnContext, mechanism string, response []byte) error {
		if mechanism != "PLAIN" {
			return fmt.Errorf("unsupported mechanism %q", mechanism)
		}
		// PLAIN response: authzid \x00 authcid \x00 password
		parts := bytes.SplitN(response, []byte{0}, 3)
		var username, password string
		if len(parts) == 3 {
			username = string(parts[1])
			password = string(parts[2])
		} else if len(parts) == 2 {
			username = string(parts[0])
			password = string(parts[1])
		} else {
			return fmt.Errorf("invalid PLAIN response")
		}
		if username != "guest" || password != "guest" {
			return fmt.Errorf("invalid credentials")
		}
		logger.Info().Str("user", username).Str("vhost", ctx.Vhost).Bool("tls", ctx.TLSState != nil).Msg("user authentication successful")

		return nil
	}

	// start plain TCP server
	go func() {
		if err := amqp.ServeWithAuth(*addr, nil, auth, handlers); err != nil {
			logger.Fatal().Err(err).Msg("server error")
		}
	}()

	// try to start TLS server on :5671 if certs are available
	certFile := "tls/server.pem"
	keyFile := "tls/server.key"
	if _, err := os.Stat(certFile); err == nil {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			logger.Error().Err(err).Msg("failed to load tls cert")
		} else {
			tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
			lnRaw, err := net.Listen("tcp", ":5671")
			if err != nil {
				logger.Error().Err(err).Msg("failed to listen on :5671")
			} else {
				tlsLn := tls.NewListener(lnRaw, tlsCfg)
				go func() {
					if err := amqp.ServeWithListener(tlsLn, nil, auth, handlers); err != nil {
						logger.Fatal().Err(err).Msg("tls server error")
					}
				}()
				logger.Info().Str("addr", ":5671").Msg("started TLS server")
			}
		}
	} else {
		logger.Info().Msg("tls certs not found, skipping TLS listener (see: Makefile gen-certs)")
	}

	// block forever
	select {}
}
