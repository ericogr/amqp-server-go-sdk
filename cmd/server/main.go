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

// encoding helpers
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

func encodeShort(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
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

// Broker holds minimal in-memory broker state used by the example server.
type Broker struct {
	mu        sync.Mutex
	exchanges map[string]struct{}
	queues    map[string]*Queue
	logger    zerolog.Logger
}

type Consumer struct {
	tag       string
	channel   uint16
	writeMeth func(channel uint16, classID, methodID uint16, args []byte) error
	writeFrm  func(f amqp.Frame) error
}

type Queue struct {
	name            string
	messages        [][]byte
	consumers       []*Consumer
	nextDeliveryTag uint64
}

func NewBroker(logger zerolog.Logger) *Broker {
	return &Broker{
		exchanges: make(map[string]struct{}),
		queues:    make(map[string]*Queue),
		logger:    logger,
	}
}

func (b *Broker) declareExchange(exchange, kind string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.exchanges[exchange] = struct{}{}
	b.logger.Info().Str("exchange", exchange).Str("type", kind).Msg("exchange declared")
}

func (b *Broker) deleteExchange(exchange string, ifUnused, nowait bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.exchanges, exchange)
	b.logger.Info().Str("exchange", exchange).Bool("ifUnused", ifUnused).Bool("nowait", nowait).Msg("exchange deleted")
}

func (b *Broker) bindExchange(destination, source, routingKey string, nowait bool, vhost string, tlsEnabled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logger.Info().Str("dest", destination).Str("source", source).Str("key", routingKey).Bool("nowait", nowait).Str("vhost", vhost).Bool("tls", tlsEnabled).Msg("exchange bind")
}

func (b *Broker) unbindExchange(destination, source, routingKey string, nowait bool, vhost string, tlsEnabled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logger.Info().Str("dest", destination).Str("source", source).Str("key", routingKey).Bool("nowait", nowait).Str("vhost", vhost).Bool("tls", tlsEnabled).Msg("exchange unbind")
}

func (b *Broker) declareQueue(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.queues[name]; !ok {
		b.queues[name] = &Queue{name: name, messages: make([][]byte, 0), consumers: make([]*Consumer, 0)}
	}
	b.logger.Info().Str("queue", name).Msg("queue declared")
}

func (b *Broker) deleteQueue(name string, ifUnused, ifEmpty, nowait bool) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	cnt := 0
	if q, ok := b.queues[name]; ok {
		cnt = len(q.messages)
		delete(b.queues, name)
	}
	b.logger.Info().Str("queue", name).Int("count", cnt).Bool("ifUnused", ifUnused).Bool("ifEmpty", ifEmpty).Bool("nowait", nowait).Msg("queue deleted")
	return cnt
}

func (b *Broker) purgeQueue(name string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if q, ok := b.queues[name]; ok {
		cnt := len(q.messages)
		q.messages = make([][]byte, 0)
		b.logger.Info().Str("queue", name).Int("count", cnt).Msg("queue purged")
		return cnt
	}
	return 0
}

func (b *Broker) bindQueue(queue, exchange, rkey string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.queues[queue]; !ok {
		b.queues[queue] = &Queue{name: queue, messages: make([][]byte, 0), consumers: make([]*Consumer, 0)}
	}
	b.exchanges[exchange] = struct{}{}
	b.logger.Info().Str("queue", queue).Str("exchange", exchange).Str("rkey", rkey).Msg("bind")
}

func (b *Broker) unbindQueue(queue, exchange, rkey, vhost string, tlsEnabled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logger.Info().Str("queue", queue).Str("exchange", exchange).Str("rkey", rkey).Str("vhost", vhost).Bool("tls", tlsEnabled).Msg("queue unbind")
}

func (b *Broker) addConsumer(queueName, consumerTag string, ch uint16, writeMeth func(channel uint16, classID, methodID uint16, args []byte) error, writeFrm func(amqp.Frame) error) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.queues[queueName]; !ok {
		b.queues[queueName] = &Queue{name: queueName, messages: make([][]byte, 0), consumers: make([]*Consumer, 0)}
	}
	if consumerTag == "" {
		consumerTag = fmt.Sprintf("ctag-%d", time.Now().UnixNano())
	}
	c := &Consumer{tag: consumerTag, channel: ch, writeMeth: writeMeth, writeFrm: writeFrm}
	q := b.queues[queueName]
	q.consumers = append(q.consumers, c)

	// deliver queued message to new consumer if available
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.nextDeliveryTag++
		delTag := q.nextDeliveryTag
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
	return consumerTag
}

func (b *Broker) publish(exchange, rkey string, body []byte) (bool, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// default routing: if exchange empty, deliver to queue named rkey
	if exchange == "" {
		if q, ok := b.queues[rkey]; ok {
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
				return true, false
			}
			q.messages = append(q.messages, append([]byte(nil), body...))
			return true, false
		}
		// no such queue -> drop
		return false, false
	}
	return false, false
}

func (b *Broker) logClientNack(channel uint16, deliveryTag uint64, multiple, requeue bool) {
	b.logger.Info().Uint16("chan", channel).Uint64("tag", deliveryTag).Bool("multiple", multiple).Bool("requeue", requeue).Msg("client basic.nack")
}

func (b *Broker) logClientReject(channel uint16, deliveryTag uint64, requeue bool) {
	b.logger.Info().Uint16("chan", channel).Uint64("tag", deliveryTag).Bool("requeue", requeue).Msg("client basic.reject")
}

func (b *Broker) logClientAck(channel uint16, deliveryTag uint64, multiple bool, vhost string, tlsEnabled bool) {
	b.logger.Info().Uint16("chan", channel).Uint64("tag", deliveryTag).Bool("multiple", multiple).Str("vhost", vhost).Bool("tls", tlsEnabled).Msg("client basic.ack")
}

func (b *Broker) getMessage(queue string) (bool, uint64, []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[queue]
	if !ok || len(q.messages) == 0 {
		return false, 0, nil
	}
	msg := q.messages[0]
	q.messages = q.messages[1:]
	q.nextDeliveryTag++
	delTag := q.nextDeliveryTag
	return true, delTag, msg
}

func (b *Broker) startAutoGenerator(interval time.Duration, qname string) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for t := range ticker.C {
			b.mu.Lock()
			q, ok := b.queues[qname]
			if ok && len(q.consumers) > 0 {
				msg := []byte(fmt.Sprintf("auto-msg-%d", t.UnixNano()))
				delivered := false
				for i := 0; i < len(q.consumers); {
					c := q.consumers[i]
					delTag := q.nextDeliveryTag + 1
					var dar bytes.Buffer
					dar.Write(encodeShortStr(c.tag))
					dar.Write(encodeLongLong(delTag))
					dar.WriteByte(0)
					dar.Write(encodeShortStr(""))
					dar.Write(encodeShortStr(""))
					if err := c.writeMeth(c.channel, 60, 60, dar.Bytes()); err != nil {
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
					q.nextDeliveryTag = delTag
					b.logger.Info().Str("queue", qname).Uint64("tag", delTag).Msg("auto-delivered msg")
					delivered = true
					break
				}
				if !delivered {
					// nothing
				}
			}
			b.mu.Unlock()
		}
	}()
}

func main() {
	addr := flag.String("addr", ":5672", "listen address")
	flag.Parse()

	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	amqp.SetLogger(logger)

	logger.Info().Str("addr", *addr).Msg("starting minimal AMQP server")

	broker := NewBroker(logger)

	// wire handlers to broker methods
	handlers := &amqp.ServerHandlers{
		OnExchangeDeclare: func(ctx amqp.ConnContext, channel uint16, exchange, kind string, args []byte) error {
			broker.declareExchange(exchange, kind)
			return nil
		},
		OnExchangeDelete: func(ctx amqp.ConnContext, channel uint16, exchange string, ifUnused bool, nowait bool, args []byte) error {
			broker.deleteExchange(exchange, ifUnused, nowait)
			return nil
		},
		OnExchangeBind: func(ctx amqp.ConnContext, channel uint16, destination, source, routingKey string, nowait bool, args []byte) error {
			broker.bindExchange(destination, source, routingKey, nowait, ctx.Vhost, ctx.TLSState != nil)
			return nil
		},
		OnExchangeUnbind: func(ctx amqp.ConnContext, channel uint16, destination, source, routingKey string, nowait bool, args []byte) error {
			broker.unbindExchange(destination, source, routingKey, nowait, ctx.Vhost, ctx.TLSState != nil)
			return nil
		},
		OnQueueDeclare: func(ctx amqp.ConnContext, channel uint16, queue string, args []byte) error {
			broker.declareQueue(queue)
			return nil
		},
		OnQueueDelete: func(ctx amqp.ConnContext, channel uint16, queue string, ifUnused bool, ifEmpty bool, nowait bool, args []byte) (int, error) {
			cnt := broker.deleteQueue(queue, ifUnused, ifEmpty, nowait)
			return cnt, nil
		},
		OnQueuePurge: func(ctx amqp.ConnContext, channel uint16, queue string, args []byte) (int, error) {
			cnt := broker.purgeQueue(queue)
			return cnt, nil
		},
		OnQueueBind: func(ctx amqp.ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error {
			broker.bindQueue(queue, exchange, rkey)
			return nil
		},
		OnQueueUnbind: func(ctx amqp.ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error {
			broker.unbindQueue(queue, exchange, rkey, ctx.Vhost, ctx.TLSState != nil)
			return nil
		},
		OnBasicConsume: func(ctx amqp.ConnContext, channel uint16, queue, consumerTag string, flags byte, args []byte) (string, error) {
			serverTag := broker.addConsumer(queue, consumerTag, channel, ctx.WriteMethod, ctx.WriteFrame)
			return serverTag, nil
		},
		OnBasicPublish: func(ctx amqp.ConnContext, channel uint16, exchange, rkey string, mandatory bool, immediate bool, properties amqp.BasicProperties, body []byte) (bool, bool, error) {
			routed, nack := broker.publish(exchange, rkey, body)
			return routed, nack, nil
		},
		OnBasicQos: func(ctx amqp.ConnContext, channel uint16, prefetchSize uint32, prefetchCount uint16, global bool) error {
			broker.logger.Info().Uint32("prefetch_size", prefetchSize).Uint16("prefetch_count", prefetchCount).Bool("global", global).Msg("basic.qos")
			return nil
		},
		OnChannelFlow: func(ctx amqp.ConnContext, channel uint16, active bool) (bool, error) {
			broker.logger.Info().Uint16("chan", channel).Bool("active", active).Msg("channel.flow")
			return active, nil
		},
		OnBasicReturn: func(ctx amqp.ConnContext, channel uint16, replyCode uint16, replyText string, exchange, routingKey string, properties amqp.BasicProperties, body []byte) error {
			broker.logger.Info().Uint16("chan", channel).Uint16("reply_code", replyCode).Str("reply_text", replyText).Str("exchange", exchange).Str("rkey", routingKey).Msg("basic.return")
			return nil
		},
		OnBasicNack: func(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, multiple bool, requeue bool) error {
			broker.logClientNack(channel, deliveryTag, multiple, requeue)
			return nil
		},
		OnBasicReject: func(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, requeue bool) error {
			broker.logClientReject(channel, deliveryTag, requeue)
			return nil
		},
		OnBasicAck: func(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, multiple bool) error {
			broker.logClientAck(channel, deliveryTag, multiple, ctx.Vhost, ctx.TLSState != nil)
			return nil
		},
		OnBasicGet: func(ctx amqp.ConnContext, channel uint16, queue string, noAck bool) (bool, uint64, []byte, error) {
			found, tag, body := broker.getMessage(queue)
			return found, tag, body, nil
		},
	}

	// simple auth handler: accept PLAIN guest:guest
	auth := func(ctx amqp.ConnContext, mechanism string, response []byte) error {
		if mechanism != "PLAIN" {
			return fmt.Errorf("unsupported mechanism %q", mechanism)
		}
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

	// start servers
	go func() {
		if err := amqp.ServeWithAuth(*addr, nil, auth, handlers); err != nil {
			logger.Fatal().Err(err).Msg("server error")
		}
	}()

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

	// start an auto message generator for demo queue
	broker.startAutoGenerator(2*time.Second, "test-queue")

	select {}
}
