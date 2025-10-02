package upstream

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	amqp "github.com/ericogr/amqp-test/pkg/amqp"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// FailurePolicy controls behavior when upstream (real broker) fails.
type FailurePolicy int

const (
	FailCloseClient FailurePolicy = iota // close client immediately
	FailReconnect                        // attempt reconnect
	FailEnqueue                          // enqueue in memory until upstream back
)

// UpstreamConfig configures the upstream broker connection and behavior.
type UpstreamConfig struct {
	URL            string
	TLS            bool
	DefaultUser    string
	DefaultPass    string
	TLSConfig      *tls.Config
	FailurePolicy  FailurePolicy
	ReconnectDelay time.Duration
}

// AuthHook allows custom authentication handling. See package docs for behavior.
type AuthHook func(ctx amqp.ConnContext, mechanism string, response []byte) (allow bool, useOriginalCreds bool, upstreamUser, upstreamPass string, err error)

// PublishHook allows intercepting publishes before they are forwarded upstream.
type PublishHook func(ctx amqp.ConnContext, ch uint16, exchange, rkey string, mandatory, immediate bool, props amqp.BasicProperties, body []byte) (handled bool, routed bool, nack bool, err error)

// UpstreamAdapter forwards SDK server operations to a real AMQP broker (RabbitMQ)
// unless hooks are provided that intercept specific operations. It implements
// a set of handlers compatible with `amqp.ServerHandlers` that can be passed to
// `amqp.ServeWithAuth`.
type UpstreamAdapter struct {
	cfg         UpstreamConfig
	AuthHook    AuthHook
	PublishHook PublishHook

	mu       sync.Mutex
	sessions map[net.Conn]*upstreamSession
}

// NewUpstreamAdapter creates a new adapter configured to talk to the provided
// upstream URL. The URL should include host:port and may omit credentials; the
// adapter will use credentials provided by the client or the configured defaults.
func NewUpstreamAdapter(cfg UpstreamConfig) *UpstreamAdapter {
	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = 5 * time.Second
	}
	return &UpstreamAdapter{cfg: cfg, sessions: map[net.Conn]*upstreamSession{}}
}

// Handlers returns a `ServerHandlers` instance that delegates operations to
// this adapter. You can pass this to `amqp.ServeWithAuth` or use the adapter's
// AuthHandler directly for authentication.
func (a *UpstreamAdapter) Handlers() *amqp.ServerHandlers {
	return &amqp.ServerHandlers{
		OnExchangeDeclare: a.OnExchangeDeclare,
		OnExchangeDelete:  a.OnExchangeDelete,
		OnExchangeBind:    a.OnExchangeBind,
		OnExchangeUnbind:  a.OnExchangeUnbind,
		OnQueueDeclare:    a.OnQueueDeclare,
		OnQueueDelete:     a.OnQueueDelete,
		OnQueuePurge:      a.OnQueuePurge,
		OnQueueBind:       a.OnQueueBind,
		OnQueueUnbind:     a.OnQueueUnbind,
		OnBasicConsume:    a.OnBasicConsume,
		OnBasicPublish:    a.OnBasicPublish,
		OnBasicGet:        a.OnBasicGet,
		OnBasicNack:       a.OnBasicNack,
		OnBasicReject:     a.OnBasicReject,
		OnBasicAck:        a.OnBasicAck,
		OnBasicQos:        a.OnBasicQos,
		OnBasicReturn:     a.OnBasicReturn,
		OnChannelFlow:     a.OnChannelFlow,
	}
}

// AuthHandler implements amqp.AuthHandler. It is intended to be passed to
// `amqp.ServeWithAuth`. The adapter will, by default, accept credentials and
// open an upstream connection for the client using those credentials (or the
// configured defaults). The AuthHook can intercept and alter behavior.
func (a *UpstreamAdapter) AuthHandler(ctx amqp.ConnContext, mechanism string, response []byte) error {
	// default behavior
	useOriginal := true
	user := a.cfg.DefaultUser
	pass := a.cfg.DefaultPass

	if a.AuthHook != nil {
		ok, useOrig, upUser, upPass, err := a.AuthHook(ctx, mechanism, response)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("access refused")
		}
		useOriginal = useOrig
		if upUser != "" {
			user = upUser
			pass = upPass
		}
	}

	// if using original credentials and mechanism is PLAIN, try to extract
	// username/password from response. Otherwise keep defaults or hook-provided.
	if useOriginal && mechanism == "PLAIN" {
		// response is authzid\x00authcid\x00password
		parts := bytes.SplitN(response, []byte{0}, 3)
		if len(parts) == 3 {
			user = string(parts[1])
			pass = string(parts[2])
		} else if len(parts) == 2 {
			user = string(parts[0])
			pass = string(parts[1])
		}
	}

	// create session and connect upstream using chosen credentials
	s := a.getOrCreateSession(ctx.Conn)
	if err := s.connectUpstream(a.cfg, user, pass); err != nil {
		return fmt.Errorf("upstream connection failed: %w", err)
	}
	return nil
}

// OnBasicPublish forwards the publish to the upstream broker unless a PublishHook
// handled it.
func (a *UpstreamAdapter) OnBasicPublish(ctx amqp.ConnContext, channel uint16, exchange, rkey string, mandatory bool, immediate bool, properties amqp.BasicProperties, body []byte) (bool, bool, error) {
	// hook first
	if a.PublishHook != nil {
		handled, routed, nack, err := a.PublishHook(ctx, channel, exchange, rkey, mandatory, immediate, properties, body)
		if handled {
			return routed, nack, err
		}
	}

	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		// no upstream channel available
		switch s.cfg.FailurePolicy {
		case FailEnqueue:
			// enqueue and report accepted
			pub := amqp091.Publishing{ContentType: properties.ContentType, Body: body, Headers: amqp091.Table(properties.Headers)}
			_ = s.enqueuePublish(channel, exchange, rkey, pub)
			return true, false, nil
		case FailReconnect:
			// try to reconnect synchronously once
			_ = s.connectUpstream(s.cfg, s.upUser, s.upPass)
			ch, err = s.getOrCreateChannel(channel)
			if err != nil {
				return false, false, err
			}
		default:
			return false, false, err
		}
	}
	// map properties to amqp091.Publishing
	pub := amqp091.Publishing{
		ContentType: properties.ContentType,
		Body:        body,
		Headers:     amqp091.Table(properties.Headers),
	}
	ch.mu.Lock()
	if !ch.confirming {
		// enable confirm mode upstream for this channel
		if err := ch.upstreamCh.Confirm(false); err == nil {
			ch.confirming = true
		}
	}
	ch.mu.Unlock()

	var confCh chan amqp091.Confirmation
	if ch.confirming {
		confCh = make(chan amqp091.Confirmation, 1)
		ch.upstreamCh.NotifyPublish(confCh)
	}

	if ch.upstreamCh == nil {
		// upstream channel lost - handle according to policy
		switch s.cfg.FailurePolicy {
		case FailEnqueue:
			_ = s.enqueuePublish(channel, exchange, rkey, pub)
			return true, false, nil
		case FailReconnect:
			if err := s.connectUpstream(s.cfg, s.upUser, s.upPass); err != nil {
				return false, false, err
			}
			// recreate channel
			ch, err = s.getOrCreateChannel(channel)
			if err != nil {
				return false, false, err
			}
		default:
			return false, false, fmt.Errorf("no upstream channel")
		}
	}

	if err := ch.upstreamCh.Publish(exchange, rkey, mandatory, immediate, pub); err != nil {
		// on publish error apply failure policy
		switch s.cfg.FailurePolicy {
		case FailEnqueue:
			_ = s.enqueuePublish(channel, exchange, rkey, pub)
			return true, false, nil
		case FailReconnect:
			if cerr := s.connectUpstream(s.cfg, s.upUser, s.upPass); cerr == nil {
				ch, err = s.getOrCreateChannel(channel)
				if err == nil {
					// retry once
					if perr := ch.upstreamCh.Publish(exchange, rkey, mandatory, immediate, pub); perr == nil {
						// proceed to confirm handling below
					} else {
						return false, false, perr
					}
				}
			}
			return false, false, err
		default:
			return false, false, err
		}
	}

	if ch.confirming && confCh != nil {
		// wait for confirmation (bounded time)
		select {
		case c := <-confCh:
			if c.Ack {
				return true, false, nil
			}
			return false, true, nil
		case <-time.After(5 * time.Second):
			return false, false, fmt.Errorf("upstream confirm timeout")
		}
	}
	// no confirm mode upstream — assume routed
	return true, false, nil
}

// OnBasicConsume creates an upstream consumer and forwards messages back to the client.
func (a *UpstreamAdapter) OnBasicConsume(ctx amqp.ConnContext, channel uint16, queue, consumerTag string, flags byte, args []byte) (string, error) {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return "", err
	}
	// default: noAck = false, exclusive=false, noWait=false
	upTag := consumerTag
	if upTag == "" {
		upTag = fmt.Sprintf("up-%d", time.Now().UnixNano())
	}
	deliveries, err := ch.upstreamCh.Consume(queue, upTag, false, false, false, false, nil)
	if err != nil {
		return "", err
	}
	// forward deliveries
	go func() {
		for d := range deliveries {
			// assign client delivery-tag
			ch.mu.Lock()
			clientTag := ch.nextClientTag
			ch.nextClientTag++
			ch.clientToUpstream[clientTag] = d.DeliveryTag
			ch.upstreamToClient[d.DeliveryTag] = clientTag
			ch.mu.Unlock()
			// build and send basic.deliver method using exported helpers
			var dar bytes.Buffer
			dar.Write(amqp.EncodeShortStr(upTag))
			dar.Write(amqp.EncodeLongLong(clientTag))
			dar.WriteByte(0)
			dar.Write(amqp.EncodeShortStr(d.Exchange))
			dar.Write(amqp.EncodeShortStr(d.RoutingKey))
			_ = ctx.WriteMethod(channel, amqp.ClassBasic, amqp.MethodBasicDeliver, dar.Bytes())
			// header + body
			_ = ctx.WriteFrame(amqp.Frame{Type: amqp.FrameHeader, Channel: channel, Payload: amqp.BuildContentHeaderPayload(amqp.ClassBasic, uint64(len(d.Body)))})
			_ = ctx.WriteFrame(amqp.Frame{Type: amqp.FrameBody, Channel: channel, Payload: d.Body})
		}
	}()
	return upTag, nil
}

// OnBasicQos forwards QoS requests to upstream.
func (a *UpstreamAdapter) OnBasicQos(ctx amqp.ConnContext, channel uint16, prefetchSize uint32, prefetchCount uint16, global bool) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	return ch.upstreamCh.Qos(int(prefetchCount), int(prefetchSize), global)
}

// OnChannelFlow forwards flow control requests to upstream.
func (a *UpstreamAdapter) OnChannelFlow(ctx amqp.ConnContext, channel uint16, active bool) (bool, error) {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return false, err
	}
	err = ch.upstreamCh.Flow(active)
	return active, err
}

// OnBasicReturn just logs/forwards (adapter currently notifies application via handlers elsewhere)
func (a *UpstreamAdapter) OnBasicReturn(ctx amqp.ConnContext, channel uint16, replyCode uint16, replyText string, exchange, routingKey string, properties amqp.BasicProperties, body []byte) error {
	// default: no-op (applications can supply hooks)
	return nil
}

// OnBasicGet attempts to fetch a message from upstream.
func (a *UpstreamAdapter) OnBasicGet(ctx amqp.ConnContext, channel uint16, queue string, noAck bool) (bool, uint64, []byte, error) {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return false, 0, nil, err
	}
	msg, ok, err := ch.upstreamCh.Get(queue, noAck)
	if err != nil {
		return false, 0, nil, err
	}
	if !ok {
		return false, 0, nil, nil
	}
	// produce client delivery tag mapping
	ch.mu.Lock()
	clientTag := ch.nextClientTag
	ch.nextClientTag++
	ch.clientToUpstream[clientTag] = msg.DeliveryTag
	ch.upstreamToClient[msg.DeliveryTag] = clientTag
	ch.mu.Unlock()
	return true, clientTag, msg.Body, nil
}

// OnBasicAck forwards consumer acks to upstream (simple implementation).
func (a *UpstreamAdapter) OnBasicAck(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, multiple bool) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	// map client tag -> upstream tag
	ch.mu.Lock()
	upTag, ok := ch.clientToUpstream[deliveryTag]
	ch.mu.Unlock()
	if !ok {
		return nil
	}
	return ch.upstreamCh.Ack(upTag, multiple)
}

// OnBasicNack forwards consumer nacks to upstream.
func (a *UpstreamAdapter) OnBasicNack(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, multiple bool, requeue bool) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	ch.mu.Lock()
	upTag, ok := ch.clientToUpstream[deliveryTag]
	ch.mu.Unlock()
	if !ok {
		return nil
	}
	return ch.upstreamCh.Nack(upTag, multiple, requeue)
}

// OnBasicReject forwards basic.reject to upstream (similar to nack with single)
func (a *UpstreamAdapter) OnBasicReject(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, requeue bool) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	ch.mu.Lock()
	upTag, ok := ch.clientToUpstream[deliveryTag]
	ch.mu.Unlock()
	if !ok {
		return nil
	}
	return ch.upstreamCh.Reject(upTag, requeue)
}

// Exchange/Queue operations: basic forwarding to upstream using reasonable defaults.
func (a *UpstreamAdapter) OnExchangeDeclare(ctx amqp.ConnContext, channel uint16, exchange, kind string, args []byte) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	// defaults: durable=true, autoDelete=false, internal=false
	return ch.upstreamCh.ExchangeDeclare(exchange, kind, true, false, false, false, nil)
}

func (a *UpstreamAdapter) OnExchangeDelete(ctx amqp.ConnContext, channel uint16, exchange string, ifUnused bool, nowait bool, args []byte) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	return ch.upstreamCh.ExchangeDelete(exchange, ifUnused, nowait)
}

func (a *UpstreamAdapter) OnExchangeBind(ctx amqp.ConnContext, channel uint16, destination, source, routingKey string, nowait bool, args []byte) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	return ch.upstreamCh.ExchangeBind(destination, routingKey, source, nowait, nil)
}

func (a *UpstreamAdapter) OnExchangeUnbind(ctx amqp.ConnContext, channel uint16, destination, source, routingKey string, nowait bool, args []byte) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	return ch.upstreamCh.ExchangeUnbind(destination, routingKey, source, nowait, nil)
}

func (a *UpstreamAdapter) OnQueueDeclare(ctx amqp.ConnContext, channel uint16, queue string, args []byte) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	_, err = ch.upstreamCh.QueueDeclare(queue, true, false, false, false, nil)
	return err
}

func (a *UpstreamAdapter) OnQueueDelete(ctx amqp.ConnContext, channel uint16, queue string, ifUnused bool, ifEmpty bool, nowait bool, args []byte) (int, error) {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return 0, err
	}
	cnt, err := ch.upstreamCh.QueueDelete(queue, ifUnused, ifEmpty, nowait)
	return int(cnt), err
}

func (a *UpstreamAdapter) OnQueuePurge(ctx amqp.ConnContext, channel uint16, queue string, args []byte) (int, error) {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return 0, err
	}
	cnt, err := ch.upstreamCh.QueuePurge(queue, false)
	return int(cnt), err
}

func (a *UpstreamAdapter) OnQueueBind(ctx amqp.ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	return ch.upstreamCh.QueueBind(queue, rkey, exchange, false, nil)
}

func (a *UpstreamAdapter) OnQueueUnbind(ctx amqp.ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error {
	s := a.getOrCreateSession(ctx.Conn)
	ch, err := s.getOrCreateChannel(channel)
	if err != nil {
		return err
	}
	return ch.upstreamCh.QueueUnbind(queue, rkey, exchange, nil)
}
