package upstream

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	amqp "github.com/ericogr/amqp-test/pkg/amqp"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

type upstreamSession struct {
	clientConn   net.Conn
	clientCtx    amqp.ConnContext
	upstreamConn *amqp091.Connection
	mu           sync.Mutex
	channels     map[uint16]*upstreamChannel
	// optional in-memory enqueue for failure policy (not fully implemented)
	enqueueMu sync.Mutex
	enqueued  []enqueuedMsg
	// last used upstream credentials (for reconnect)
	upUser string
	upPass string
	cfg    UpstreamConfig
	// closed indicates the client connection has been closed and the
	// session should not attempt reconnects.
	closed bool
	// logger copied from adapter for session-level logs
	logger zerolog.Logger
	// connection in-progress singleflight
	connecting    bool
	connectWaitCh chan struct{}
	connectErr    error
}

type upstreamChannel struct {
	upstreamCh *amqp091.Channel
	mu         sync.Mutex
	// delivery tag mappings client->upstream and upstream->client
	clientToUpstream map[uint64]uint64
	upstreamToClient map[uint64]uint64
	nextClientTag    uint64
	confirming       bool
	// active consumer subscriptions keyed by upstream consumer-tag
	consumers map[string]*consumerSubscription
	// logger copied from session for convenience
	logger zerolog.Logger
	// When true the monitor should not attempt to reconnect the upstream
	// automatically (used when we intentionally closed the upstream
	// connection because there are no active client channels).
	suppressReconnect bool
}

type enqueuedMsg struct {
	channel  uint16
	exchange string
	rkey     string
	pub      amqp091.Publishing
	when     time.Time
}

func (a *UpstreamAdapter) getOrCreateSession(ctx amqp.ConnContext) *upstreamSession {
	conn := ctx.Conn
	a.mu.Lock()
	defer a.mu.Unlock()
	if s, ok := a.sessions[conn]; ok {
		// update the stored conn context so we have the latest write helpers
		s.clientCtx = ctx
		return s
	}
	s := &upstreamSession{clientConn: conn, clientCtx: ctx, channels: map[uint16]*upstreamChannel{}, logger: a.logger}
	a.sessions[conn] = s
	return s
}

// consumerSubscription stores information needed to recreate a consumer on reconnect
type consumerSubscription struct {
	queue   string
	upTag   string
	channel uint16
	ctx     amqp.ConnContext
}

// addConsumer registers a subscription and starts it if an upstream channel
// is available.
func (c *upstreamChannel) addConsumer(sub *consumerSubscription) error {
	c.mu.Lock()
	if c.consumers == nil {
		c.consumers = map[string]*consumerSubscription{}
	}
	c.consumers[sub.upTag] = sub
	upch := c.upstreamCh
	c.mu.Unlock()
	if upch != nil {
		return c.startConsumer(sub)
	}
	return nil
}

// startConsumer starts delivering messages from upstream for a subscription.
func (c *upstreamChannel) startConsumer(sub *consumerSubscription) error {
	if c.upstreamCh == nil {
		return fmt.Errorf("no upstream channel")
	}
	deliveries, err := c.upstreamCh.Consume(sub.queue, sub.upTag, false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range deliveries {
			c.mu.Lock()
			clientTag := c.nextClientTag
			c.nextClientTag++
			c.clientToUpstream[clientTag] = d.DeliveryTag
			c.upstreamToClient[d.DeliveryTag] = clientTag
			c.mu.Unlock()

			var dar bytes.Buffer
			dar.Write(amqp.EncodeShortStr(sub.upTag))
			dar.Write(amqp.EncodeLongLong(clientTag))
			dar.WriteByte(0)
			dar.Write(amqp.EncodeShortStr(d.Exchange))
			dar.Write(amqp.EncodeShortStr(d.RoutingKey))

			if err := sub.ctx.WriteMethod(sub.channel, amqp.ClassBasic, amqp.MethodBasicDeliver, dar.Bytes()); err != nil {
				// client likely gone — remove consumer and cancel upstream
				c.mu.Lock()
				delete(c.consumers, sub.upTag)
				c.mu.Unlock()
				if c.upstreamCh != nil {
					if cerr := c.upstreamCh.Cancel(sub.upTag, false); cerr != nil {
						c.logger.Error().Err(cerr).Str("up_tag", sub.upTag).Msg("failed to cancel upstream consumer")
					}
				}
				c.logger.Error().Err(err).Str("up_tag", sub.upTag).Msg("failed to write deliver method to client; stopping consumer")
				return
			}

			if err := sub.ctx.WriteFrame(amqp.Frame{Type: amqp.FrameHeader, Channel: sub.channel, Payload: amqp.BuildContentHeaderPayload(amqp.ClassBasic, uint64(len(d.Body)))}); err != nil {
				c.mu.Lock()
				delete(c.consumers, sub.upTag)
				c.mu.Unlock()
				if c.upstreamCh != nil {
					if cerr := c.upstreamCh.Cancel(sub.upTag, false); cerr != nil {
						c.logger.Error().Err(cerr).Str("up_tag", sub.upTag).Msg("failed to cancel upstream consumer")
					}
				}
				c.logger.Error().Err(err).Str("up_tag", sub.upTag).Msg("failed to write content header to client; stopping consumer")
				return
			}

			if err := sub.ctx.WriteFrame(amqp.Frame{Type: amqp.FrameBody, Channel: sub.channel, Payload: d.Body}); err != nil {
				c.mu.Lock()
				delete(c.consumers, sub.upTag)
				c.mu.Unlock()
				if c.upstreamCh != nil {
					if cerr := c.upstreamCh.Cancel(sub.upTag, false); cerr != nil {
						c.logger.Error().Err(cerr).Str("up_tag", sub.upTag).Msg("failed to cancel upstream consumer")
					}
				}
				c.logger.Error().Err(err).Str("up_tag", sub.upTag).Msg("failed to write body frame to client; stopping consumer")
				return
			}
		}
	}()
	return nil
}

// restoreConsumers starts all registered consumers when an upstream channel
// has been (re)created.
func (c *upstreamChannel) restoreConsumers() {
	c.mu.Lock()
	subs := make([]*consumerSubscription, 0, len(c.consumers))
	for _, s := range c.consumers {
		subs = append(subs, s)
	}
	c.mu.Unlock()
	for _, s := range subs {
		if err := c.startConsumer(s); err != nil {
			c.logger.Error().Err(err).Str("up_tag", s.upTag).Msg("failed to start consumer during restore")
		}
	}
}

// buildDialURL injects credentials into a configured upstream URL.
func buildDialURL(rawURL, user, pass string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if user != "" {
		u.User = url.UserPassword(user, pass)
	}
	return u.String(), nil
}

func (s *upstreamSession) connectUpstream(cfg UpstreamConfig, user, pass string) error {
	// store cfg and credentials for reconnect
	s.mu.Lock()
	s.cfg = cfg
	s.upUser = user
	s.upPass = pass
	// if already connected, nothing to do
	if s.upstreamConn != nil {
		s.mu.Unlock()
		return nil
	}
	// singleflight: if another goroutine is connecting, wait for it
	if s.connecting {
		ch := s.connectWaitCh
		s.mu.Unlock()
		<-ch
		s.mu.Lock()
		err := s.connectErr
		s.mu.Unlock()
		return err
	}
	// mark connecting and create wait channel
	s.connecting = true
	s.connectWaitCh = make(chan struct{})
	s.connectErr = nil
	s.mu.Unlock()

	dialURL, err := buildDialURL(cfg.URL, user, pass)
	if err != nil {
		s.mu.Lock()
		s.connectErr = err
		close(s.connectWaitCh)
		s.connecting = false
		s.connectWaitCh = nil
		s.mu.Unlock()
		return err
	}

	var conn *amqp091.Connection
	if cfg.TLS {
		tlsCfg := cfg.TLSConfig
		if tlsCfg == nil {
			tlsCfg = &tls.Config{InsecureSkipVerify: true}
		}
		conn, err = amqp091.DialTLS(dialURL, tlsCfg)
	} else {
		conn, err = amqp091.Dial(dialURL)
	}

	s.mu.Lock()
	// if session was closed while dialing, discard connection
	if s.closed {
		if conn != nil {
			_ = conn.Close()
		}
		s.connectErr = fmt.Errorf("session closed")
		close(s.connectWaitCh)
		s.connecting = false
		s.connectWaitCh = nil
		s.mu.Unlock()
		return s.connectErr
	}
	if err != nil {
		s.connectErr = err
		close(s.connectWaitCh)
		s.connecting = false
		s.connectWaitCh = nil
		s.mu.Unlock()
		return err
	}

	s.upstreamConn = conn
	// clear connect state and notify waiters
	s.connectErr = nil
	close(s.connectWaitCh)
	s.connecting = false
	s.connectWaitCh = nil
	s.mu.Unlock()

	// start monitor goroutine for reconnects
	go s.monitorUpstream()

	// create channels for existing client channels
	s.mu.Lock()
	for clientChan := range s.channels {
		// allocate new upstream channel
		upch, err := s.upstreamConn.Channel()
		if err != nil {
			// continue, will be retried on publish
			continue
		}
		s.channels[clientChan].upstreamCh = upch
		// restore any consumers that were previously registered on this channel
		s.channels[clientChan].restoreConsumers()
	}
	// drain any enqueued messages
	go s.drainEnqueued()
	s.mu.Unlock()
	return nil
}

func (s *upstreamSession) monitorUpstream() {
	if s.upstreamConn == nil {
		return
	}
	closeCh := make(chan *amqp091.Error)
	s.upstreamConn.NotifyClose(closeCh)
	errInfo := <-closeCh
	// upstream closed
	s.logger.Info().Err(errInfo).Msg("upstream connection closed")
	s.mu.Lock()
	s.upstreamConn = nil
	// mark channels upstreamCh nil
	for _, c := range s.channels {
		c.upstreamCh = nil
	}
	cfg := s.cfg
	user := s.upUser
	pass := s.upPass
	client := s.clientConn
	closed := s.closed
	s.mu.Unlock()

	// if the client connection is closed/marked, do not attempt reconnect
	if client == nil || closed {
		return
	}

	// policy handling
	switch cfg.FailurePolicy {
	case FailCloseClient:
		// notify client with an AMQP Connection.Close instead of abruptly
		// closing the TCP socket. This lets the client receive an AMQP
		// exception (Connection.Close) and respond with Close-Ok.
		if s.clientCtx.Conn != nil && s.clientCtx.WriteMethod != nil {
			if err := amqp.SendConnectionClose(s.clientCtx, 320, fmt.Sprintf("upstream connection closed: %v", errInfo), 0, 0); err != nil {
				s.logger.Error().Err(err).Msg("failed to send connection.close to client")
			}
			// do not close the client socket here; allow the connection handler
			// goroutine to observe the Close-Ok and shut down gracefully.
		} else {
			// fallback: close client connection immediately
			if client != nil {
				if err := client.Close(); err != nil {
					s.logger.Error().Err(err).Msg("failed to close client after upstream connection closed")
				}
			}
		}
		return
	case FailReconnect:
		// attempt reconnect loop
		for {
			dialURL, err := buildDialURL(cfg.URL, user, pass)
			if err != nil {
				time.Sleep(cfg.ReconnectDelay)
				continue
			}
			var conn *amqp091.Connection
			if cfg.TLS {
				tlsCfg := cfg.TLSConfig
				if tlsCfg == nil {
					tlsCfg = &tls.Config{InsecureSkipVerify: true}
				}
				conn, err = amqp091.DialTLS(dialURL, tlsCfg)
			} else {
				conn, err = amqp091.Dial(dialURL)
			}
			if err != nil {
				s.logger.Warn().Err(err).Msg("failed to reconnect to upstream, will retry")
				time.Sleep(cfg.ReconnectDelay)
				continue
			}
			// success
			s.logger.Info().Msg("reconnected to upstream")
			s.mu.Lock()
			s.upstreamConn = conn
			// recreate channels
			for clientChan := range s.channels {
				upch, err := s.upstreamConn.Channel()
				if err == nil {
					s.channels[clientChan].upstreamCh = upch
					s.channels[clientChan].restoreConsumers()
				}
			}
			s.mu.Unlock()
			go s.drainEnqueued()
			// restart monitor
			go s.monitorUpstream()
			return
		}
	case FailEnqueue:
		// simply leave messages enqueued and run reconnect attempts in background
		go func() {
			for {
				dialURL, err := buildDialURL(cfg.URL, user, pass)
				if err != nil {
					time.Sleep(cfg.ReconnectDelay)
					continue
				}
				var conn *amqp091.Connection
				if cfg.TLS {
					tlsCfg := cfg.TLSConfig
					if tlsCfg == nil {
						tlsCfg = &tls.Config{InsecureSkipVerify: true}
					}
					conn, err = amqp091.DialTLS(dialURL, tlsCfg)
				} else {
					conn, err = amqp091.Dial(dialURL)
				}
				if err != nil {
					s.logger.Warn().Err(err).Msg("failed to reconnect to upstream, will retry")
					time.Sleep(cfg.ReconnectDelay)
					continue
				}
				// success
				s.logger.Info().Msg("reconnected to upstream")
				s.mu.Lock()
				s.upstreamConn = conn
				for clientChan := range s.channels {
					upch, err := s.upstreamConn.Channel()
					if err == nil {
						s.channels[clientChan].upstreamCh = upch
						s.channels[clientChan].restoreConsumers()
					}
				}
				s.mu.Unlock()
				go s.drainEnqueued()
				go s.monitorUpstream()
				return
			}
		}()
		return
	}
}

func (s *upstreamSession) enqueuePublish(channel uint16, exchange, rkey string, pub amqp091.Publishing) error {
	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	s.enqueued = append(s.enqueued, enqueuedMsg{channel: channel, exchange: exchange, rkey: rkey, pub: pub, when: time.Now()})
	return nil
}

func (s *upstreamSession) drainEnqueued() {
	s.enqueueMu.Lock()
	queue := s.enqueued
	s.enqueued = nil
	s.enqueueMu.Unlock()
	for _, em := range queue {
		s.mu.Lock()
		chm, ok := s.channels[em.channel]
		upConn := s.upstreamConn
		s.mu.Unlock()
		if !ok || upConn == nil || chm == nil {
			// re-enqueue
			s.enqueueMu.Lock()
			s.enqueued = append(s.enqueued, em)
			s.enqueueMu.Unlock()
			continue
		}
		// publish and handle errors: on failure re-enqueue and log
		if err := chm.upstreamCh.PublishWithContext(context.Background(), em.exchange, em.rkey, false, false, em.pub); err != nil {
			// re-enqueue for future retry
			s.enqueueMu.Lock()
			s.enqueued = append(s.enqueued, em)
			s.enqueueMu.Unlock()
			s.logger.Error().Err(err).Uint16("client_channel", em.channel).Str("exchange", em.exchange).Str("rkey", em.rkey).Msg("failed to publish enqueued message, re-enqueued")
		}
	}
}

func (s *upstreamSession) getOrCreateChannel(clientChan uint16) (*upstreamChannel, error) {
	// Fast-path: if channel exists return it.
	s.mu.Lock()
	if c, ok := s.channels[clientChan]; ok {
		s.mu.Unlock()
		return c, nil
	}
	// If there is no upstream connection try to connect synchronously using
	// stored credentials; unlock before dialing to avoid deadlock.
	if s.upstreamConn == nil {
		cfg := s.cfg
		user := s.upUser
		pass := s.upPass
		s.mu.Unlock()
		if err := s.connectUpstream(cfg, user, pass); err != nil {
			return nil, err
		}
		// re-acquire lock and check again
		s.mu.Lock()
		if c, ok := s.channels[clientChan]; ok {
			s.mu.Unlock()
			return c, nil
		}
	}
	// create an upstream channel
	if s.upstreamConn == nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("no upstream connection")
	}
	upch, err := s.upstreamConn.Channel()
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	c := &upstreamChannel{upstreamCh: upch, clientToUpstream: map[uint64]uint64{}, upstreamToClient: map[uint64]uint64{}, nextClientTag: 1, logger: s.logger}
	s.channels[clientChan] = c
	s.mu.Unlock()
	return c, nil
}
