package upstream

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

type upstreamSession struct {
	clientConn   net.Conn
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
}

type upstreamChannel struct {
	upstreamCh *amqp091.Channel
	mu         sync.Mutex
	// delivery tag mappings client->upstream and upstream->client
	clientToUpstream map[uint64]uint64
	upstreamToClient map[uint64]uint64
	nextClientTag    uint64
	confirming       bool
}

type enqueuedMsg struct {
	channel  uint16
	exchange string
	rkey     string
	pub      amqp091.Publishing
	when     time.Time
}

func (a *UpstreamAdapter) getOrCreateSession(conn net.Conn) *upstreamSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s, ok := a.sessions[conn]; ok {
		return s
	}
	s := &upstreamSession{clientConn: conn, channels: map[uint16]*upstreamChannel{}}
	a.sessions[conn] = s
	return s
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
	s.mu.Lock()
	// store cfg and credentials for reconnect
	s.cfg = cfg
	s.upUser = user
	s.upPass = pass
	s.mu.Unlock()

	dialURL, err := buildDialURL(cfg.URL, user, pass)
	if err != nil {
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
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.upstreamConn = conn
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
	<-closeCh
	// upstream closed
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
		// close client connection
		_ = client.Close()
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
			if err == nil {
				s.mu.Lock()
				s.upstreamConn = conn
				// recreate channels
				for clientChan := range s.channels {
					upch, err := s.upstreamConn.Channel()
					if err == nil {
						s.channels[clientChan].upstreamCh = upch
					}
				}
				s.mu.Unlock()
				go s.drainEnqueued()
				// restart monitor
				go s.monitorUpstream()
				return
			}
			time.Sleep(cfg.ReconnectDelay)
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
				if err == nil {
					s.mu.Lock()
					s.upstreamConn = conn
					for clientChan := range s.channels {
						upch, err := s.upstreamConn.Channel()
						if err == nil {
							s.channels[clientChan].upstreamCh = upch
						}
					}
					s.mu.Unlock()
					go s.drainEnqueued()
					go s.monitorUpstream()
					return
				}
				time.Sleep(cfg.ReconnectDelay)
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
		// publish and ignore errors (could re-enqueue)
		_ = chm.upstreamCh.Publish(em.exchange, em.rkey, false, false, em.pub)
	}
}

func (s *upstreamSession) getOrCreateChannel(clientChan uint16) (*upstreamChannel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.channels[clientChan]; ok {
		return c, nil
	}
	if s.upstreamConn == nil {
		return nil, fmt.Errorf("no upstream connection")
	}
	upch, err := s.upstreamConn.Channel()
	if err != nil {
		return nil, err
	}
	c := &upstreamChannel{upstreamCh: upch, clientToUpstream: map[uint64]uint64{}, upstreamToClient: map[uint64]uint64{}, nextClientTag: 1}
	s.channels[clientChan] = c
	return c, nil
}
