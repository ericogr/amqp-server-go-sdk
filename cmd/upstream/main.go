package main

import (
	"crypto/tls"
	"flag"
	"os"
	"time"

	amqp "github.com/ericogr/go-amqp-server-sdk/pkg/amqp"
	"github.com/ericogr/go-amqp-server-sdk/pkg/amqp/upstream"
	"github.com/rs/zerolog"
)

// Example upstream proxy: delegates all operations to a configured upstream
// RabbitMQ broker by default. Use flags to configure listening address and
// upstream connection.
func main() {
	addr := flag.String("addr", ":5672", "listen address")
	upstreamURL := flag.String("upstream", "amqp://admin:admin@127.0.0.1:5672/", "upstream broker URL")
	upstreamTLS := flag.Bool("upstream-tls", false, "use TLS when connecting to upstream broker")
	tlsSkipVerify := flag.Bool("upstream-tls-skip-verify", true, "skip TLS verification for upstream (example only)")
	failurePolicy := flag.String("failure-policy", "reconnect", "failure policy: close|reconnect|enqueue")
	reconnectDelay := flag.Int("reconnect-delay", 5, "reconnect delay seconds")
	flag.Parse()

	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	amqp.SetLogger(logger)

	var policy upstream.FailurePolicy
	switch *failurePolicy {
	case "close":
		policy = upstream.FailCloseClient
	case "enqueue":
		policy = upstream.FailEnqueue
	default:
		policy = upstream.FailReconnect
	}

	cfg := upstream.UpstreamConfig{
		URL:            *upstreamURL,
		TLS:            *upstreamTLS,
		FailurePolicy:  policy,
		ReconnectDelay: time.Duration(*reconnectDelay) * time.Second,
	}
	if cfg.TLS {
		cfg.TLSConfig = &tls.Config{InsecureSkipVerify: *tlsSkipVerify}
	}

	adapter := upstream.NewUpstreamAdapter(cfg)
	adapter.SetLogger(logger)

	logger.Info().Str("addr", *addr).Str("upstream", *upstreamURL).Msg("starting AMQP upstream proxy (delegates by default)")

	if err := amqp.ServeWithAuth(*addr, nil, adapter.AuthHandler, adapter.Handlers()); err != nil {
		logger.Fatal().Err(err).Msg("server error")
	}
}
