package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"time"

	"github.com/rs/zerolog"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

type publishConfig struct {
	Addr                 string
	Exchange             string
	Key                  string
	Queue                string
	Body                 string
	DeleteExchange       bool
	DeleteQueue          bool
	PurgeQueue           bool
	BindExchangeSource   string
	BindExchangeDest     string
	BindExchangeKey      string
	UnbindExchangeSource string
	UnbindExchangeDest   string
	UnbindExchangeKey    string
	PrefetchCount        int
	Mandatory            bool
}

func parsePublishConfig() publishConfig {
	var c publishConfig
	flag.StringVar(&c.Addr, "addr", "amqps://admin:admin@127.0.0.1:5671/", "AMQP URL")
	flag.StringVar(&c.Exchange, "exchange", "", "exchange name")
	flag.StringVar(&c.Key, "key", "test", "routing key")
	flag.StringVar(&c.Queue, "queue", "test-queue", "queue name")
	flag.StringVar(&c.Body, "body", "hello", "message body")
	flag.BoolVar(&c.DeleteExchange, "delete-exchange", false, "delete exchange after publish")
	flag.BoolVar(&c.DeleteQueue, "delete-queue", false, "delete queue after publish")
	flag.BoolVar(&c.PurgeQueue, "purge-queue", false, "purge queue after publish")
	flag.StringVar(&c.BindExchangeSource, "bind-exchange-source", "", "source exchange to bind from")
	flag.StringVar(&c.BindExchangeDest, "bind-exchange-dest", "", "destination exchange to bind to")
	flag.StringVar(&c.BindExchangeKey, "bind-exchange-key", "", "routing key for exchange bind")
	flag.StringVar(&c.UnbindExchangeSource, "unbind-exchange-source", "", "source exchange to unbind from")
	flag.StringVar(&c.UnbindExchangeDest, "unbind-exchange-dest", "", "destination exchange to unbind")
	flag.StringVar(&c.UnbindExchangeKey, "unbind-exchange-key", "", "routing key for exchange unbind")
	flag.IntVar(&c.PrefetchCount, "prefetch-count", 0, "basic.qos prefetch-count (0 = no limit)")
	flag.BoolVar(&c.Mandatory, "mandatory", false, "publish with mandatory=true to request basic.return for unroutable messages")
	flag.Parse()
	return c
}

func newLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).With().Timestamp().Logger()
}

func openConnectionAndChannel(addr string, logger zerolog.Logger) (*amqp091.Connection, *amqp091.Channel) {
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := amqp091.DialTLS(addr, tlsCfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("dial")
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		logger.Fatal().Err(err).Msg("channel")
	}
	return conn, ch
}

func ensureExchangeAndQueue(ch *amqp091.Channel, exchange, queue, key string, logger zerolog.Logger) {
	if exchange == "" {
		return
	}
	if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		logger.Fatal().Err(err).Msg("exchange declare")
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		logger.Fatal().Err(err).Msg("queue declare")
	}
	if err := ch.QueueBind(queue, key, exchange, false, nil); err != nil {
		logger.Fatal().Err(err).Msg("queue bind")
	}
}

func optionalBind(ch *amqp091.Channel, dest, source, key string, logger zerolog.Logger) {
	if source == "" || dest == "" {
		return
	}
	if err := ch.ExchangeBind(dest, key, source, false, nil); err != nil {
		logger.Error().Err(err).Msg("exchange bind")
	} else {
		logger.Info().Str("dest", dest).Str("source", source).Str("key", key).Msg("exchange bind")
	}
}

func optionalUnbind(ch *amqp091.Channel, dest, source, key string, logger zerolog.Logger) {
	if source == "" || dest == "" {
		return
	}
	if err := ch.ExchangeUnbind(dest, key, source, false, nil); err != nil {
		logger.Error().Err(err).Msg("exchange unbind")
	} else {
		logger.Info().Str("dest", dest).Str("source", source).Str("key", key).Msg("exchange unbound")
	}
}

func publishAndWait(ctx context.Context, ch *amqp091.Channel, exchange, key string, body []byte, mandatory bool, logger zerolog.Logger) {
	dConfirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, false, amqp091.Publishing{ContentType: "text/plain", Body: body})
	if err != nil {
		logger.Fatal().Err(err).Msg("publish")
	}
	if dConfirm == nil {
		logger.Info().Msg("published (no confirm mode)")
		return
	}
	if ok := dConfirm.Wait(); ok {
		logger.Info().Msg("published and confirmed")
	} else {
		logger.Fatal().Msg("publish was not acknowledged or timed out")
	}
}

func deleteAndPurge(ch *amqp091.Channel, cfg publishConfig, logger zerolog.Logger) {
	if cfg.DeleteExchange && cfg.Exchange != "" {
		if err := ch.ExchangeDelete(cfg.Exchange, false, false); err != nil {
			logger.Error().Err(err).Msg("exchange delete")
		} else {
			logger.Info().Msg("exchange deleted")
		}
	}
	if cfg.DeleteQueue && cfg.Queue != "" {
		if _, err := ch.QueueDelete(cfg.Queue, false, false, false); err != nil {
			logger.Error().Err(err).Msg("queue delete")
		} else {
			logger.Info().Msg("queue deleted")
		}
	}
	if cfg.PurgeQueue && cfg.Queue != "" {
		if cnt, err := ch.QueuePurge(cfg.Queue, false); err != nil {
			logger.Error().Err(err).Msg("queue purge")
		} else {
			logger.Info().Int("purged_messages", cnt).Msg("queue purged")
		}
	}
}

func main() {
	cfg := parsePublishConfig()
	logger := newLogger()

	conn, ch := openConnectionAndChannel(cfg.Addr, logger)
	defer func() {
		logger.Info().Msg("closing channel")
		if err := ch.Close(); err != nil {
			logger.Error().Err(err).Msg("channel close error")
		}
		logger.Info().Msg("channel closed")
		logger.Info().Msg("closing connection")
		if err := conn.Close(); err != nil {
			logger.Error().Err(err).Msg("connection close error")
		}
		logger.Info().Msg("connection closed")
	}()

	logger.Info().Msg("publishing")
	if err := ch.Confirm(false); err != nil {
		logger.Fatal().Err(err).Msg("channel could not be put into confirm mode")
	}

	// apply QoS if requested
	if cfg.PrefetchCount > 0 {
		if err := ch.Qos(cfg.PrefetchCount, 0, false); err != nil {
			logger.Error().Err(err).Msg("failed to set qos")
		} else {
			logger.Info().Int("prefetch_count", cfg.PrefetchCount).Msg("qos set")
		}
	}

	// listen for basic.return (unroutable messages) when publishing mandatory
	returns := make(chan amqp091.Return)
	ch.NotifyReturn(returns)
	go func() {
		for r := range returns {
			logger.Info().Str("exchange", r.Exchange).Str("routing_key", r.RoutingKey).Str("body", string(r.Body)).Msg("message returned")
		}
	}()

	ensureExchangeAndQueue(ch, cfg.Exchange, cfg.Queue, cfg.Key, logger)
	optionalBind(ch, cfg.BindExchangeDest, cfg.BindExchangeSource, cfg.BindExchangeKey, logger)
	optionalUnbind(ch, cfg.UnbindExchangeDest, cfg.UnbindExchangeSource, cfg.UnbindExchangeKey, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	publishAndWait(ctx, ch, cfg.Exchange, cfg.Key, []byte(cfg.Body), cfg.Mandatory, logger)

	deleteAndPurge(ch, cfg, logger)
}
