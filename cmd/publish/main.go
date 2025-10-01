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

func main() {
	addr := flag.String("addr", "amqps://guest:guest@127.0.0.1:5671/", "AMQP URL")
	exchange := flag.String("exchange", "", "exchange name")
	key := flag.String("key", "test", "routing key")
	queue := flag.String("queue", "test-queue", "queue name")
	body := flag.String("body", "hello", "message body")
	deleteExchange := flag.Bool("delete-exchange", false, "delete exchange after publish")
	deleteQueue := flag.Bool("delete-queue", false, "delete queue after publish")
	purgeQueue := flag.Bool("purge-queue", false, "purge queue after publish")
	bindExchangeSource := flag.String("bind-exchange-source", "", "source exchange to bind from")
	bindExchangeDest := flag.String("bind-exchange-dest", "", "destination exchange to bind to")
	bindExchangeKey := flag.String("bind-exchange-key", "", "routing key for exchange bind")
	unbindExchangeSource := flag.String("unbind-exchange-source", "", "source exchange to unbind from")
	unbindExchangeDest := flag.String("unbind-exchange-dest", "", "destination exchange to unbind")
	unbindExchangeKey := flag.String("unbind-exchange-key", "", "routing key for exchange unbind")
	flag.Parse()

	// configure logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// dial using TLS (insecure skip verify for demo/self-signed certs)
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := amqp091.DialTLS(*addr, tlsCfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("dial")
	}
	defer func() {
		logger.Info().Msg("closing connection")
		conn.Close()
		logger.Info().Msg("connection closed")
	}()

	ch, err := conn.Channel()
	if err != nil {
		logger.Fatal().Err(err).Msg("channel")
	}
	defer func() {
		logger.Info().Msg("closing channel")
		ch.Close()
		logger.Info().Msg("channel closed")
	}()

	logger.Info().Msg("publishing")

	if err := ch.Confirm(false); err != nil {
		logger.Fatal().Err(err).Msg("channel could not be put into confirm mode")
	}

	// declare exchange and queue and bind (demo of server SDK features)
	if *exchange != "" {
		if err := ch.ExchangeDeclare(*exchange, "direct", true, false, false, false, nil); err != nil {
			logger.Fatal().Err(err).Msg("exchange declare")
		}
		if _, err := ch.QueueDeclare(*queue, true, false, false, false, nil); err != nil {
			logger.Fatal().Err(err).Msg("queue declare")
		}
		if err := ch.QueueBind(*queue, *key, *exchange, false, nil); err != nil {
			logger.Fatal().Err(err).Msg("queue bind")
		}
		// optionally bind exchanges
		if *bindExchangeSource != "" && *bindExchangeDest != "" {
			if err := ch.ExchangeBind(*bindExchangeDest, *bindExchangeKey, *bindExchangeSource, false, nil); err != nil {
				logger.Error().Err(err).Msg("exchange bind")
			} else {
				logger.Info().Str("dest", *bindExchangeDest).Str("source", *bindExchangeSource).Str("key", *bindExchangeKey).Msg("exchange bind")
			}
		}
	}

	// optionally unbind an exchange if flags provided (demo)
	if *unbindExchangeSource != "" && *unbindExchangeDest != "" {
		if err := ch.ExchangeUnbind(*unbindExchangeDest, *unbindExchangeKey, *unbindExchangeSource, false, nil); err != nil {
			logger.Error().Err(err).Msg("exchange unbind")
		} else {
			logger.Info().Str("dest", *unbindExchangeDest).Str("source", *unbindExchangeSource).Str("key", *unbindExchangeKey).Msg("exchange unbound")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dConfirm, err := ch.PublishWithDeferredConfirmWithContext(ctx,
		*exchange,
		*key,
		false,
		false,
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(*body),
		},
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("publish")
	}

	// Wait for the server to confirm the publish. Wait() will return true on ack.
	if dConfirm == nil {
		// not in confirm mode
		logger.Info().Msg("published (no confirm mode)")
		return
	}

	if ok := dConfirm.Wait(); ok {
		logger.Info().Msg("published and confirmed")
	} else {
		logger.Fatal().Msg("publish was not acknowledged or timed out")
	}

	// optionally delete exchange / queue as a demo of server delete handling
	if *deleteExchange && *exchange != "" {
		if err := ch.ExchangeDelete(*exchange, false, false); err != nil {
			logger.Error().Err(err).Msg("exchange delete")
		} else {
			logger.Info().Msg("exchange deleted")
		}
	}
	if *deleteQueue && *queue != "" {
		if _, err := ch.QueueDelete(*queue, false, false, false); err != nil {
			logger.Error().Err(err).Msg("queue delete")
		} else {
			logger.Info().Msg("queue deleted")
		}
	}

	if *purgeQueue && *queue != "" {
		if cnt, err := ch.QueuePurge(*queue, false); err != nil {
			logger.Error().Err(err).Msg("queue purge")
		} else {
			logger.Info().Int("purged_messages", cnt).Msg("queue purged")
		}
	}
}
