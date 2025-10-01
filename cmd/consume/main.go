package main

import (
	"crypto/tls"
	"flag"
	"os"

	"github.com/rs/zerolog"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

type consumeConfig struct {
	Addr     string
	Queue    string
	AutoAck  bool
	Insecure bool
}

func parseConsumeConfig() consumeConfig {
	var c consumeConfig
	flag.StringVar(&c.Addr, "addr", "amqps://guest:guest@127.0.0.1:5671/", "AMQP URL")
	flag.StringVar(&c.Queue, "queue", "test-queue", "queue name")
	flag.BoolVar(&c.AutoAck, "auto-ack", false, "auto ack messages")
	flag.BoolVar(&c.Insecure, "insecure", true, "skip TLS verify for demo")
	flag.Parse()
	return c
}

func newLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).With().Timestamp().Logger()
}

func runConsumer(cfg consumeConfig, logger zerolog.Logger) {
	tlsCfg := &tls.Config{InsecureSkipVerify: cfg.Insecure}
	conn, err := amqp091.DialTLS(cfg.Addr, tlsCfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("dial")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logger.Fatal().Err(err).Msg("channel")
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
		logger.Fatal().Err(err).Msg("queue declare")
	}

	msgs, err := ch.Consume(cfg.Queue, "", cfg.AutoAck, false, false, false, nil)
	if err != nil {
		logger.Fatal().Err(err).Msg("consume")
	}

	logger.Info().Str("queue", cfg.Queue).Bool("autoAck", cfg.AutoAck).Msg("consuming from queue")
	for d := range msgs {
		logger.Info().Uint64("delivery-tag", d.DeliveryTag).Str("body", string(d.Body)).Msg("received delivery")
		if !cfg.AutoAck {
			if err := d.Ack(false); err != nil {
				logger.Error().Err(err).Msg("ack failed")
			} else {
				logger.Info().Uint64("delivery-tag", d.DeliveryTag).Msg("acked delivery")
			}
		}
	}
}

func main() {
	cfg := parseConsumeConfig()
	logger := newLogger()
	runConsumer(cfg, logger)
}
