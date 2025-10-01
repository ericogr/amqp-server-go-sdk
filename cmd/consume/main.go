package main

import (
	"crypto/tls"
	"flag"
	"os"

	"github.com/rs/zerolog"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

func main() {
	addr := flag.String("addr", "amqps://guest:guest@127.0.0.1:5671/", "AMQP URL")
	queue := flag.String("queue", "test-queue", "queue name")
	autoAck := flag.Bool("auto-ack", false, "auto ack messages")
	insecure := flag.Bool("insecure", true, "skip TLS verify for demo")
	flag.Parse()

	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	tlsCfg := &tls.Config{InsecureSkipVerify: *insecure}
	conn, err := amqp091.DialTLS(*addr, tlsCfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("dial")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logger.Fatal().Err(err).Msg("channel")
	}
	defer ch.Close()

	// ensure queue exists
	if _, err := ch.QueueDeclare(*queue, true, false, false, false, nil); err != nil {
		logger.Fatal().Err(err).Msg("queue declare")
	}

	msgs, err := ch.Consume(*queue, "", *autoAck, false, false, false, nil)
	if err != nil {
		logger.Fatal().Err(err).Msg("consume")
	}

	logger.Info().Str("queue", *queue).Bool("autoAck", *autoAck).Msg("consuming from queue")
	for d := range msgs {
		logger.Info().Uint64("delivery-tag", d.DeliveryTag).Str("body", string(d.Body)).Msg("received delivery")
		if !*autoAck {
			if err := d.Ack(false); err != nil {
				logger.Error().Err(err).Msg("ack failed")
			} else {
				logger.Info().Uint64("delivery-tag", d.DeliveryTag).Msg("acked delivery")
			}
		}
	}
}
