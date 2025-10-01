package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

func main() {
	addr := flag.String("addr", "amqps://guest:guest@127.0.0.1:5671/", "AMQP URL")
	queue := flag.String("queue", "test-queue", "queue name")
	autoAck := flag.Bool("auto-ack", false, "auto ack messages")
	insecure := flag.Bool("insecure", true, "skip TLS verify for demo")
	flag.Parse()

	tlsCfg := &tls.Config{InsecureSkipVerify: *insecure}
	conn, err := amqp091.DialTLS(*addr, tlsCfg)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	// ensure queue exists
	if _, err := ch.QueueDeclare(*queue, true, false, false, false, nil); err != nil {
		log.Fatalf("queue declare: %v", err)
	}

	msgs, err := ch.Consume(*queue, "", *autoAck, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	fmt.Printf("consuming from queue %s (autoAck=%v)\n", *queue, *autoAck)
	for d := range msgs {
		fmt.Printf("received delivery-tag=%d body=%s\n", d.DeliveryTag, string(d.Body))
		if !*autoAck {
			if err := d.Ack(false); err != nil {
				log.Printf("ack failed: %v", err)
			} else {
				fmt.Printf("acked delivery-tag=%d\n", d.DeliveryTag)
			}
		}
	}
}
