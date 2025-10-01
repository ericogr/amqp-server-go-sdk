package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"time"

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
	flag.Parse()

	// dial using TLS (insecure skip verify for demo/self-signed certs)
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := amqp091.DialTLS(*addr, tlsCfg)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() {
		fmt.Println("closing connection...")
		conn.Close()
		fmt.Println("connection closed")
	}()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel: %v", err)
	}
	defer func() {
		fmt.Println("closing channel...")
		ch.Close()
		fmt.Println("channel closed")
	}()

	fmt.Println("publishing...")

	if err := ch.Confirm(false); err != nil {
		log.Fatalf("channel could not be put into confirm mode: %v", err)
	}

	// declare exchange and queue and bind (demo of server SDK features)
	if *exchange != "" {
		if err := ch.ExchangeDeclare(*exchange, "direct", true, false, false, false, nil); err != nil {
			log.Fatalf("exchange declare: %v", err)
		}
		if _, err := ch.QueueDeclare(*queue, true, false, false, false, nil); err != nil {
			log.Fatalf("queue declare: %v", err)
		}
		if err := ch.QueueBind(*queue, *key, *exchange, false, nil); err != nil {
			log.Fatalf("queue bind: %v", err)
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
		log.Fatalf("publish: %v", err)
	}

	// Wait for the server to confirm the publish. Wait() will return true on ack.
	if dConfirm == nil {
		// not in confirm mode
		fmt.Println("published (no confirm mode)")
		return
	}

	if ok := dConfirm.Wait(); ok {
		fmt.Println("published and confirmed")
	} else {
		log.Fatalf("publish was not acknowledged or timed out")
	}

	// optionally delete exchange / queue as a demo of server delete handling
	if *deleteExchange && *exchange != "" {
		if err := ch.ExchangeDelete(*exchange, false, false); err != nil {
			log.Printf("exchange delete: %v", err)
		} else {
			fmt.Println("exchange deleted")
		}
	}
	if *deleteQueue && *queue != "" {
		if _, err := ch.QueueDelete(*queue, false, false, false); err != nil {
			log.Printf("queue delete: %v", err)
		} else {
			fmt.Println("queue deleted")
		}
	}

	if *purgeQueue && *queue != "" {
		if cnt, err := ch.QueuePurge(*queue, false); err != nil {
			log.Printf("queue purge: %v", err)
		} else {
			fmt.Printf("queue purged: %d messages\n", cnt)
		}
	}
}
