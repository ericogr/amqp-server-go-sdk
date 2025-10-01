package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

func main() {
	addr := flag.String("addr", "amqp://guest:guest@127.0.0.1:5672/", "AMQP URL")
	exchange := flag.String("exchange", "", "exchange name")
	key := flag.String("key", "test", "routing key")
	body := flag.String("body", "hello", "message body")
	flag.Parse()

	conn, err := amqp091.Dial(*addr)
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
}
