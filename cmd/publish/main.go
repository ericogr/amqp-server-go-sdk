package main

import (
    "flag"
    "fmt"
    "log"

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
    err = ch.Publish(
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

    fmt.Println("published")
}
