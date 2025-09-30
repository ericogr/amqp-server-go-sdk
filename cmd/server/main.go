package main

import (
    "flag"
    "fmt"
    "log"
    "os"

    "github.com/ericogr/amqp-test/pkg/amqp"
)

func main() {
    addr := flag.String("addr", ":5672", "listen address")
    flag.Parse()

    fmt.Fprintf(os.Stdout, "starting minimal AMQP server on %s\n", *addr)

    handler := func(channel uint16, body []byte) error {
        // For now just print the message and return nil
        fmt.Printf("[channel %d] received message: %s\n", channel, string(body))
        return nil
    }

    if err := amqp.Serve(*addr, handler); err != nil {
        log.Fatalf("server error: %v", err)
    }
}

