package main

import (
	"bytes"
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

	// simple auth handler: accept PLAIN guest:guest
	auth := func(mechanism string, response []byte) error {
		if mechanism != "PLAIN" {
			return fmt.Errorf("unsupported mechanism %q", mechanism)
		}
		// PLAIN response: authzid \x00 authcid \x00 password
		parts := bytes.SplitN(response, []byte{0}, 3)
		var username, password string
		if len(parts) == 3 {
			username = string(parts[1])
			password = string(parts[2])
		} else if len(parts) == 2 {
			username = string(parts[0])
			password = string(parts[1])
		} else {
			return fmt.Errorf("invalid PLAIN response")
		}
		if username != "guest" || password != "guest" {
			return fmt.Errorf("invalid credentials")
		}

		fmt.Printf("user %s authentication successful\n", username)

		return nil
	}

	if err := amqp.ServeWithAuth(*addr, handler, auth); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
