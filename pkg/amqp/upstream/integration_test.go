//go:build integration
// +build integration

package upstream

import (
	"context"
	"net"
	"os"
	"testing"

	amppkg "github.com/ericogr/amqp-test/pkg/amqp"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// This integration test requires a running RabbitMQ instance available at
// RABBIT_URL (default: amqp://guest:guest@127.0.0.1:5672/). It verifies the
// adapter can connect upstream and perform a basic declare/publish/get.
func TestIntegration_UpstreamBasic(t *testing.T) {
	url := os.Getenv("RABBIT_URL")
	if url == "" {
		url = "amqp://guest:guest@127.0.0.1:5672/"
	}

	a := NewUpstreamAdapter(UpstreamConfig{URL: url, TLS: false})

	// create a fake client connection using net.Pipe
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx := amppkg.ConnContext{Conn: serverConn}

	// perform auth handshake using PLAIN with guest:guest
	resp := []byte{0, 'g', 'u', 'e', 's', 't', 0, 'g', 'u', 'e', 's', 't'}
	if err := a.AuthHandler(ctx, "PLAIN", resp); err != nil {
		t.Fatalf("AuthHandler failed: %v", err)
	}

	// verify upstream session exists and can declare/publish/get
	s := a.getOrCreateSession(amppkg.ConnContext{Conn: serverConn})
	ch, err := s.getOrCreateChannel(1)
	if err != nil {
		t.Fatalf("getOrCreateChannel: %v", err)
	}

	q := "amqp_upstream_it_test"
	if _, err := ch.upstreamCh.QueueDeclare(q, false, true, false, false, nil); err != nil {
		t.Fatalf("queue declare failed: %v", err)
	}

	body := []byte("hello-it")
	if err := ch.upstreamCh.PublishWithContext(context.Background(), "", q, false, false, amqp091.Publishing{Body: body}); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	msg, ok, err := ch.upstreamCh.Get(q, true)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !ok {
		t.Fatalf("no message retrieved")
	}
	if string(msg.Body) != string(body) {
		t.Fatalf("body mismatch: want %s got %s", string(body), string(msg.Body))
	}
}
