package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ericogr/amqp-test/pkg/amqp"
)

// minimal encoders used by the default server
func encodeShortStr(s string) []byte {
	if len(s) > 255 {
		s = s[:255]
	}
	b := make([]byte, 1+len(s))
	b[0] = byte(len(s))
	copy(b[1:], []byte(s))
	return b
}

func encodeLongLong(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func buildContentHeaderPayload(classID uint16, bodySize uint64) []byte {
	var buf bytes.Buffer
	buf.Write(encodeShort(classID))
	buf.Write(encodeShort(0))
	buf.Write(encodeLongLong(bodySize))
	buf.Write(encodeShort(0))
	return buf.Bytes()
}

func encodeShort(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func encodeLong(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func main() {
	addr := flag.String("addr", ":5672", "listen address")
	flag.Parse()

	fmt.Fprintf(os.Stdout, "starting minimal AMQP server on %s\n", *addr)

	// minimal in-memory broker for default server behavior
	type consumer struct {
		tag       string
		channel   uint16
		writeMeth func(channel uint16, classID, methodID uint16, args []byte) error
		writeFrm  func(f amqp.Frame) error
	}
	type queueState struct {
		name            string
		messages        [][]byte
		consumers       []*consumer
		nextDeliveryTag uint64
	}

	var mu sync.Mutex
	exchanges := map[string]struct{}{}
	queues := map[string]*queueState{}

	// simple default handlers using the in-memory broker
	handlers := &amqp.ServerHandlers{}

	handlers.OnExchangeDeclare = func(ctx amqp.ConnContext, channel uint16, exchange, kind string, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		exchanges[exchange] = struct{}{}
		fmt.Printf("exchange declared: %q type=%q\n", exchange, kind)
		return nil
	}

	handlers.OnExchangeDelete = func(ctx amqp.ConnContext, channel uint16, exchange string, ifUnused bool, nowait bool, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		delete(exchanges, exchange)
		fmt.Printf("exchange deleted: %q ifUnused=%v nowait=%v\n", exchange, ifUnused, nowait)
		return nil
	}

	handlers.OnQueueDeclare = func(ctx amqp.ConnContext, channel uint16, queue string, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := queues[queue]; !ok {
			queues[queue] = &queueState{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
		}
		fmt.Printf("queue declared: %q\n", queue)
		return nil
	}

	handlers.OnQueueDelete = func(ctx amqp.ConnContext, channel uint16, queue string, ifUnused bool, ifEmpty bool, nowait bool, args []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		cnt := 0
		if q, ok := queues[queue]; ok {
			cnt = len(q.messages)
			delete(queues, queue)
		}
		fmt.Printf("queue deleted: %q count=%d ifUnused=%v ifEmpty=%v nowait=%v\n", queue, cnt, ifUnused, ifEmpty, nowait)
		return cnt, nil
	}

	handlers.OnQueuePurge = func(ctx amqp.ConnContext, channel uint16, queue string, args []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		if q, ok := queues[queue]; ok {
			cnt := len(q.messages)
			q.messages = make([][]byte, 0)
			fmt.Printf("queue purged: %q count=%d\n", queue, cnt)
			return cnt, nil
		}
		return 0, nil
	}

	handlers.OnQueueBind = func(ctx amqp.ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error {
		mu.Lock()
		defer mu.Unlock()
		// ensure queue exists
		if _, ok := queues[queue]; !ok {
			queues[queue] = &queueState{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
		}
		// ensure exchange exists
		exchanges[exchange] = struct{}{}
		fmt.Printf("bind %q -> %q key=%q\n", queue, exchange, rkey)
		return nil
	}

	handlers.OnBasicConsume = func(ctx amqp.ConnContext, channel uint16, queue, consumerTag string, flags byte, args []byte) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := queues[queue]; !ok {
			queues[queue] = &queueState{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
		}
		if consumerTag == "" {
			consumerTag = fmt.Sprintf("ctag-%d", time.Now().UnixNano())
		}
		c := &consumer{tag: consumerTag, channel: channel, writeMeth: ctx.WriteMethod, writeFrm: ctx.WriteFrame}
		queues[queue].consumers = append(queues[queue].consumers, c)
		// deliver any queued messages to this new consumer
		if len(queues[queue].messages) > 0 {
			msg := queues[queue].messages[0]
			queues[queue].messages = queues[queue].messages[1:]
			queues[queue].nextDeliveryTag++
			delTag := queues[queue].nextDeliveryTag
			var dar bytes.Buffer
			dar.Write(encodeShortStr(consumerTag))
			dar.Write(encodeLongLong(delTag))
			dar.WriteByte(0)
			dar.Write(encodeShortStr(""))
			dar.Write(encodeShortStr(""))
			_ = c.writeMeth(c.channel, 60, 60, dar.Bytes())
			_ = c.writeFrm(amqp.Frame{Type: 2, Channel: c.channel, Payload: buildContentHeaderPayload(60, uint64(len(msg)))})
			_ = c.writeFrm(amqp.Frame{Type: 3, Channel: c.channel, Payload: msg})
		}
		return consumerTag, nil
	}

	handlers.OnBasicPublish = func(ctx amqp.ConnContext, channel uint16, exchange, rkey string, properties []byte, body []byte) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		// default routing: if exchange empty, deliver to queue named rkey
		if exchange == "" {
			if q, ok := queues[rkey]; ok {
				if len(q.consumers) > 0 {
					c := q.consumers[0]
					q.nextDeliveryTag++
					delTag := q.nextDeliveryTag
					var dar bytes.Buffer
					dar.Write(encodeShortStr(c.tag))
					dar.Write(encodeLongLong(delTag))
					dar.WriteByte(0)
					dar.Write(encodeShortStr(exchange))
					dar.Write(encodeShortStr(rkey))
					_ = c.writeMeth(c.channel, 60, 60, dar.Bytes())
					_ = c.writeFrm(amqp.Frame{Type: 2, Channel: c.channel, Payload: buildContentHeaderPayload(60, uint64(len(body)))})
					_ = c.writeFrm(amqp.Frame{Type: 3, Channel: c.channel, Payload: body})
					return false, nil
				}
				// enqueue
				q.messages = append(q.messages, append([]byte(nil), body...))
				return false, nil
			}
			// no such queue -> drop
			return false, nil
		}
		// for non-empty exchange default do nothing
		return false, nil
	}

	// optional: log client-side nacks/rejects
	handlers.OnBasicNack = func(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, multiple bool, requeue bool) error {
		fmt.Printf("client basic.nack chan=%d tag=%d multiple=%v requeue=%v\n", channel, deliveryTag, multiple, requeue)
		return nil
	}
	handlers.OnBasicReject = func(ctx amqp.ConnContext, channel uint16, deliveryTag uint64, requeue bool) error {
		fmt.Printf("client basic.reject chan=%d tag=%d requeue=%v\n", channel, deliveryTag, requeue)
		return nil
	}

	handlers.OnBasicGet = func(ctx amqp.ConnContext, channel uint16, queue string, noAck bool) (bool, uint64, []byte, error) {
		mu.Lock()
		defer mu.Unlock()
		q, ok := queues[queue]
		if !ok || len(q.messages) == 0 {
			return false, 0, nil, nil
		}
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.nextDeliveryTag++
		delTag := q.nextDeliveryTag
		return true, delTag, msg, nil
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

	if err := amqp.ServeWithAuth(*addr, nil, auth, handlers); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
