package amqp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestFrameRoundtrip(t *testing.T) {
	var bufConn net.Conn
	// use net.Pipe to get io.ReadWriter pair via connections
	c1, c2 := net.Pipe()
	bufConn = c1
	defer c1.Close()
	defer c2.Close()

	// write a frame from one side and read on the other
	go func() {
		// write a method frame using WriteMethod
		if err := WriteMethod(bufConn, 5, 10, 11, []byte("payload")); err != nil {
			t.Errorf("WriteMethod error: %v", err)
			return
		}
	}()

	// read from the other side
	f, err := ReadFrame(c2)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if f.Type != frameMethod {
		t.Fatalf("expected frame type %d got %d", frameMethod, f.Type)
	}
	classID, methodID, args, err := ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("ParseMethod failed: %v", err)
	}
	if classID != 10 || methodID != 11 {
		t.Fatalf("unexpected method id %d:%d", classID, methodID)
	}
	if string(args) != "payload" {
		t.Fatalf("unexpected args: %s", string(args))
	}
}

func TestServePublishConfirmFlow(t *testing.T) {
	sConn, cConn := net.Pipe()
	defer sConn.Close()
	defer cConn.Close()

	var got [][]byte
	done := make(chan struct{})

	go func() {
		// call internal handler
		handleConn(sConn, func(ctx ConnContext, channel uint16, body []byte) error {
			b := append([]byte(nil), body...)
			got = append(got, b)
			return nil
		})
		close(done)
	}()

	// client side: perform minimal handshake and publish
	// send protocol header
	hdr := []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	if _, err := cConn.Write(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}

	// read Connection.Start
	f, err := ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read start: %v", err)
	}
	classID, methodID, _, err := ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse start: %v", err)
	}
	if classID != 10 || methodID != 10 {
		t.Fatalf("expected start got %d:%d", classID, methodID)
	}

	// send Start-Ok
	if err := WriteMethod(cConn, 0, 10, 11, []byte{}); err != nil {
		t.Fatalf("write start-ok: %v", err)
	}

	// read Tune
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read tune: %v", err)
	}
	classID, methodID, _, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse tune: %v", err)
	}
	if classID != 10 || methodID != 30 {
		t.Fatalf("expected tune got %d:%d", classID, methodID)
	}

	// send Tune-Ok
	if err := WriteMethod(cConn, 0, 10, 31, []byte{}); err != nil {
		t.Fatalf("write tune-ok: %v", err)
	}

	// send Connection.Open
	if err := WriteMethod(cConn, 0, 10, 40, []byte{}); err != nil {
		t.Fatalf("write open: %v", err)
	}

	// read Open-Ok
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read open-ok: %v", err)
	}
	classID, methodID, _, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse open-ok: %v", err)
	}
	if classID != 10 || methodID != 41 {
		t.Fatalf("expected open-ok got %d:%d", classID, methodID)
	}

	// open a channel (channel 1)
	if err := WriteMethod(cConn, 1, 20, 10, []byte{}); err != nil {
		t.Fatalf("write channel.open: %v", err)
	}
	// read channel.open-ok
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read channel.open-ok: %v", err)
	}
	classID, methodID, _, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse channel.open-ok: %v", err)
	}
	if classID != 20 || methodID != 11 {
		t.Fatalf("expected channel.open-ok got %d:%d", classID, methodID)
	}

	// send confirm.select on channel 1
	if err := WriteMethod(cConn, 1, classConfirm, methodConfirmSelect, []byte{}); err != nil {
		t.Fatalf("write confirm.select: %v", err)
	}

	// read confirm.select-ok
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read confirm.select-ok: %v", err)
	}
	classID, methodID, _, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse confirm.select-ok: %v", err)
	}
	if classID != classConfirm || methodID != methodConfirmSelectOk {
		t.Fatalf("expected confirm.select-ok got %d:%d", classID, methodID)
	}

	// publish two messages and expect ack tags 1 and 2
	body1 := []byte("hello-1")
	body2 := []byte("hello-2")

	// first publish
	if err := WriteMethod(cConn, 1, 60, 40, []byte{}); err != nil {
		t.Fatalf("write basic.publish: %v", err)
	}
	hdrPayload := buildContentHeaderPayload(60, uint64(len(body1)))
	if err := WriteFrame(cConn, Frame{Type: frameHeader, Channel: 1, Payload: hdrPayload}); err != nil {
		t.Fatalf("write header frame: %v", err)
	}
	if err := WriteFrame(cConn, Frame{Type: frameBody, Channel: 1, Payload: body1}); err != nil {
		t.Fatalf("write body frame: %v", err)
	}

	// read ack1
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read ack1: %v", err)
	}
	classID, methodID, args, err := ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse ack1: %v", err)
	}
	if classID != 60 || methodID != 80 {
		t.Fatalf("expected basic.ack got %d:%d", classID, methodID)
	}
	if len(args) < 9 {
		t.Fatalf("ack1 args too short")
	}
	tag1 := binary.BigEndian.Uint64(args[0:8])
	if tag1 != 1 {
		t.Fatalf("expected tag 1 got %d", tag1)
	}

	// second publish
	if err := WriteMethod(cConn, 1, 60, 40, []byte{}); err != nil {
		t.Fatalf("write basic.publish 2: %v", err)
	}
	hdrPayload = buildContentHeaderPayload(60, uint64(len(body2)))
	if err := WriteFrame(cConn, Frame{Type: frameHeader, Channel: 1, Payload: hdrPayload}); err != nil {
		t.Fatalf("write header frame 2: %v", err)
	}
	if err := WriteFrame(cConn, Frame{Type: frameBody, Channel: 1, Payload: body2}); err != nil {
		t.Fatalf("write body frame 2: %v", err)
	}

	// read ack2
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read ack2: %v", err)
	}
	classID, methodID, args, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse ack2: %v", err)
	}
	if classID != 60 || methodID != 80 {
		t.Fatalf("expected basic.ack got %d:%d", classID, methodID)
	}
	if len(args) < 9 {
		t.Fatalf("ack2 args too short")
	}
	tag2 := binary.BigEndian.Uint64(args[0:8])
	if tag2 != 2 {
		t.Fatalf("expected tag 2 got %d", tag2)
	}

	// close client side to allow server to exit
	cConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not exit")
	}

	if len(got) != 2 {
		t.Fatalf("handler did not receive 2 bodies, got=%d", len(got))
	}
	if string(got[0]) != string(body1) || string(got[1]) != string(body2) {
		t.Fatalf("handler did not receive expected bodies, got=%v", got)
	}
}

func TestMethodRoundtripBuffer(t *testing.T) {
	// Test WriteFrame/ReadFrame roundtrip for body frames
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	go func() {
		if err := WriteFrame(c1, Frame{Type: frameBody, Channel: 1, Payload: []byte("hello")}); err != nil {
			t.Errorf("WriteFrame error: %v", err)
		}
	}()

	f, err := ReadFrame(c2)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if f.Type != frameBody || f.Channel != 1 || string(f.Payload) != "hello" {
		t.Fatalf("mismatch frame: %+v", f)
	}
}

func TestServePublishFlow(t *testing.T) {
	sConn, cConn := net.Pipe()
	defer sConn.Close()
	defer cConn.Close()

	var got []byte
	done := make(chan struct{})

	go func() {
		// call internal handler
		handleConn(sConn, func(ctx ConnContext, channel uint16, body []byte) error {
			got = append([]byte(nil), body...)
			return nil
		})
		close(done)
	}()

	// client side: perform minimal handshake and publish
	// send protocol header
	hdr := []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	if _, err := cConn.Write(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}

	// read Connection.Start
	f, err := ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read start: %v", err)
	}
	classID, methodID, _, err := ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse start: %v", err)
	}
	if classID != 10 || methodID != 10 {
		t.Fatalf("expected start got %d:%d", classID, methodID)
	}

	// send Start-Ok
	if err := WriteMethod(cConn, 0, 10, 11, []byte{}); err != nil {
		t.Fatalf("write start-ok: %v", err)
	}

	// read Tune
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read tune: %v", err)
	}
	classID, methodID, _, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse tune: %v", err)
	}
	if classID != 10 || methodID != 30 {
		t.Fatalf("expected tune got %d:%d", classID, methodID)
	}

	// send Tune-Ok
	if err := WriteMethod(cConn, 0, 10, 31, []byte{}); err != nil {
		t.Fatalf("write tune-ok: %v", err)
	}

	// send Connection.Open
	if err := WriteMethod(cConn, 0, 10, 40, []byte{}); err != nil {
		t.Fatalf("write open: %v", err)
	}

	// read Open-Ok
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read open-ok: %v", err)
	}
	classID, methodID, _, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse open-ok: %v", err)
	}
	if classID != 10 || methodID != 41 {
		t.Fatalf("expected open-ok got %d:%d", classID, methodID)
	}

	// open a channel (channel 1)
	if err := WriteMethod(cConn, 1, 20, 10, []byte{}); err != nil {
		t.Fatalf("write channel.open: %v", err)
	}
	// read channel.open-ok
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read channel.open-ok: %v", err)
	}
	classID, methodID, _, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse channel.open-ok: %v", err)
	}
	if classID != 20 || methodID != 11 {
		t.Fatalf("expected channel.open-ok got %d:%d", classID, methodID)
	}

	// send basic.publish on channel 1
	if err := WriteMethod(cConn, 1, 60, 40, []byte{}); err != nil {
		t.Fatalf("write basic.publish: %v", err)
	}

	// send content header with body size
	body := []byte("hello-test")
	hdrPayload := buildContentHeaderPayload(60, uint64(len(body)))
	if err := WriteFrame(cConn, Frame{Type: frameHeader, Channel: 1, Payload: hdrPayload}); err != nil {
		t.Fatalf("write header frame: %v", err)
	}

	// send body frame
	if err := WriteFrame(cConn, Frame{Type: frameBody, Channel: 1, Payload: body}); err != nil {
		t.Fatalf("write body frame: %v", err)
	}

	// read basic.ack from server
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	classID, methodID, _, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse ack: %v", err)
	}
	if classID != 60 || methodID != 80 {
		t.Fatalf("expected basic.ack got %d:%d", classID, methodID)
	}

	// close client side to allow server to exit
	cConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not exit")
	}

	if string(got) != string(body) {
		t.Fatalf("handler did not receive body, got=%s want=%s", string(got), string(body))
	}
}

func TestQueueDeclareConsumeFlow(t *testing.T) {
	sConn, cConn := net.Pipe()
	defer sConn.Close()
	defer cConn.Close()

	done := make(chan struct{})
	go func() {
		// create default handlers for this test (in-memory)
		var mu sync.Mutex
		type consumer struct {
			tag     string
			channel uint16
			write   func(channel uint16, classID, methodID uint16, args []byte) error
			writeF  func(f Frame) error
		}
		type qstate struct {
			name            string
			messages        [][]byte
			consumers       []*consumer
			nextDeliveryTag uint64
		}
		queues := map[string]*qstate{}
		handlers := &ServerHandlers{}
		handlers.OnQueueDeclare = func(ctx ConnContext, channel uint16, queue string, args []byte) error {
			mu.Lock()
			defer mu.Unlock()
			if _, ok := queues[queue]; !ok {
				queues[queue] = &qstate{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
			}
			return nil
		}
		handlers.OnBasicConsume = func(ctx ConnContext, channel uint16, queue, consumerTag string, flags byte, args []byte) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			if _, ok := queues[queue]; !ok {
				queues[queue] = &qstate{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
			}
			if consumerTag == "" {
				consumerTag = fmt.Sprintf("ctag-%d", time.Now().UnixNano())
			}
			c := &consumer{tag: consumerTag, channel: channel, write: ctx.WriteMethod, writeF: ctx.WriteFrame}
			queues[queue].consumers = append(queues[queue].consumers, c)
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
				_ = c.write(c.channel, 60, 60, dar.Bytes())
				_ = c.writeF(Frame{Type: frameHeader, Channel: c.channel, Payload: buildContentHeaderPayload(60, uint64(len(msg)))})
				_ = c.writeF(Frame{Type: frameBody, Channel: c.channel, Payload: msg})
			}
			return consumerTag, nil
		}
		handlers.OnBasicPublish = func(ctx ConnContext, channel uint16, exchange, rkey string, properties []byte, body []byte) error {
			mu.Lock()
			defer mu.Unlock()
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
						_ = c.write(c.channel, 60, 60, dar.Bytes())
						_ = c.writeF(Frame{Type: frameHeader, Channel: c.channel, Payload: buildContentHeaderPayload(60, uint64(len(body)))})
						_ = c.writeF(Frame{Type: frameBody, Channel: c.channel, Payload: body})
						return nil
					}
					q.messages = append(q.messages, append([]byte(nil), body...))
					return nil
				}
				return nil
			}
			return nil
		}
		handleConnWithAuth(sConn, func(ctx ConnContext, channel uint16, body []byte) error { return nil }, nil, handlers)
		close(done)
	}()

	// handshake
	hdr := []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	if _, err := cConn.Write(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}
	f, err := ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read start: %v", err)
	}
	if _, _, _, err := ParseMethod(f.Payload); err != nil {
		t.Fatalf("parse start: %v", err)
	}
	if err := WriteMethod(cConn, 0, 10, 11, []byte{}); err != nil {
		t.Fatalf("write start-ok: %v", err)
	}
	// read Tune
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read tune: %v", err)
	}
	if _, _, _, err := ParseMethod(f.Payload); err != nil {
		t.Fatalf("parse tune: %v", err)
	}
	if err := WriteMethod(cConn, 0, 10, 31, []byte{}); err != nil {
		t.Fatalf("write tune-ok: %v", err)
	}
	if err := WriteMethod(cConn, 0, 10, 40, []byte{}); err != nil {
		t.Fatalf("write open: %v", err)
	}
	if _, err := ReadFrame(cConn); err != nil {
		t.Fatalf("read open-ok: %v", err)
	}
	// open channel
	if err := WriteMethod(cConn, 1, 20, 10, []byte{}); err != nil {
		t.Fatalf("write channel.open: %v", err)
	}
	if _, err := ReadFrame(cConn); err != nil {
		t.Fatalf("read channel.open-ok: %v", err)
	}

	// declare queue
	qname := "q1"
	if err := WriteMethod(cConn, 1, classQueue, methodQueueDeclare, append(encodeShort(0), encodeShortStr(qname)...)); err != nil {
		t.Fatalf("write queue.declare: %v", err)
	}
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read queue.declare-ok: %v", err)
	}
	ci, mi, _, err := ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse declare-ok: %v", err)
	}
	if ci != classQueue || mi != methodQueueDeclareOk {
		t.Fatalf("expected queue.declare-ok got %d:%d", ci, mi)
	}

	// consume
	consumerTag := "ctag"
	carr := append(encodeShort(0), encodeShortStr(qname)...)
	carr = append(carr, encodeShortStr(consumerTag)...)
	carr = append(carr, byte(0)) // flags
	if err := WriteMethod(cConn, 1, classBasic, methodBasicConsume, carr); err != nil {
		t.Fatalf("write basic.consume: %v", err)
	}
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read consume-ok: %v", err)
	}
	ci, mi, args, err := ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse consume-ok: %v", err)
	}
	if ci != classBasic || mi != methodBasicConsumeOk {
		t.Fatalf("expected consume-ok got %d:%d", ci, mi)
	}
	// publish to default exchange routing-key = qname
	pubArgs := append(encodeShort(0), encodeShortStr("")...)
	pubArgs = append(pubArgs, encodeShortStr(qname)...)
	if err := WriteMethod(cConn, 1, classBasic, methodBasicPublish, pubArgs); err != nil {
		t.Fatalf("write basic.publish: %v", err)
	}
	if err := WriteFrame(cConn, Frame{Type: frameHeader, Channel: 1, Payload: buildContentHeaderPayload(classBasic, uint64(len([]byte("hello-q1"))))}); err != nil {
		t.Fatalf("write header frame: %v", err)
	}
	if err := WriteFrame(cConn, Frame{Type: frameBody, Channel: 1, Payload: []byte("hello-q1")}); err != nil {
		t.Fatalf("write body frame: %v", err)
	}

	// read deliver
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read deliver: %v", err)
	}
	ci, mi, args, err = ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse deliver: %v", err)
	}
	if ci != classBasic || mi != methodBasicDeliver {
		t.Fatalf("expected basic.deliver got %d:%d", ci, mi)
	}
	if len(args) < 9 {
		t.Fatalf("deliver args too short")
	}
	clen := int(args[0])
	if 1+clen+8 > len(args) {
		t.Fatalf("deliver args truncated")
	}
	// read header and body
	hf, err := ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	if hf.Type != frameHeader {
		t.Fatalf("expected header frame")
	}
	bf, err := ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(bf.Payload) != "hello-q1" {
		t.Fatalf("body mismatch got=%s", string(bf.Payload))
	}

	cConn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not exit")
	}
}

func TestBasicGetFlow(t *testing.T) {
	sConn, cConn := net.Pipe()
	defer sConn.Close()
	defer cConn.Close()

	done := make(chan struct{})
	go func() {
		var mu sync.Mutex
		type consumer struct {
			tag     string
			channel uint16
			write   func(channel uint16, classID, methodID uint16, args []byte) error
			writeF  func(f Frame) error
		}
		type qstate struct {
			name            string
			messages        [][]byte
			consumers       []*consumer
			nextDeliveryTag uint64
		}
		queues := map[string]*qstate{}
		handlers := &ServerHandlers{}
		handlers.OnQueueDeclare = func(ctx ConnContext, channel uint16, queue string, args []byte) error {
			mu.Lock()
			defer mu.Unlock()
			if _, ok := queues[queue]; !ok {
				queues[queue] = &qstate{name: queue, messages: make([][]byte, 0), consumers: make([]*consumer, 0), nextDeliveryTag: 0}
			}
			return nil
		}
		handlers.OnBasicPublish = func(ctx ConnContext, channel uint16, exchange, rkey string, properties []byte, body []byte) error {
			mu.Lock()
			defer mu.Unlock()
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
						_ = c.write(c.channel, 60, 60, dar.Bytes())
						_ = c.writeF(Frame{Type: frameHeader, Channel: c.channel, Payload: buildContentHeaderPayload(60, uint64(len(body)))})
						_ = c.writeF(Frame{Type: frameBody, Channel: c.channel, Payload: body})
						return nil
					}
					q.messages = append(q.messages, append([]byte(nil), body...))
					return nil
				}
				return nil
			}
			return nil
		}
		handlers.OnBasicGet = func(ctx ConnContext, channel uint16, queue string, noAck bool) (bool, uint64, []byte, error) {
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
		handleConnWithAuth(sConn, func(ctx ConnContext, channel uint16, body []byte) error { return nil }, nil, handlers)
		close(done)
	}()

	// handshake
	hdr := []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	if _, err := cConn.Write(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}
	f, err := ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read start: %v", err)
	}
	if _, _, _, err := ParseMethod(f.Payload); err != nil {
		t.Fatalf("parse start: %v", err)
	}
	if err := WriteMethod(cConn, 0, 10, 11, []byte{}); err != nil {
		t.Fatalf("write start-ok: %v", err)
	}
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read tune: %v", err)
	}
	if _, _, _, err := ParseMethod(f.Payload); err != nil {
		t.Fatalf("parse tune: %v", err)
	}
	if err := WriteMethod(cConn, 0, 10, 31, []byte{}); err != nil {
		t.Fatalf("write tune-ok: %v", err)
	}
	if err := WriteMethod(cConn, 0, 10, 40, []byte{}); err != nil {
		t.Fatalf("write open: %v", err)
	}
	if _, err := ReadFrame(cConn); err != nil {
		t.Fatalf("read open-ok: %v", err)
	}
	// open channel
	if err := WriteMethod(cConn, 1, 20, 10, []byte{}); err != nil {
		t.Fatalf("write channel.open: %v", err)
	}
	if _, err := ReadFrame(cConn); err != nil {
		t.Fatalf("read channel.open-ok: %v", err)
	}

	// declare queue
	qname := "qget"
	if err := WriteMethod(cConn, 1, classQueue, methodQueueDeclare, append(encodeShort(0), encodeShortStr(qname)...)); err != nil {
		t.Fatalf("write queue.declare: %v", err)
	}
	if _, err := ReadFrame(cConn); err != nil {
		t.Fatalf("read queue.declare-ok: %v", err)
	}

	// publish a message
	pubArgs := append(encodeShort(0), encodeShortStr("")...)
	pubArgs = append(pubArgs, encodeShortStr(qname)...)
	if err := WriteMethod(cConn, 1, classBasic, methodBasicPublish, pubArgs); err != nil {
		t.Fatalf("write basic.publish: %v", err)
	}
	body := []byte("hello-get")
	if err := WriteFrame(cConn, Frame{Type: frameHeader, Channel: 1, Payload: buildContentHeaderPayload(classBasic, uint64(len(body)))}); err != nil {
		t.Fatalf("write header frame: %v", err)
	}
	if err := WriteFrame(cConn, Frame{Type: frameBody, Channel: 1, Payload: body}); err != nil {
		t.Fatalf("write body frame: %v", err)
	}

	// read basic.ack
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read ack after publish: %v", err)
	}
	ci, mi, _, err := ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse ack after publish: %v", err)
	}
	if ci != classBasic || mi != methodBasicAck {
		t.Fatalf("expected basic.ack after publish got %d:%d", ci, mi)
	}

	// call basic.get
	getArgs := append(encodeShort(0), encodeShortStr(qname)...) // no-ack omitted
	if err := WriteMethod(cConn, 1, classBasic, methodBasicGet, getArgs); err != nil {
		t.Fatalf("write basic.get: %v", err)
	}
	f, err = ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read get-ok: %v", err)
	}
	ci, mi, args, err := ParseMethod(f.Payload)
	if err != nil {
		t.Fatalf("parse get-ok: %v", err)
	}
	if ci != classBasic || mi != methodBasicGetOk {
		t.Fatalf("expected get-ok got %d:%d", ci, mi)
	}
	if len(args) < 8 {
		t.Fatalf("get-ok args too short")
	}
	hf, err := ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	if hf.Type != frameHeader {
		t.Fatalf("expected header frame")
	}
	bf, err := ReadFrame(cConn)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(bf.Payload) != string(body) {
		t.Fatalf("body mismatch got=%s want=%s", string(bf.Payload), string(body))
	}

	cConn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not exit")
	}
}
