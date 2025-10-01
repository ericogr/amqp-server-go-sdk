package amqp

import (
	"encoding/binary"
	"net"
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
		handleConn(sConn, func(channel uint16, body []byte) error {
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
		handleConn(sConn, func(channel uint16, body []byte) error {
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
