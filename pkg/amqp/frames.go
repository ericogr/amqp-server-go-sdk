package amqp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"io"
)

const (
	frameMethod    = 1
	frameHeader    = 2
	frameBody      = 3
	frameHeartbeat = 8
	frameEnd       = 0xCE
)

// package logger used for SDK logs. Libraries should default to a no-op
// logger and let the embedding application configure logging. Use
// SetLogger to provide an application logger.
var logger zerolog.Logger = zerolog.Nop()

// SetLogger sets the package logger used by the AMQP SDK. Callers should
// pass a configured `zerolog.Logger` (for example one created with
// `zerolog.New(os.Stderr).With().Timestamp().Logger()`).
func SetLogger(l zerolog.Logger) { logger = l }

// limits and well-known classes/methods
const (
	MaxFrameSize = 1 << 20 // 1MB

	classConnection = 10
	classChannel    = 20
	classExchange   = 40
	classQueue      = 50
	classBasic      = 60
	classConfirm    = 85

	methodConnStart   = 10
	methodConnStartOk = 11
	methodConnTune    = 30
	methodConnTuneOk  = 31
	methodConnOpen    = 40
	methodConnOpenOk  = 41
	methodConnClose   = 50
	methodConnCloseOk = 51

	methodChannelOpen    = 10
	methodChannelOpenOk  = 11
	methodChannelClose   = 40
	methodChannelCloseOk = 41

	methodBasicPublish    = 40
	methodBasicAck        = 80
	methodConfirmSelect   = 10
	methodConfirmSelectOk = 11
	// exchange methods (class 40)
	methodExchangeDeclare   = 10
	methodExchangeDeclareOk = 11
	methodExchangeDelete    = 20
	methodExchangeDeleteOk  = 21
	methodExchangeBind      = 30
	methodExchangeBindOk    = 31
	methodExchangeUnbind    = 40
	methodExchangeUnbindOk  = 51

	// queue methods (class 50)
	methodQueueDeclare   = 10
	methodQueueDeclareOk = 11
	methodQueueBind      = 20
	methodQueueBindOk    = 21
	methodQueuePurge     = 30
	methodQueuePurgeOk   = 31
	methodQueueUnbind    = 50
	methodQueueUnbindOk  = 51
	methodQueueDelete    = 40
	methodQueueDeleteOk  = 41

	// basic methods (class 60)
	methodBasicQos       = 10
	methodBasicConsume   = 20
	methodBasicConsumeOk = 21
	methodBasicCancel    = 30
	methodBasicCancelOk  = 31
	methodBasicReturn    = 50
	methodBasicDeliver   = 60
	methodBasicGet       = 70
	methodBasicGetOk     = 71
	methodBasicGetEmpty  = 72
	methodBasicReject    = 90
	methodBasicNack      = 120
)

// Frame represents a raw AMQP frame
type Frame struct {
	Type    uint8
	Channel uint16
	Payload []byte
}

// ReadFrame reads a single frame from r
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [7]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	t := hdr[0]
	ch := binary.BigEndian.Uint16(hdr[1:3])
	size := binary.BigEndian.Uint32(hdr[3:7])
	if size > MaxFrameSize {
		return Frame{}, fmt.Errorf("frame size %d exceeds limit %d", size, MaxFrameSize)
	}
	payload := make([]byte, size)
	if size > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, err
		}
	}
	// read frame-end octet
	var end [1]byte
	if _, err := io.ReadFull(r, end[:]); err != nil {
		return Frame{}, err
	}
	if end[0] != frameEnd {
		return Frame{}, errors.New("invalid frame end")
	}
	return Frame{Type: t, Channel: ch, Payload: payload}, nil
}

// WriteFrame writes a frame to w
func WriteFrame(w io.Writer, f Frame) error {
	var hdr [7]byte
	hdr[0] = f.Type
	binary.BigEndian.PutUint16(hdr[1:3], f.Channel)
	binary.BigEndian.PutUint32(hdr[3:7], uint32(len(f.Payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return err
		}
	}
	// frame end
	if _, err := w.Write([]byte{frameEnd}); err != nil {
		return err
	}
	return nil
}

// helper to write a method frame (type 1). args does NOT include class/method ids.
func WriteMethod(w io.Writer, channel uint16, classID, methodID uint16, args []byte) error {
	payload := make([]byte, 4+len(args))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	copy(payload[4:], args)
	return WriteFrame(w, Frame{Type: frameMethod, Channel: channel, Payload: payload})
}

// ParseMethod parses a method frame payload and returns class, method and remaining args
func ParseMethod(payload []byte) (classID, methodID uint16, args []byte, err error) {
	if len(payload) < 4 {
		return 0, 0, nil, fmt.Errorf("method payload too short")
	}
	classID = binary.BigEndian.Uint16(payload[0:2])
	methodID = binary.BigEndian.Uint16(payload[2:4])
	args = payload[4:]
	return classID, methodID, args, nil
}
