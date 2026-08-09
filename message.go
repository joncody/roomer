package roomer

import (
	"bytes"
	"encoding/binary"
)

// Message represents a length-prefixed binary message for efficient parsing.
type Message struct {
	RoomLength    int
	Room          string
	EventLength   int
	Event         string
	DstLength     int
	Dst           string
	SrcLength     int
	Src           string
	PayloadLength int
	Payload       []byte
}

// readString reads a 4-byte big-endian length-prefixed string from buffer without cursor corruption on invalid input.
func readString(buf *bytes.Buffer) (string, int, bool) {
	if buf.Len() < 4 {
		return "", 0, false
	}
	b := buf.Bytes()
	length := int(binary.BigEndian.Uint32(b[:4]))
	if length < 0 || buf.Len()-4 < length {
		return "", 0, false
	}
	buf.Next(4)
	return string(buf.Next(length)), length, true
}

// readPayload reads a 4-byte big-endian length-prefixed byte slice from buffer without cursor corruption on invalid input.
func readPayload(buf *bytes.Buffer) ([]byte, int, bool) {
	if buf.Len() < 4 {
		return nil, 0, false
	}
	b := buf.Bytes()
	length := int(binary.BigEndian.Uint32(b[:4]))
	if length < 0 || buf.Len()-4 < length {
		return nil, 0, false
	}
	buf.Next(4)
	return buf.Next(length), length, true
}

// BytesToMessage decodes raw bytes into a Message (returns nil on malformed input).
func BytesToMessage(data []byte) *Message {
	if len(data) < 20 {
		return nil
	}
	buf := bytes.NewBuffer(data)
	msg := &Message{}
	var ok bool
	if msg.Room, msg.RoomLength, ok = readString(buf); !ok {
		return nil
	}
	if msg.Event, msg.EventLength, ok = readString(buf); !ok {
		return nil
	}
	if msg.Dst, msg.DstLength, ok = readString(buf); !ok {
		return nil
	}
	if msg.Src, msg.SrcLength, ok = readString(buf); !ok {
		return nil
	}
	if msg.Payload, msg.PayloadLength, ok = readPayload(buf); !ok {
		return nil
	}
	if buf.Len() != 0 {
		return nil
	}
	return msg
}

// Bytes serializes the Message into a binary format with length prefixes.
func (msg *Message) Bytes() []byte {
	roomBytes := []byte(msg.Room)
	eventBytes := []byte(msg.Event)
	dstBytes := []byte(msg.Dst)
	srcBytes := []byte(msg.Src)

	totalLen := 20 + len(roomBytes) + len(eventBytes) + len(dstBytes) + len(srcBytes) + len(msg.Payload)
	buf := make([]byte, totalLen)

	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(roomBytes)))
	offset += 4
	offset += copy(buf[offset:], roomBytes)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(eventBytes)))
	offset += 4
	offset += copy(buf[offset:], eventBytes)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(dstBytes)))
	offset += 4
	offset += copy(buf[offset:], dstBytes)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(srcBytes)))
	offset += 4
	offset += copy(buf[offset:], srcBytes)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(msg.Payload)))
	offset += 4
	copy(buf[offset:], msg.Payload)

	return buf
}

// NewMessage builds a new Message with computed length fields.
func NewMessage(room, event, dst, src string, payload []byte) *Message {
	return &Message{
		RoomLength:    len(room),
		Room:          room,
		EventLength:   len(event),
		Event:         event,
		DstLength:     len(dst),
		Dst:           dst,
		SrcLength:     len(src),
		Src:           src,
		PayloadLength: len(payload),
		Payload:       payload,
	}
}
