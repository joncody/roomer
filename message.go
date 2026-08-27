package roomer

import (
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

// readString reads a 4-byte big-endian length-prefixed string from slice without cursor corruption on invalid input.
func readString(data []byte, offset *int) (string, int, bool) {
	if len(data)-*offset < 4 {
		return "", 0, false
	}
	length := int(binary.BigEndian.Uint32(data[*offset:]))
	*offset += 4
	if length < 0 || len(data)-*offset < length {
		return "", 0, false
	}
	str := string(data[*offset : *offset+length])
	*offset += length
	return str, length, true
}

// readPayload reads a 4-byte big-endian length-prefixed byte slice from slice without cursor corruption on invalid input.
func readPayload(data []byte, offset *int) ([]byte, int, bool) {
	if len(data)-*offset < 4 {
		return nil, 0, false
	}
	length := int(binary.BigEndian.Uint32(data[*offset:]))
	*offset += 4
	if length < 0 || len(data)-*offset < length {
		return nil, 0, false
	}
	payload := data[*offset : *offset+length]
	*offset += length
	return payload, length, true
}

// BytesToMessage decodes raw bytes into a Message (returns nil on malformed input).
func BytesToMessage(data []byte) *Message {
	if len(data) < 20 {
		return nil
	}
	offset := 0
	msg := &Message{}
	var ok bool
	if msg.Room, msg.RoomLength, ok = readString(data, &offset); !ok {
		return nil
	}
	if msg.Event, msg.EventLength, ok = readString(data, &offset); !ok {
		return nil
	}
	if msg.Dst, msg.DstLength, ok = readString(data, &offset); !ok {
		return nil
	}
	if msg.Src, msg.SrcLength, ok = readString(data, &offset); !ok {
		return nil
	}
	if msg.Payload, msg.PayloadLength, ok = readPayload(data, &offset); !ok {
		return nil
	}
	if offset != len(data) {
		return nil
	}
	return msg
}

// Bytes serializes the Message into a binary format with length prefixes.
func (msg *Message) Bytes() []byte {
	totalLen := 20 + len(msg.Room) + len(msg.Event) + len(msg.Dst) + len(msg.Src) + len(msg.Payload)
	buf := make([]byte, totalLen)

	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(msg.Room)))
	offset += 4
	offset += copy(buf[offset:], msg.Room)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(msg.Event)))
	offset += 4
	offset += copy(buf[offset:], msg.Event)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(msg.Dst)))
	offset += 4
	offset += copy(buf[offset:], msg.Dst)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(msg.Src)))
	offset += 4
	offset += copy(buf[offset:], msg.Src)

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
