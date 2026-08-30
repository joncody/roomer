package roomer

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// Message represents a length-prefixed binary message frame.
//
// Wire format:
// [4B room_len][room][4B event_len][event][4B dst_len][dst][4B src_len][src][4B payload_len][payload]
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

// readString reads a 4-byte big-endian length-prefixed UTF-8 string from a byte slice.
func readString(data []byte, offset *int) (string, int, bool) {
	if len(data)-*offset < 4 {
		return "", 0, false
	}
	length := int(binary.BigEndian.Uint32(data[*offset:]))
	*offset += 4
	if length < 0 || len(data)-*offset < length {
		return "", 0, false
	}
	strBytes := data[*offset : *offset+length]
	if !utf8.Valid(strBytes) {
		return "", 0, false
	}
	str := string(strBytes)
	*offset += length
	return str, length, true
}

// readPayload reads a 4-byte big-endian length-prefixed raw byte slice from data.
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

// BytesToMessage decodes raw binary bytes into a Message. Returns nil on malformed input.
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

// Bytes serializes the Message into contiguous binary bytes with exact pre-allocation.
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

// PayloadString returns the payload as a string.
func (msg *Message) PayloadString() string {
	return string(msg.Payload)
}

// PayloadJSON unmarshals the message payload into the given target interface.
func (msg *Message) PayloadJSON(v any) error {
	if len(msg.Payload) == 0 {
		return errors.New("empty payload")
	}
	return json.Unmarshal(msg.Payload, v)
}

// NewMessage constructs a new Message instance.
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

// NewTextMessage constructs a new Message with a plain-text payload.
func NewTextMessage(room, event, dst, src, text string) *Message {
	return NewMessage(room, event, dst, src, []byte(text))
}

// NewJSONMessage constructs a new Message with a JSON-encoded payload.
func NewJSONMessage(room, event, dst, src string, v any) (*Message, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return NewMessage(room, event, dst, src, data), nil
}
