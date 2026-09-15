package roomer

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

const (
	// HeaderOverhead represents the 12-byte fixed wire header overhead:
	// [1B version][1B flags][2B room_len][2B event_len][1B dst_len][1B src_len][4B payload_len]
	HeaderOverhead = 12

	// CurrentProtocolVersion represents the active wire framing protocol version.
	CurrentProtocolVersion uint8 = 1

	// DefaultFlags represents standard uncompressed, unencrypted payload flags.
	DefaultFlags uint8 = 0
)

// Message represents a 12-byte header length-prefixed binary message frame.
//
// Wire format:
// [1B version][1B flags][2B room_len][room][2B event_len][event][1B dst_len][dst][1B src_len][src][4B payload_len][payload]
type Message struct {
	Version       uint8
	Flags         uint8
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

// readString16 reads a 2-byte big-endian length-prefixed UTF-8 string from a byte slice.
func readString16(data []byte, offset *int) (string, int, bool) {
	if len(data)-*offset < 2 {
		return "", 0, false
	}
	length := int(binary.BigEndian.Uint16(data[*offset:]))
	*offset += 2
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

// readString8 reads a 1-byte length-prefixed UTF-8 string from a byte slice.
func readString8(data []byte, offset *int) (string, int, bool) {
	if len(data)-*offset < 1 {
		return "", 0, false
	}
	length := int(data[*offset])
	*offset += 1
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

// readPayload32 reads a 4-byte big-endian length-prefixed raw byte slice from data.
func readPayload32(data []byte, offset *int) ([]byte, int, bool) {
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
	return BytesToMessageWithLimit(data, 0)
}

// BytesToMessageWithLimit decodes raw binary bytes into a Message with an enforced maximum frame limit.
func BytesToMessageWithLimit(data []byte, maxSize int64) *Message {
	if len(data) < HeaderOverhead {
		return nil
	}
	if maxSize > 0 && int64(len(data)) > maxSize {
		return nil
	}

	offset := 0
	msg := &Message{}

	msg.Version = data[offset]
	offset++
	msg.Flags = data[offset]
	offset++

	var ok bool
	if msg.Room, msg.RoomLength, ok = readString16(data, &offset); !ok {
		return nil
	}
	if msg.Event, msg.EventLength, ok = readString16(data, &offset); !ok {
		return nil
	}
	if msg.Dst, msg.DstLength, ok = readString8(data, &offset); !ok {
		return nil
	}
	if msg.Src, msg.SrcLength, ok = readString8(data, &offset); !ok {
		return nil
	}
	if msg.Payload, msg.PayloadLength, ok = readPayload32(data, &offset); !ok {
		return nil
	}
	if offset != len(data) {
		return nil
	}
	return msg
}

// Bytes serializes the Message into contiguous binary bytes with exact pre-allocation.
func (msg *Message) Bytes() []byte {
	totalLen := HeaderOverhead + len(msg.Room) + len(msg.Event) + len(msg.Dst) + len(msg.Src) + len(msg.Payload)
	buf := make([]byte, totalLen)

	version := msg.Version
	if version == 0 {
		version = CurrentProtocolVersion
	}

	buf[0] = version
	buf[1] = msg.Flags
	offset := 2

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(msg.Room)))
	offset += 2
	offset += copy(buf[offset:], msg.Room)

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(msg.Event)))
	offset += 2
	offset += copy(buf[offset:], msg.Event)

	buf[offset] = uint8(len(msg.Dst))
	offset++
	offset += copy(buf[offset:], msg.Dst)

	buf[offset] = uint8(len(msg.Src))
	offset++
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
		Version:       CurrentProtocolVersion,
		Flags:         DefaultFlags,
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

// NewMessageWithFlags constructs a new Message instance with custom version and flags.
func NewMessageWithFlags(version, flags uint8, room, event, dst, src string, payload []byte) *Message {
	return &Message{
		Version:       version,
		Flags:         flags,
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
