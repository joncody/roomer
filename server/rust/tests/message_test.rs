use bytes::Bytes;
use roomer::{DEFAULT_FLAGS, HEADER_OVERHEAD, Message, PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct UserData {
    name: String,
    age: u32,
}

#[test]
fn test_message_roundtrip() {
    let original = Message::new(
        "lobby",
        "chat",
        "user_dst",
        "user_src",
        Bytes::from_static(b"hello roomer!"),
    );

    let raw = original.encode();
    let decoded = Message::decode(raw).expect("expected message to decode successfully");

    assert_eq!(decoded.version, PROTOCOL_VERSION);
    assert_eq!(decoded.flags, DEFAULT_FLAGS);
    assert_eq!(decoded.room, original.room);
    assert_eq!(decoded.event, original.event);
    assert_eq!(decoded.dst, original.dst);
    assert_eq!(decoded.src, original.src);
    assert_eq!(decoded.payload, original.payload);
    assert_eq!(decoded.payload_str().unwrap(), "hello roomer!");
}

#[test]
fn test_message_empty_fields_overhead() {
    let empty = Message::new("", "", "", "", Bytes::new());
    let raw = empty.encode();
    assert_eq!(
        raw.len(),
        HEADER_OVERHEAD,
        "Empty message must be exactly 12 header bytes"
    );

    let decoded = Message::decode(raw).expect("empty message decodes");
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.flags, 0);
    assert_eq!(decoded.room, "");
    assert_eq!(decoded.event, "");
    assert_eq!(decoded.dst, "");
    assert_eq!(decoded.src, "");
    assert!(decoded.payload.is_empty());
}

#[test]
fn test_message_json_helpers() {
    let data = UserData {
        name: "Alice".into(),
        age: 30,
    };

    let msg = Message::with_json("room1", "user_update", "", "system", &data).unwrap();
    let decoded_data: UserData = msg.payload_json().unwrap();
    assert_eq!(decoded_data, data);
}

#[test]
fn test_message_malformed_input() {
    // Too short (< 12 bytes)
    assert!(Message::decode(Bytes::from_static(&[1, 2, 3])).is_none());
    assert!(Message::decode(Bytes::from_static(&[1, 0, 0, 4])).is_none());

    // Corrupted room length prefix
    let corrupted = vec![
        1, 0, // version, flags
        0, 255, // room_len claims 255 bytes, but buffer ends
        b'a', b'b', b'c', b'd',
    ];
    assert!(Message::decode(Bytes::from(corrupted)).is_none());
}

#[test]
fn test_message_decode_strict_with_limit() {
    let msg = Message::new("room", "evt", "", "", Bytes::from_static(b"0123456789"));
    let encoded = msg.encode();

    // Limit allows frame
    let res = Message::decode_strict_with_limit(encoded.clone(), encoded.len());
    assert!(res.is_ok());

    // Limit rejects frame
    let res_err = Message::decode_strict_with_limit(encoded.clone(), encoded.len() - 1);
    assert!(res_err.is_err());
}
