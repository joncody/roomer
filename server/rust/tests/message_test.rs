use bytes::Bytes;
use roomer::Message;
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

    assert_eq!(decoded.room, original.room);
    assert_eq!(decoded.event, original.event);
    assert_eq!(decoded.dst, original.dst);
    assert_eq!(decoded.src, original.src);
    assert_eq!(decoded.payload, original.payload);
    assert_eq!(decoded.payload_str().unwrap(), "hello roomer!");
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
    assert!(Message::decode(Bytes::from_static(&[1, 2, 3])).is_none());

    let corrupted = vec![
        0, 0, 0, 255, b'a', b'b', b'c', b'd', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    assert!(Message::decode(Bytes::from(corrupted)).is_none());
}
