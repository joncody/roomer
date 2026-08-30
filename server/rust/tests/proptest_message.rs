use bytes::Bytes;
use proptest::prelude::*;
use roomer::Message;

proptest! {
    #[test]
    fn test_message_arbitrary_fuzz(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
        // Must never panic regardless of arbitrary corrupted byte input
        let _ = Message::decode(Bytes::from(bytes));
    }

    #[test]
    fn test_message_roundtrip_proptest(
        room in "\\PC*",
        event in "\\PC*",
        dst in "\\PC*",
        src in "\\PC*",
        payload in proptest::collection::vec(any::<u8>(), 0..2048)
    ) {
        let original = Message::new(
            room,
            event,
            dst,
            src,
            Bytes::from(payload)
        );

        let encoded = original.encode();
        let decoded = Message::decode(encoded).expect("valid encoded message must decode successfully");

        prop_assert_eq!(decoded.room, original.room);
        prop_assert_eq!(decoded.event, original.event);
        prop_assert_eq!(decoded.dst, original.dst);
        prop_assert_eq!(decoded.src, original.src);
        prop_assert_eq!(decoded.payload, original.payload);
    }
}
