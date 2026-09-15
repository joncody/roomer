use bytes::Bytes;
use proptest::prelude::*;
use roomer::{HEADER_OVERHEAD, Message};

proptest! {
    #[test]
    fn test_message_arbitrary_fuzz(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
        // Must never panic regardless of arbitrary corrupted byte input
        let _ = Message::decode(Bytes::from(bytes));
    }

    #[test]
    fn test_message_roundtrip_proptest(
        version in any::<u8>(),
        flags in any::<u8>(),
        room in "\\PC{0, 200}",
        event in "\\PC{0, 200}",
        dst in "\\PC{0, 100}",
        src in "\\PC{0, 100}",
        payload in proptest::collection::vec(any::<u8>(), 0..2048)
    ) {
        let original = Message::with_flags(
            version,
            flags,
            room,
            event,
            dst,
            src,
            Bytes::from(payload)
        );

        let encoded = original.encode();
        prop_assert!(encoded.len() >= HEADER_OVERHEAD);

        let decoded = Message::decode(encoded).expect("valid encoded message must decode successfully");

        prop_assert_eq!(decoded.version, original.version);
        prop_assert_eq!(decoded.flags, original.flags);
        prop_assert_eq!(decoded.room, original.room);
        prop_assert_eq!(decoded.event, original.event);
        prop_assert_eq!(decoded.dst, original.dst);
        prop_assert_eq!(decoded.src, original.src);
        prop_assert_eq!(decoded.payload, original.payload);
    }
}
