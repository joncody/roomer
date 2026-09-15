use crate::error::FrameError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Protocol version identifier.
pub const PROTOCOL_VERSION: u8 = 1;

/// Default wire protocol flags.
pub const DEFAULT_FLAGS: u8 = 0;

/// Base header overhead in bytes:
/// `[1B version][1B flags][2B room_len][2B event_len][1B dst_len][1B src_len][4B payload_len]`
pub const HEADER_OVERHEAD: usize = 12;

/// High-performance binary message packet framing with a 12-byte header overhead.
///
/// Binary wire format:
/// `[1B version][1B flags][2B room_len][room][2B event_len][event][1B dst_len][dst][1B src_len][src][4B payload_len][payload]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Protocol version.
    pub version: u8,
    /// Bitfield flags reserved for compression, encryption, or fragmentation.
    pub flags: u8,
    /// Target room channel name.
    pub room: String,
    /// Event name descriptor.
    pub event: String,
    /// Destination client ID (empty string if room broadcast).
    pub dst: String,
    /// Source client ID.
    pub src: String,
    /// Zero-copy binary message payload.
    pub payload: Bytes,
}

impl Message {
    /// Creates a new `Message` instance with protocol version 1 and zero flags.
    ///
    /// # Example
    /// ```rust
    /// use roomer::Message;
    /// use bytes::Bytes;
    ///
    /// let msg = Message::new("lobby", "chat", "", "user_1", Bytes::from_static(b"hello"));
    /// assert_eq!(msg.room, "lobby");
    /// assert_eq!(msg.version, 1);
    /// ```
    pub fn new(
        room: impl Into<String>,
        event: impl Into<String>,
        dst: impl Into<String>,
        src: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self::with_flags(
            PROTOCOL_VERSION,
            DEFAULT_FLAGS,
            room,
            event,
            dst,
            src,
            payload,
        )
    }

    /// Creates a new `Message` with custom version and flags.
    pub fn with_flags(
        version: u8,
        flags: u8,
        room: impl Into<String>,
        event: impl Into<String>,
        dst: impl Into<String>,
        src: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            version,
            flags,
            room: room.into(),
            event: event.into(),
            dst: dst.into(),
            src: src.into(),
            payload: payload.into(),
        }
    }

    /// Convenience constructor for UTF-8 string payloads.
    ///
    /// # Example
    /// ```rust
    /// use roomer::Message;
    ///
    /// let msg = Message::with_text("lobby", "chat", "", "user_1", "hello roomer");
    /// assert_eq!(msg.payload_str().unwrap(), "hello roomer");
    /// ```
    pub fn with_text(
        room: impl Into<String>,
        event: impl Into<String>,
        dst: impl Into<String>,
        src: impl Into<String>,
        text: impl AsRef<str>,
    ) -> Self {
        Self::new(
            room,
            event,
            dst,
            src,
            Bytes::copy_from_slice(text.as_ref().as_bytes()),
        )
    }

    /// Convenience constructor for JSON-serializable payloads.
    ///
    /// # Errors
    /// Returns `serde_json::Error` if serialization fails.
    pub fn with_json<T: serde::Serialize>(
        room: impl Into<String>,
        event: impl Into<String>,
        dst: impl Into<String>,
        src: impl Into<String>,
        value: &T,
    ) -> Result<Self, serde_json::Error> {
        let json_bytes = serde_json::to_vec(value)?;
        Ok(Self::new(room, event, dst, src, Bytes::from(json_bytes)))
    }

    /// Accesses the payload as a UTF-8 string slice without heap allocations.
    ///
    /// # Errors
    /// Returns `Utf8Error` if the payload contains invalid UTF-8 sequences.
    pub fn payload_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.payload)
    }

    /// Deserializes the payload from JSON into type `T`.
    ///
    /// # Errors
    /// Returns `serde_json::Error` if parsing fails.
    pub fn payload_json<'a, T: serde::Deserialize<'a>>(&'a self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.payload)
    }

    /// Decodes raw binary bytes into a `Message` with strict error reporting.
    ///
    /// # Errors
    /// Returns `FrameError` on underflows, invalid UTF-8 fields, or trailing bytes.
    pub fn decode_strict(data: Bytes) -> Result<Self, FrameError> {
        Self::decode_strict_with_limit(data, 0)
    }

    /// Decodes raw binary bytes into a `Message` enforcing an optional maximum frame limit.
    ///
    /// # Errors
    /// Returns `FrameError` on size limit exceeded, underflows, invalid UTF-8 fields, or trailing bytes.
    pub fn decode_strict_with_limit(mut data: Bytes, max_size: usize) -> Result<Self, FrameError> {
        if data.len() < HEADER_OVERHEAD {
            return Err(FrameError::BufferUnderflow {
                expected: HEADER_OVERHEAD,
                actual: data.len(),
            });
        }

        if max_size > 0 && data.len() > max_size {
            return Err(FrameError::TruncatedPayload {
                expected: max_size,
                actual: data.len(),
            });
        }

        // 1. Version & Flags
        let version = data.get_u8();
        let flags = data.get_u8();

        // 2. Room (2B length)
        let room_len = data.get_u16() as usize;
        if data.remaining() < room_len {
            return Err(FrameError::TruncatedPayload {
                expected: room_len,
                actual: data.remaining(),
            });
        }
        let room_bytes = data.split_to(room_len);
        let room = std::str::from_utf8(&room_bytes)
            .map_err(|_| FrameError::InvalidUtf8 { field: "room" })?
            .to_string();

        // 3. Event (2B length)
        if data.remaining() < 2 {
            return Err(FrameError::BufferUnderflow {
                expected: 2,
                actual: data.remaining(),
            });
        }
        let event_len = data.get_u16() as usize;
        if data.remaining() < event_len {
            return Err(FrameError::TruncatedPayload {
                expected: event_len,
                actual: data.remaining(),
            });
        }
        let event_bytes = data.split_to(event_len);
        let event = std::str::from_utf8(&event_bytes)
            .map_err(|_| FrameError::InvalidUtf8 { field: "event" })?
            .to_string();

        // 4. Dst (1B length)
        if data.remaining() < 1 {
            return Err(FrameError::BufferUnderflow {
                expected: 1,
                actual: data.remaining(),
            });
        }
        let dst_len = data.get_u8() as usize;
        if data.remaining() < dst_len {
            return Err(FrameError::TruncatedPayload {
                expected: dst_len,
                actual: data.remaining(),
            });
        }
        let dst_bytes = data.split_to(dst_len);
        let dst = std::str::from_utf8(&dst_bytes)
            .map_err(|_| FrameError::InvalidUtf8 { field: "dst" })?
            .to_string();

        // 5. Src (1B length)
        if data.remaining() < 1 {
            return Err(FrameError::BufferUnderflow {
                expected: 1,
                actual: data.remaining(),
            });
        }
        let src_len = data.get_u8() as usize;
        if data.remaining() < src_len {
            return Err(FrameError::TruncatedPayload {
                expected: src_len,
                actual: data.remaining(),
            });
        }
        let src_bytes = data.split_to(src_len);
        let src = std::str::from_utf8(&src_bytes)
            .map_err(|_| FrameError::InvalidUtf8 { field: "src" })?
            .to_string();

        // 6. Payload (4B length)
        if data.remaining() < 4 {
            return Err(FrameError::BufferUnderflow {
                expected: 4,
                actual: data.remaining(),
            });
        }
        let payload_len = data.get_u32() as usize;
        if data.remaining() < payload_len {
            return Err(FrameError::TruncatedPayload {
                expected: payload_len,
                actual: data.remaining(),
            });
        }
        let payload = data.split_to(payload_len);

        if data.has_remaining() {
            return Err(FrameError::TrailingBytes {
                remaining: data.remaining(),
            });
        }

        Ok(Self {
            version,
            flags,
            room,
            event,
            dst,
            src,
            payload,
        })
    }

    /// Decodes raw binary bytes into a `Message`. Returns `None` on any malformed input.
    pub fn decode(data: Bytes) -> Option<Self> {
        Self::decode_strict(data).ok()
    }

    /// Serializes the `Message` into a contiguous `Bytes` buffer using the 12-byte wire format.
    pub fn encode(&self) -> Bytes {
        let room_bytes = self.room.as_bytes();
        let event_bytes = self.event.as_bytes();
        let dst_bytes = self.dst.as_bytes();
        let src_bytes = self.src.as_bytes();

        let total_size = HEADER_OVERHEAD
            + room_bytes.len()
            + event_bytes.len()
            + dst_bytes.len()
            + src_bytes.len()
            + self.payload.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u8(self.version);
        buf.put_u8(self.flags);

        buf.put_u16(room_bytes.len() as u16);
        buf.put_slice(room_bytes);

        buf.put_u16(event_bytes.len() as u16);
        buf.put_slice(event_bytes);

        buf.put_u8(dst_bytes.len() as u8);
        buf.put_slice(dst_bytes);

        buf.put_u8(src_bytes.len() as u8);
        buf.put_slice(src_bytes);

        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);

        buf.freeze()
    }
}
