use crate::error::FrameError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// High-performance binary message packet framing.
///
/// Binary wire format:
/// `[4B room_len][room][4B event_len][event][4B dst_len][dst][4B src_len][src][4B payload_len][payload]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
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
    /// Creates a new `Message` instance.
    ///
    /// # Example
    /// ```rust
    /// use roomer::Message;
    /// use bytes::Bytes;
    ///
    /// let msg = Message::new("lobby", "chat", "", "user_1", Bytes::from_static(b"hello"));
    /// assert_eq!(msg.room, "lobby");
    /// ```
    pub fn new(
        room: impl Into<String>,
        event: impl Into<String>,
        dst: impl Into<String>,
        src: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
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
    pub fn decode_strict(mut data: Bytes) -> Result<Self, FrameError> {
        if data.len() < 20 {
            return Err(FrameError::BufferUnderflow {
                expected: 20,
                actual: data.len(),
            });
        }

        // 1. Room
        let room_len = data.get_u32() as usize;
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

        // 2. Event
        if data.remaining() < 4 {
            return Err(FrameError::BufferUnderflow {
                expected: 4,
                actual: data.remaining(),
            });
        }
        let event_len = data.get_u32() as usize;
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

        // 3. Dst
        if data.remaining() < 4 {
            return Err(FrameError::BufferUnderflow {
                expected: 4,
                actual: data.remaining(),
            });
        }
        let dst_len = data.get_u32() as usize;
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

        // 4. Src
        if data.remaining() < 4 {
            return Err(FrameError::BufferUnderflow {
                expected: 4,
                actual: data.remaining(),
            });
        }
        let src_len = data.get_u32() as usize;
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

        // 5. Payload (Zero-copy slice split directly from Bytes buffer)
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

    /// Serializes the `Message` into a single-pass, pre-allocated contiguous `Bytes` buffer.
    pub fn encode(&self) -> Bytes {
        let room_bytes = self.room.as_bytes();
        let event_bytes = self.event.as_bytes();
        let dst_bytes = self.dst.as_bytes();
        let src_bytes = self.src.as_bytes();

        let total_size = 20
            + room_bytes.len()
            + event_bytes.len()
            + dst_bytes.len()
            + src_bytes.len()
            + self.payload.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32(room_bytes.len() as u32);
        buf.put_slice(room_bytes);

        buf.put_u32(event_bytes.len() as u32);
        buf.put_slice(event_bytes);

        buf.put_u32(dst_bytes.len() as u32);
        buf.put_slice(dst_bytes);

        buf.put_u32(src_bytes.len() as u32);
        buf.put_slice(src_bytes);

        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);

        buf.freeze()
    }
}
