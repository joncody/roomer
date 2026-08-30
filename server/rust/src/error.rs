use thiserror::Error;

/// Primary unified error type for the `roomer` WebSocket framework.
#[derive(Error, Debug)]
pub enum RoomerError {
    /// Errors related to binary framing, encoding, or decoding.
    #[error("Frame error: {0}")]
    Frame(#[from] FrameError),

    /// Errors related to handler registration or dispatch.
    #[error("Handler error: {0}")]
    Handler(#[from] HandlerError),

    /// Errors related to distributed cluster adapters (e.g., Redis).
    #[error("Adapter error: {0}")]
    Adapter(#[from] AdapterError),

    /// Errors occurring during HTTP upgrade handshake authorization.
    #[error("Authentication error: {0}")]
    Auth(#[from] AuthError),

    /// JSON serialization or deserialization failures.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Standard I/O failures.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors encountered when decoding or validating binary message frames.
#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum FrameError {
    /// Provided byte buffer was too small to contain header metadata.
    #[error("Buffer underflow: expected at least {expected} bytes, found {actual}")]
    BufferUnderflow {
        /// Expected minimum byte count.
        expected: usize,
        /// Actual byte count present.
        actual: usize,
    },

    /// UTF-8 string decoding failed for a specific packet header.
    #[error("Field '{field}' contains invalid UTF-8 bytes")]
    InvalidUtf8 {
        /// The name of the invalid packet field.
        field: &'static str,
    },

    /// Frame contains extraneous trailing bytes after all fields were read.
    #[error("Unexpected trailing bytes remaining in frame: {remaining} bytes")]
    TrailingBytes {
        /// Number of unconsumed trailing bytes.
        remaining: usize,
    },

    /// Length prefix claims more bytes than exist in the remainder of the buffer.
    #[error("Payload truncated: header specified {expected} bytes, but only {actual} bytes remain")]
    TruncatedPayload {
        /// Expected payload length.
        expected: usize,
        /// Remaining buffer length.
        actual: usize,
    },
}

/// Errors encountered when managing message handlers.
#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum HandlerError {
    /// Attempted to register a handler for a protocol-reserved event name.
    #[error("Cannot register handler for reserved event: '{0}'")]
    ReservedEvent(String),

    /// Handler registration failed because an event handler is already registered.
    #[error("Handler already registered for event: '{0}'")]
    DuplicateHandler(String),

    /// Custom handler execution failed.
    #[error("Handler execution failed: {0}")]
    Execution(String),
}

/// Errors related to distributed cluster adapters and transport.
#[derive(Error, Debug)]
pub enum AdapterError {
    /// Connection to the distributed broker could not be established.
    #[error("Cluster backend connection failed: {0}")]
    ConnectionFailed(String),

    /// Publishing a cluster message timed out.
    #[error("Publish operation timed out")]
    PublishTimeout,

    /// Publishing a cluster message failed.
    #[error("Publish operation failed: {0}")]
    PublishFailed(String),

    /// Subscribing to cluster channels failed.
    #[error("Subscription failed: {0}")]
    SubscribeFailed(String),

    /// Node envelope corrupted or malformed.
    #[error("Envelope decoding failed: malformed node envelope")]
    InvalidEnvelope,

    /// Underlying Redis client error.
    #[cfg(feature = "redis-adapter")]
    #[error("Redis error: {0}")]
    Redis(#[from] ::redis::RedisError),
}

/// Errors occurring during WebSocket connection authorization.
#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum AuthError {
    /// Authorization header was missing.
    #[error("Missing Authorization header")]
    MissingHeader,

    /// Authorization header value is malformed (e.g. not a Bearer token).
    #[error("Invalid Authorization format: expected Bearer token")]
    InvalidFormat,

    /// Supplied token was expired, corrupted, or rejected.
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
}
