use crate::conn::{BackpressureStrategy, Conn, OutboundMessage};
use crate::error::AuthError;
use crate::hub::Hub;
use crate::message::Message;
use axum::{
    extract::{
        State, WebSocketUpgrade,
        ws::{self, WebSocket},
    },
    http::{HeaderMap, Uri},
    response::IntoResponse,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;
use uuid::Uuid;

/// Server configuration options for WebSocket lifecycle and buffering.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Maximum allowed binary frame size in bytes.
    pub max_message_size: usize,
    /// Heartbeat ping transmission interval.
    pub ping_interval: Duration,
    /// Timeout duration waiting for client pong reply.
    pub pong_timeout: Duration,
    /// In-memory outbound message channel buffer capacity.
    pub channel_capacity: usize,
    /// Buffer saturation policy.
    pub backpressure: BackpressureStrategy,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024,
            ping_interval: Duration::from_secs(54),
            pong_timeout: Duration::from_secs(60),
            channel_capacity: 2048,
            backpressure: BackpressureStrategy::default(),
        }
    }
}

impl ServerConfig {
    /// Creates a new `ServerConfig` with default parameters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets maximum message payload size.
    #[must_use]
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Sets heartbeat ping interval.
    #[must_use]
    pub fn with_ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval;
        self
    }

    /// Sets pong timeout duration.
    #[must_use]
    pub fn with_pong_timeout(mut self, timeout: Duration) -> Self {
        self.pong_timeout = timeout;
        self
    }

    /// Sets connection send channel buffer capacity.
    #[must_use]
    pub fn with_channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    /// Sets the buffer saturation policy.
    #[must_use]
    pub fn with_backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.backpressure = strategy;
        self
    }
}

/// Authorization callback extracting claims from headers and URI during WebSocket upgrade.
pub type AuthorizeFn =
    Arc<dyn Fn(&HeaderMap, &Uri) -> Result<HashMap<String, String>, AuthError> + Send + Sync>;

/// Authorization callback validating room subscription permissions.
pub type RoomAuthFn = Arc<dyn Fn(&Conn, &str) -> bool + Send + Sync>;

/// Axum shared state container for the WebSocket handler.
#[derive(Clone)]
pub struct AppState {
    /// Central Hub instance.
    pub hub: Arc<Hub>,
    /// Server configuration parameters.
    pub config: ServerConfig,
    /// Optional connection upgrade authorization validator.
    pub auth: Option<AuthorizeFn>,
    /// Optional room subscription authorization validator.
    pub room_auth: Option<RoomAuthFn>,
}

impl AppState {
    /// Creates a new `AppState` with the given `Hub`.
    pub fn new(hub: Arc<Hub>) -> Self {
        Self {
            hub,
            config: ServerConfig::default(),
            auth: None,
            room_auth: None,
        }
    }

    /// Configures custom server settings.
    pub fn with_config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    /// Attaches an upgrade authorization function.
    pub fn with_auth(mut self, auth: AuthorizeFn) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Attaches a room subscription authorization function.
    pub fn with_room_auth(mut self, auth: RoomAuthFn) -> Self {
        self.room_auth = Some(auth);
        self
    }
}

/// Axum HTTP WebSocket upgrade handler.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    uri: Uri,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut claims = HashMap::new();
    if let Some(ref auth_fn) = state.auth {
        match auth_fn(&headers, &uri) {
            Ok(c) => claims = c,
            Err(e) => {
                warn!("Unauthorized WebSocket handshake: {}", e);
                return axum::http::StatusCode::UNAUTHORIZED.into_response();
            }
        }
    }

    ws.max_message_size(state.config.max_message_size)
        .on_upgrade(move |socket| handle_socket(socket, state, claims))
}

async fn handle_socket(socket: WebSocket, state: AppState, claims: HashMap<String, String>) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (send_tx, mut send_rx) = mpsc::channel::<OutboundMessage>(state.config.channel_capacity);

    let conn_id = Uuid::new_v4().to_string();
    let conn = Conn::with_backpressure(
        conn_id,
        claims,
        send_tx,
        state.hub.metrics(),
        state.config.backpressure,
    );

    state.hub.add_conn(conn.clone());
    state.hub.join_room("root", conn.clone());

    // Send join_ack for default "root" room using cluster presence snapshot
    let snap = state.hub.get_cluster_presence("root").await;
    let snap_json = serde_json::to_vec(&snap).unwrap_or_else(|_| b"[]".to_vec());
    let ack = Message::new("root", "join_ack", "", &conn.id, Bytes::from(snap_json));
    conn.try_send(ack.encode());

    let ping_interval = state.config.ping_interval;
    let pong_timeout = state.config.pong_timeout;
    let start_instant = tokio::time::Instant::now();
    let last_activity_ms = Arc::new(AtomicU64::new(0));

    let last_activity_writer = last_activity_ms.clone();
    let conn_id_writer = conn.id.clone();

    // High-performance coalescing WebSocket writer loop with biased branch priority
    let writer_task = tokio::spawn(async move {
        let check_interval = Duration::from_secs(1).min(ping_interval);
        let mut ticker = tokio::time::interval(check_interval);
        let mut last_ping = tokio::time::Instant::now();

        loop {
            tokio::select! {
                biased;

                Some(outbound) = send_rx.recv() => {
                    let mut current = outbound;
                    let mut count = 0;

                    loop {
                        match current {
                            OutboundMessage::Binary(msg_bytes) => {
                                // Feed into buffer without triggering immediate TCP flush syscall
                                if ws_sender.feed(ws::Message::Binary(msg_bytes)).await.is_err() {
                                    return;
                                }
                            }
                            OutboundMessage::Close(code, reason) => {
                                let _ = ws_sender.send(ws::Message::Close(Some(ws::CloseFrame {
                                    code,
                                    reason: reason.into(),
                                }))).await;
                                return;
                            }
                        }

                        count += 1;
                        if count >= 128 {
                            break;
                        }

                        // Opportunistically batch queued burst frames before flushing TCP socket
                        match send_rx.try_recv() {
                            Ok(next_msg) => current = next_msg,
                            Err(_) => break,
                        }
                    }

                    // Single coalesced TCP flush for the entire batch
                    if ws_sender.flush().await.is_err() {
                        return;
                    }
                }
                _ = ticker.tick() => {
                    let now = tokio::time::Instant::now();
                    let last_act = last_activity_writer.load(Ordering::Relaxed);
                    let elapsed_since_act = now.duration_since(start_instant + Duration::from_millis(last_act));

                    if elapsed_since_act > pong_timeout {
                        warn!(conn_id = %conn_id_writer, "Connection heartbeat timed out");
                        let _ = ws_sender.send(ws::Message::Close(Some(ws::CloseFrame {
                            code: 1000,
                            reason: "Heartbeat timeout".into(),
                        }))).await;
                        return;
                    }

                    if now.duration_since(last_ping) >= ping_interval {
                        last_ping = now;
                        if ws_sender.send(ws::Message::Ping(Bytes::new())).await.is_err() {
                            return;
                        }
                    }
                }
                else => break,
            }
        }
    });

    // Axum 0.8.9 zero-copy reader loop
    let hub = state.hub.clone();
    let conn_for_reader = conn.clone();
    let last_activity_reader = last_activity_ms.clone();
    let room_auth_checker = state.room_auth.clone();

    let reader_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_receiver.next().await {
            match msg {
                ws::Message::Binary(bin) => {
                    let elapsed = tokio::time::Instant::now()
                        .duration_since(start_instant)
                        .as_millis() as u64;
                    last_activity_reader.store(elapsed, Ordering::Relaxed);

                    conn_for_reader.metrics.on_message_received(bin.len());
                    if let Some(packet) = Message::decode(bin) {
                        if packet.event == "join" {
                            if let Some(ref auth) = room_auth_checker {
                                if !auth(&conn_for_reader, &packet.room) {
                                    continue;
                                }
                            }
                        }
                        hub.dispatch(conn_for_reader.clone(), packet).await;
                    }
                }
                ws::Message::Close(_) => break,
                ws::Message::Ping(_) | ws::Message::Pong(_) => {
                    let elapsed = tokio::time::Instant::now()
                        .duration_since(start_instant)
                        .as_millis() as u64;
                    last_activity_reader.store(elapsed, Ordering::Relaxed);
                }
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = writer_task => {},
        _ = reader_task => {},
    }

    state.hub.leave_all_rooms(&conn);
    state.hub.remove_conn(&conn.id);
}
