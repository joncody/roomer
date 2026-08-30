use axum::{routing::get, Router};
use bytes::Bytes;
use roomer::{
    ws_handler, AppState, Hub, InMemoryMetrics, Message, RedisAdapter, ServerConfig,
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tower_http::services::{ServeDir, ServeFile};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn find_existing_path(candidates: &[&str]) -> PathBuf {
    for candidate in candidates {
        let p = Path::new(candidate);
        if p.exists() {
            return p.to_path_buf();
        }
    }
    PathBuf::from(candidates[0])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let mut redis_url = std::env::var("REDIS_URL")
        .or_else(|_| std::env::var("REDIS_ADDR"))
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    if !redis_url.starts_with("redis://") && !redis_url.starts_with("rediss://") {
        redis_url = format!("redis://{}", redis_url);
    }
    let prefix = std::env::var("REDIS_PREFIX").unwrap_or_else(|_| "roomer:demo:".into());

    let hub = Hub::new();
    let metrics = Arc::new(InMemoryMetrics::new());

    // 1. Initialize Redis Adapter with loopback suppression
    info!("Connecting to Redis cluster at {}", redis_url);
    let adapter = RedisAdapter::builder(&redis_url)
        .prefix(&prefix)
        .publish_timeout(Duration::from_secs(3))
        .build()?;

    hub.configure(Arc::new(adapter), metrics.clone()).await;
    info!("Multi-node Redis cluster adapter configured successfully");

    // 2. Register custom "chat" handler
    let hub_chat = hub.clone();
    hub.register_handler(
        "chat",
        Arc::new(move |conn, msg| {
            let hub = hub_chat.clone();
            Box::pin(async move {
                info!(
                    room = %msg.room,
                    sender = %msg.src,
                    bytes = msg.payload.len(),
                    "Chat message received"
                );
                hub.broadcast_room(Some(&conn.id), msg);
                Ok(())
            })
        }),
    )?;

    // 3. Register custom "ping" handler
    hub.register_handler(
        "ping",
        Arc::new(|conn, _msg| {
            Box::pin(async move {
                let reply = Message::new("util", "pong", "", &conn.id, Bytes::new());
                conn.try_send(reply.encode());
                Ok(())
            })
        }),
    )?;

    // 4. Configure AppState with 2048 channel capacity for high-density bursts
    let state = AppState::new(hub.clone()).with_config(
        ServerConfig::default()
            .with_max_message_size(16 * 1024 * 1024)
            .with_channel_capacity(2048),
    );

    // Resolve static asset paths dynamically across root, subfolder, and container execution
    let client_dir = find_existing_path(&["../../client", "client", "../client"]);
    let static_dir = find_existing_path(&["../../examples/static", "examples/static", "../examples/static", "static"]);
    let tests_dir = find_existing_path(&["../../tests", "tests", "../tests"]);
    let index_file = find_existing_path(&["../../examples/index.html", "examples/index.html", "../examples/index.html"]);

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .nest_service("/client", ServeDir::new(client_dir))
        .nest_service("/static", ServeDir::new(static_dir))
        .nest_service(
            "/tests",
            ServeDir::new(tests_dir).append_index_html_on_directories(true),
        )
        .route_service("/", ServeFile::new(index_file))
        .with_state(state);

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!("Roomer cluster node listening on http://localhost:{}", port);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(hub))
        .await?;

    info!("Server shutdown complete");
    Ok(())
}

async fn shutdown_signal(hub: Arc<Hub>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutting down cluster node gracefully (broadcasting 1001 close frames)...");
    let _ = hub.shutdown().await;
}
