use axum::{Router, routing::get};
use bytes::Bytes;
use roomer::{AppState, Hub, Message, ServerConfig, ws_handler};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let hub = Hub::new();

    // Register "chat" handler
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

    // Register "ping" handler
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

    let state = AppState::new(hub.clone()).with_config(
        ServerConfig::default()
            .with_channel_capacity(2048)
            .with_max_message_size(16 * 1024 * 1024),
    );

    // Resolve static asset paths dynamically across root, subfolder, and container execution
    let client_dir = find_existing_path(&["../../client", "client", "../client"]);
    let static_dir = find_existing_path(&[
        "../../examples/static",
        "examples/static",
        "../examples/static",
        "static",
    ]);
    let tests_dir = find_existing_path(&["../../tests", "tests", "../tests"]);
    let index_file = find_existing_path(&[
        "../../examples/index.html",
        "examples/index.html",
        "../examples/index.html",
    ]);

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

    info!(
        "Roomer standalone server listening on http://localhost:{}",
        port
    );
    info!("Interactive Demo: http://localhost:{}/", port);
    info!("Browser Test Suite: http://localhost:{}/tests/", port);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down server gracefully...");
            let _ = hub.shutdown().await;
        })
        .await?;

    Ok(())
}
