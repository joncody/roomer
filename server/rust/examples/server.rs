use axum::{Router, routing::get};
use bytes::Bytes;
use roomer::{AppState, Hub, Message, ServerConfig, ws_handler};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tower_http::services::{ServeDir, ServeFile};
use tracing::{debug, info};
#[cfg(feature = "redis-adapter")]
use tracing::warn;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(feature = "redis-adapter")]
use roomer::{InMemoryMetrics, RedisAdapter};

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

    // 1. Connect Redis Adapter if feature is enabled and REDIS_URL/REDIS_ADDR is provided
    #[cfg(feature = "redis-adapter")]
    {
        let mut clustered = false;
        if let Ok(redis_url) = std::env::var("REDIS_URL").or_else(|_| std::env::var("REDIS_ADDR")) {
            let mut formatted_url = redis_url.clone();
            if !formatted_url.starts_with("redis://") && !formatted_url.starts_with("rediss://") {
                formatted_url = format!("redis://{}", formatted_url);
            }
            let prefix = std::env::var("REDIS_PREFIX").unwrap_or_else(|_| "roomer:demo:".into());
            let metrics = Arc::new(InMemoryMetrics::new());

            info!("Connecting to Redis cluster at {}", formatted_url);
            match RedisAdapter::builder(&formatted_url)
                .prefix(&prefix)
                .presence_ttl(std::time::Duration::from_secs(86400))
                .build()
            {
                Ok(adapter) => {
                    hub.configure(Arc::new(adapter), metrics).await;
                    info!("Configured Redis cluster adapter with SET presence");
                    clustered = true;
                }
                Err(err) => {
                    warn!(error = %err, "Could not connect to Redis; running in standalone mode");
                }
            }
        }

        if !clustered {
            info!("Running in standalone single-node mode");
        }
    }

    #[cfg(not(feature = "redis-adapter"))]
    info!("Running in standalone single-node mode");

    // 2. Register "chat" broadcast handler
    let hub_chat = hub.clone();
    hub.register_handler(
        "chat",
        Arc::new(move |conn, msg| {
            let hub = hub_chat.clone();
            Box::pin(async move {
                debug!(
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

    // 3. Register "ping" handler
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

    // 4. Configure AppState with 12-byte wire framing, channel buffer, and rate limits
    let state = AppState::new(hub.clone()).with_config(
        ServerConfig::default()
            .with_channel_capacity(8192)
            .with_max_message_size(16 * 1024 * 1024)
            .with_control_rate_limit(10.0, 20),
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

    info!("Roomer server listening on http://localhost:{}", port);
    info!("Interactive Demo:    http://localhost:{}/", port);
    info!("Browser Test Suite:  http://localhost:{}/tests/", port);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down server gracefully (broadcasting 1001 close frames)...");
            let _ = hub.shutdown().await;
        })
        .await?;

    Ok(())
}
