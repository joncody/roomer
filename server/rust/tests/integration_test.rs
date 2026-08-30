use axum::{Router, routing::get};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use roomer::{AppState, Hub, Message, ServerConfig, ws_handler};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as TungsteniteMsg;

async fn recv_msg<S>(stream: &mut S) -> Message
where
    S: StreamExt<Item = Result<TungsteniteMsg, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let frame = timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("timed out waiting for message")
            .expect("stream ended")
            .expect("websocket error");

        match frame {
            TungsteniteMsg::Binary(bin) => {
                // `bin` is already `Bytes` in tokio-tungstenite 0.26+
                return Message::decode(bin).expect("valid binary message frame");
            }
            TungsteniteMsg::Ping(_) | TungsteniteMsg::Pong(_) => {
                continue;
            }
            other => panic!("expected binary message frame, received {other:?}"),
        }
    }
}

#[tokio::test]
async fn test_full_websocket_lifecycle_over_tcp() {
    let hub = Hub::new();
    let state = AppState::new(hub.clone()).with_config(ServerConfig::default());
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ws_url = format!("ws://{addr}/ws");

    // 1. Client 1 connects and receives "root" join_ack
    let (mut client1, _) = connect_async(&ws_url).await.expect("Client 1 connects");
    let c1_root_ack = recv_msg(&mut client1).await;
    assert_eq!(c1_root_ack.room, "root");
    assert_eq!(c1_root_ack.event, "join_ack");
    let c1_id = c1_root_ack.src;
    assert!(!c1_id.is_empty());

    // 2. Client 2 connects and receives "root" join_ack
    let (mut client2, _) = connect_async(&ws_url).await.expect("Client 2 connects");
    let c2_root_ack = recv_msg(&mut client2).await;
    assert_eq!(c2_root_ack.room, "root");
    assert_eq!(c2_root_ack.event, "join_ack");
    let c2_id = c2_root_ack.src;
    assert!(!c2_id.is_empty());

    // 3. Client 1 receives "new_member" on "root" channel notifying that Client 2 joined root
    let c1_root_new_member = recv_msg(&mut client1).await;
    assert_eq!(c1_root_new_member.room, "root");
    assert_eq!(c1_root_new_member.event, "new_member");
    assert_eq!(c1_root_new_member.payload_str().unwrap(), c2_id);

    // 4. Client 1 joins "game" room (pass `Bytes` directly with zero-copy)
    let join_game = Message::new("game", "join", "", "", Bytes::new());
    client1
        .send(TungsteniteMsg::Binary(join_game.encode()))
        .await
        .unwrap();

    let c1_game_ack = recv_msg(&mut client1).await;
    assert_eq!(c1_game_ack.room, "game");
    assert_eq!(c1_game_ack.event, "join_ack");

    // 5. Client 2 joins "game" room
    let join_game_c2 = Message::new("game", "join", "", "", Bytes::new());
    client2
        .send(TungsteniteMsg::Binary(join_game_c2.encode()))
        .await
        .unwrap();

    let c2_game_ack = recv_msg(&mut client2).await;
    assert_eq!(c2_game_ack.room, "game");
    assert_eq!(c2_game_ack.event, "join_ack");

    // 6. Client 1 receives "new_member" notification for "game" room
    let c1_game_new_member = recv_msg(&mut client1).await;
    assert_eq!(c1_game_new_member.room, "game");
    assert_eq!(c1_game_new_member.event, "new_member");
    assert_eq!(c1_game_new_member.payload_str().unwrap(), c2_id);

    // 7. Client 2 sends a "chat" broadcast to "game"
    let chat = Message::new("game", "chat", "", "", Bytes::from_static(b"Hello Game!"));
    client2
        .send(TungsteniteMsg::Binary(chat.encode()))
        .await
        .unwrap();

    let c1_chat = recv_msg(&mut client1).await;
    assert_eq!(c1_chat.room, "game");
    assert_eq!(c1_chat.event, "chat");
    assert_eq!(c1_chat.src, c2_id);
    assert_eq!(c1_chat.payload_str().unwrap(), "Hello Game!");

    // 8. Client 2 leaves "game" room
    let leave_game = Message::new("game", "leave", "", "", Bytes::new());
    client2
        .send(TungsteniteMsg::Binary(leave_game.encode()))
        .await
        .unwrap();

    let c2_leave_ack = recv_msg(&mut client2).await;
    assert_eq!(c2_leave_ack.room, "game");
    assert_eq!(c2_leave_ack.event, "leave_ack");

    let c1_member_left = recv_msg(&mut client1).await;
    assert_eq!(c1_member_left.room, "game");
    assert_eq!(c1_member_left.event, "member_left");
    assert_eq!(c1_member_left.payload_str().unwrap(), c2_id);

    // 9. Graceful Hub Shutdown sends WebSocket 1001 Close frame to all active connections
    hub.shutdown().await.unwrap();

    loop {
        let frame = timeout(Duration::from_secs(3), client1.next())
            .await
            .expect("timed out waiting for close frame")
            .expect("stream ended")
            .expect("websocket error");

        match frame {
            TungsteniteMsg::Close(Some(close)) => {
                assert_eq!(close.code, 1001.into());
                break;
            }
            TungsteniteMsg::Ping(_) | TungsteniteMsg::Pong(_) => continue,
            other => panic!("expected close frame 1001, received {other:?}"),
        }
    }
}
