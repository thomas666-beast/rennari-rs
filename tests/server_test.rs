use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use rennari::protocol::{Frame, Serializer, decode, encode};
use rennari::server::{Handler, Options, Server};

async fn start_server(
    options: Options,
    setup: impl FnOnce(Server) -> Server + Send + 'static,
) -> (Server, String) {
    let server = setup(Server::new(options));
    let app = server.clone_router();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (server, format!("ws://{}/ws", addr))
}

#[tokio::test]
async fn echo_round_trip() {
    let echo: Handler = Arc::new(|ctx, p| {
        let ctx = ctx.clone();
        let payload = p.clone();
        tokio::spawn(async move {
            ctx.send_to_self("echo_back", &payload).await;
        });
        None
    });

    let (srv, url) = start_server(
        Options {
            mock: true,
            ..Default::default()
        },
        move |s| s.on("echo", echo),
    )
    .await;

    let (mut ws, _) = connect_async(&url).await.unwrap();
    let frame = Frame::new("echo", json!({"text": "hi"}));
    let bytes = encode(&frame, Serializer::Msgpack).unwrap();
    ws.send(Message::Binary(bytes)).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut got = false;
    loop {
        let msg = match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        if let Message::Binary(b) = msg {
            let f = decode(&b, Serializer::Msgpack, 65536).unwrap();
            if f.event == "echo_back" {
                assert_eq!(f.data["text"], "hi");
                got = true;
                break;
            }
        }
    }
    assert!(got, "never received echo_back");

    srv.shutdown().await;
}
