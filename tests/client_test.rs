use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::net::TcpListener;

use rennari::client::{Client, ClientHandler, ClientOptions};
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

    let mut c = Client::new(url, ClientOptions::default());
    let got = Arc::new(tokio::sync::Notify::new());
    let got_clone = got.clone();
    let handler: ClientHandler = Arc::new(move |p| {
        assert_eq!(p["text"], "hi");
        got_clone.notify_one();
    });
    c.on("echo_back", handler).await;
    c.connect().await.unwrap();

    c.emit("echo", json!({"text": "hi"})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), got.notified())
        .await
        .unwrap();

    c.disconnect().await;
    srv.shutdown().await;
}

#[tokio::test]
async fn ack_round_trip() {
    let ping: Handler = Arc::new(|_, _| None);
    let (srv, url) = start_server(
        Options {
            mock: true,
            ..Default::default()
        },
        move |s| s.on("ping", ping),
    )
    .await;

    let mut c = Client::new(
        url,
        ClientOptions {
            ack_timeout: Duration::from_secs(2),
            retries: 1,
            ..Default::default()
        },
    );
    c.connect().await.unwrap();

    let ack = c.emit_with_ack("ping", json!({"n": 1})).await.unwrap();
    assert!(ack.get("id").is_some());

    c.disconnect().await;
    srv.shutdown().await;
}

#[tokio::test]
async fn session_id_present() {
    let (srv, url) = start_server(
        Options {
            mock: true,
            recovery: true,
            ..Default::default()
        },
        |s| s,
    )
    .await;
    let mut c = Client::new(url, ClientOptions::default());
    c.connect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(c.session_id().await.is_some());
    c.disconnect().await;
    srv.shutdown().await;
}
