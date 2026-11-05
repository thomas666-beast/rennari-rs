use std::collections::HashMap;
use std::sync::Arc;

use rennari::server::{Handler, Options, Server};
use serde_json::json;

fn tokens() -> HashMap<String, serde_json::Value> {
    let mut m = HashMap::new();
    m.insert(
        "alice-token".to_string(),
        json!({"id": "alice", "userId": 1, "name": "alice"}),
    );
    m.insert(
        "bob-token".to_string(),
        json!({"id": "bob", "userId": 2, "name": "bob"}),
    );
    m
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let table = Arc::new(tokens());

    let server = Server::new(Options {
        recovery: true,
        auth_cb: Some(Arc::new(move |auth| {
            auth.query.get("token").and_then(|t| table.get(t).cloned())
        })),
        ..Default::default()
    })
    .on_connect(|ctx, identity| {
        println!(
            "[server] connect conn={} identity={}",
            ctx.conn_id(),
            identity
        );
    })
    .on_disconnect(|ctx, identity| {
        println!(
            "[server] disconnect conn={} identity={}",
            ctx.conn_id(),
            identity
        );
    });

    let chat: Handler = Arc::new(|ctx, p| {
        let room = p
            .get("room")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let text = p.get("text").cloned().unwrap_or_default();
        let from = ctx.conn_id();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            ctx.join(&room).await;
            ctx.broadcast_to_room(
                &room,
                "chat",
                &json!({"room": room, "from": from, "text": text}),
            )
            .await;
        });
        None
    });

    let ping: Handler = Arc::new(|ctx, p| {
        let ctx = ctx.clone();
        let payload = p.clone();
        tokio::spawn(async move {
            ctx.send_to_self("pong", &json!({"got": payload})).await;
        });
        None
    });

    let whoami: Handler = Arc::new(|_, _| Some(json!({"userId": 1, "name": "alice"})));

    let server = server
        .on("chat", chat)
        .on("ping", ping)
        .on("whoami", whoami);

    server.add_namespace("/chat").await;
    let hello: Handler = Arc::new(|ctx, p| {
        let ctx = ctx.clone();
        let payload = p.clone();
        tokio::spawn(async move {
            ctx.send_to_self("hello_back", &json!({"from": "/chat", "got": payload}))
                .await;
        });
        None
    });
    server.add_handler("/chat", "hello", hello).await;

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("[server] listening on ws://{}/ws", addr);

    axum::serve(listener, server.router()).await?;
    Ok(())
}
