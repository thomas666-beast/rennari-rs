use std::sync::Arc;
use std::time::Duration;

use rennari::client::{Client, ClientHandler, ClientOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "3000".to_string());
    let url = format!("ws://127.0.0.1:{}/ws?token=alice-token", port);

    let mut c = Client::new(
        url,
        ClientOptions {
            ack_timeout: Duration::from_secs(2),
            retries: 1,
            ..Default::default()
        },
    );

    let chat: ClientHandler = Arc::new(|p| println!("[client] chat {}", p));
    let pong: ClientHandler = Arc::new(|p| println!("[client] pong {}", p));
    let reply: ClientHandler = Arc::new(|p| println!("[client] reply {}", p));
    let hello_back: ClientHandler = Arc::new(|p| println!("[client] hello_back {}", p));

    c.on("chat", chat).await;
    c.on("pong", pong).await;
    c.on("reply", reply).await;
    c.on_namespace("/chat", "hello_back", hello_back).await;

    c.connect().await?;
    println!("[client] connected");

    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("[client] session {:?}", c.session_id().await);

    c.emit("chat", json!({"room": "lobby", "text": "hello from rust"}))
        .await?;
    c.emit_namespace("/chat", "hello", json!({"n": 1})).await?;

    match c.emit_with_ack("ping", json!({"n": 42})).await {
        Ok(ack) => println!("[client] ack {}", ack),
        Err(e) => println!("[client] ack error: {}", e),
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    c.disconnect().await;
    Ok(())
}
