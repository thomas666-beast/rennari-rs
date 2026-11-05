Rennari Rust
============

Rust implementation of the Rennari protocol: a small, secure, MessagePack-
or JSON-framed WebSocket event layer with custom events, rooms,
namespaces, acknowledgements, and connection state recovery.

Wire-compatible with Mojo::Rennari::Server (Perl), rennari-server-js
(Node), rennari-go, rennari-py, and rennari-client-js.

Client and server live in one crate, as is idiomatic for Rust.

STATUS

Working. Client and server tested against each other.

INSTALL

Add to Cargo.toml:

    [dependencies]
    rennari = "0.1"

QUICK START

Server:

    use std::sync::Arc;
    use rennari::server::{Handler, Options, Server};
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let server = Server::new(Options { mock: true, ..Default::default() });

        let chat: Handler = Arc::new(|ctx, p| {
            let ctx = ctx.clone();
            let payload = p.clone();
            tokio::spawn(async move {
                let room = payload["room"].as_str().unwrap_or("").to_string();
                ctx.join(&room).await;
                ctx.broadcast_to_room(&room, "chat", &payload).await;
            });
            None
        });

        let server = server.on("chat", chat);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
        axum::serve(listener, server.router()).await?;
        Ok(())
    }

Client:

    use std::sync::Arc;
    use rennari::client::{Client, ClientHandler, ClientOptions};
    use serde_json::json;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut c = Client::new("ws://127.0.0.1:3000/ws", ClientOptions::default());
        let chat: ClientHandler = Arc::new(|p| println!("chat: {}", p));
        c.on("chat", chat).await;
        c.connect().await?;
        c.emit("chat", json!({"room": "lobby", "text": "hi"})).await?;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        c.disconnect().await;
        Ok(())
    }

CORE CONCEPTS

Frames

Every message is a map:

    { e, d, id?, o?, n? }

- e - event name, [A-Za-z0-9_.:-]{1,64}
- d - payload
- id - optional acknowledgement id
- o - optional offset, present when recovery is enabled
- n - optional namespace, omitted for /

Serialized as MessagePack by default, or JSON if configured. Binary
WebSocket frames, always.

Modules

- protocol - Frame, encode, decode, validation
- server   - Server, Context, Handler, Options
- client   - Client, ClientHandler, ClientOptions

Server: authentication

auth_cb receives an AuthInfo with path, parsed query, and headers.
Return Some(identity) to accept, None to reject with 401.

    let auth = Arc::new(|info: &AuthInfo| {
        info.query.get("token").and_then(|t| tokens.get(t).cloned())
    });
    let server = Server::new(Options { auth_cb: Some(auth), ..Default::default() });

Server: events

    let smile: Handler = Arc::new(|ctx, p| Some(json!({"ok": 1})));

If the handler returns Some(value), it is sent back as
{e: 'reply', d: value}.

Handlers are synchronous. To do async work, clone the Context and spawn:

    let h: Handler = Arc::new(|ctx, p| {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            ctx.send_to_self("pong", &p).await;
        });
        None
    });

Server: sends

Context methods are async:

    ctx.send_to_self("event", &payload).await
    ctx.send_to(id, "event", &payload).await
    ctx.broadcast("event", &payload).await
    ctx.broadcast_to_room("room", "event", &payload).await

Server: rooms

    ctx.join("lobby").await
    ctx.leave("lobby").await
    ctx.members_of("lobby").await   // Vec<i64>
    ctx.rooms().await               // Vec<String>

Rooms are per-namespace. Auto-create on first join, disappear when the
last member leaves. No policy in the framework.

Server: namespaces

    let chat = srv.namespace("/chat");
    chat.on("hello", handler);

A client joins with {e: 'rennari_nsp', d: {name: '/chat'}}. The server
replies rennari_nsp_ok. Frames for a namespace the client has not joined
close the connection with code 4005.

Server: acknowledgements

Client sends id with an event. Server replies {e: 'rennari_ack',
d: {id}} after the handler returns. No ack if the handler panics; the
connection closes with 4004.

Server: lifecycle hooks

    server.on_connect(|ctx, identity| { ... })
    server.on_disconnect(|ctx, identity| { ... })
    server.on_error(|ctx, kind, detail| { ... })
    // kind: bad_frame, unknown_event, handler_error, unknown_namespace

Server: recovery

    Server::new(Options {
        recovery: true,
        recovery_window_secs: 120,
        ..Default::default()
    })

Off by default. When on, the server issues a session id at handshake,
tracks an offset on every outbound frame, and buffers frames for a
connection that has dropped. On reconnect, the client sends
{e: 'rennari_resume', d: {session_id, offset}}. The server replays missed
frames, re-attaches namespaces and rooms, and replies rennari_resumed.

Server: mock mode

    Server::new(Options { mock: true, ..Default::default() })

Skips auth, synthesizes an identity, ignores unknown events. Development
only.

CLOSE CODES

- 4002 - bad frame
- 4003 - unknown event
- 4004 - handler error
- 4005 - unknown namespace
- 4006 - recovery disabled but client sent rennari_resume

CLIENT

    let mut c = Client::new(url, ClientOptions {
        serializer: Serializer::Msgpack,
        max_bytes: 64 * 1024,
        ack_timeout: Duration::from_secs(5),
        retries: 3,
        extra_headers: vec![],
    });

    c.on("event", handler).await;
    c.on_namespace("/chat", "message", handler).await;
    c.connect().await?;
    c.emit("event", payload).await?;
    c.emit_namespace("/chat", "message", payload).await?;
    c.emit_with_ack("ping", payload).await?;
    c.disconnect().await;
    c.session_id().await;     // Option<String>
    c.last_offset().await;    // i64

The client declares all non-default namespaces it has registered
immediately after connecting.

SECURITY

- auth_cb runs before the upgrade. Failed auth = 401, no socket opened.
- Frames are validated on decode: event name grammar, id format, offset
  format, namespace grammar, size limit.
- Decoder returns ProtocolError, never panics.
- Serializer is fixed per server or client.
- Mock mode is development-only; there is no production refusal guard
  like the JS and Perl servers have. Do not set mock in production.

The framework does not implement rate limiting. Add per-connection
budgets at the application level or use a reverse proxy.

EXAMPLES

examples/app.rs runs a standalone server. examples/client.rs runs a
client against it.

    cargo run --example app
    cargo run --example client

LICENSE

MIT.
