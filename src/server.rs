use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};

use crate::protocol::{decode, encode, Frame, Serializer, DEFAULT_MAX_BYTES};

pub type Handler = Arc<dyn Fn(&Context, Value) -> Option<Value> + Send + Sync>;
pub type AuthCb = Arc<dyn Fn(&AuthInfo) -> Option<Value> + Send + Sync>;
pub type ConnectCb = Arc<dyn Fn(&Context, &Value) + Send + Sync>;
pub type DisconnectCb = Arc<dyn Fn(&Context, &Value) + Send + Sync>;
pub type ErrorCb = Arc<dyn Fn(&Context, &str, &str) + Send + Sync>;

pub struct AuthInfo {
    pub path: String,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>,
}

pub struct Options {
    pub auth_cb: Option<AuthCb>,
    pub serializer: Serializer,
    pub max_bytes: usize,
    pub recovery: bool,
    pub recovery_window_secs: u64,
    pub mock: bool,
}

impl Clone for Options {
    fn clone(&self) -> Self {
        Self {
            auth_cb: self.auth_cb.clone(),
            serializer: self.serializer,
            max_bytes: self.max_bytes,
            recovery: self.recovery,
            recovery_window_secs: self.recovery_window_secs,
            mock: self.mock,
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            auth_cb: None,
            serializer: Serializer::Msgpack,
            max_bytes: DEFAULT_MAX_BYTES,
            recovery: false,
            recovery_window_secs: 120,
            mock: false,
        }
    }
}

pub struct Server {
    pub state: Arc<Mutex<ServerState>>,
    on_connect: Option<ConnectCb>,
    on_disconnect: Option<DisconnectCb>,
    on_error: Option<ErrorCb>,
}

impl Clone for Server {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            on_connect: self.on_connect.clone(),
            on_disconnect: self.on_disconnect.clone(),
            on_error: self.on_error.clone(),
        }
    }
}

pub struct ServerState {
    pub options: Options,
    pub namespaces: HashMap<String, Namespace>,
    pub connections: HashMap<i64, Connection>,
    pub sessions: HashMap<String, Session>,
    pub next_id: i64,
}

pub struct Namespace {
    pub handlers: HashMap<String, Handler>,
    pub rooms: HashMap<String, HashSet<i64>>,
    pub members: HashSet<i64>,
}

pub struct Connection {
    pub identity: Value,
    pub session_id: String,
    pub namespaces: HashSet<String>,
    pub offset: i64,
    pub offline: bool,
    pub outbox: mpsc::UnboundedSender<Vec<u8>>,
}

pub struct Session {
    pub identity: Value,
    pub namespaces: HashSet<String>,
    pub rooms: HashMap<String, Vec<String>>,
    pub offset: i64,
    pub buffered: Vec<(i64, Vec<u8>)>,
    pub conn_id: i64,
    pub offline: bool,
    pub gen: u64,
}

#[derive(Clone)]
pub struct Context {
    pub server: Arc<Mutex<ServerState>>,
    pub conn_id: i64,
    pub namespace: String,
}

impl Server {
    pub fn new(options: Options) -> Self {
        let mut state = ServerState {
            options,
            namespaces: HashMap::new(),
            connections: HashMap::new(),
            sessions: HashMap::new(),
            next_id: 0,
        };
        state.namespaces.insert(
            "/".to_string(),
            Namespace {
                handlers: HashMap::new(),
                rooms: HashMap::new(),
                members: HashSet::new(),
            },
        );
        Self {
            state: Arc::new(Mutex::new(state)),
            on_connect: None,
            on_disconnect: None,
            on_error: None,
        }
    }

    pub fn on(self, event: &str, handler: Handler) -> Self {
        let event = event.to_string();
        let state = self.state.clone();
        let handler_clone = handler.clone();
        tokio::spawn(async move {
            let mut s = state.lock().await;
            if let Some(ns) = s.namespaces.get_mut("/") {
                ns.handlers.insert(event, handler_clone);
            }
        });
        self
    }

    pub fn on_connect<F>(mut self, cb: F) -> Self
    where
        F: Fn(&Context, &Value) + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(cb));
        self
    }

    pub fn on_disconnect<F>(mut self, cb: F) -> Self
    where
        F: Fn(&Context, &Value) + Send + Sync + 'static,
    {
        self.on_disconnect = Some(Arc::new(cb));
        self
    }

    pub fn on_error<F>(mut self, cb: F) -> Self
    where
        F: Fn(&Context, &str, &str) + Send + Sync + 'static,
    {
        self.on_error = Some(Arc::new(cb));
        self
    }

    pub async fn add_namespace(&self, name: &str) {
        let mut s = self.state.lock().await;
        s.namespaces
            .entry(name.to_string())
            .or_insert_with(|| Namespace {
                handlers: HashMap::new(),
                rooms: HashMap::new(),
                members: HashSet::new(),
            });
    }

    pub async fn add_handler(&self, namespace: &str, event: &str, handler: Handler) {
        let mut s = self.state.lock().await;
        let ns = s
            .namespaces
            .entry(namespace.to_string())
            .or_insert_with(|| Namespace {
                handlers: HashMap::new(),
                rooms: HashMap::new(),
                members: HashSet::new(),
            });
        ns.handlers.insert(event.to_string(), handler);
    }

    pub fn clone_router(&self) -> Router {
        let arc = Arc::new(self.clone());
        Router::new().route("/ws", get(ws_handler)).with_state(arc)
    }

    pub fn router(self) -> Router {
        let arc = Arc::new(self);
        Router::new().route("/ws", get(ws_handler)).with_state(arc)
    }

    pub async fn shutdown(&self) {
        let mut s = self.state.lock().await;
        for (_, c) in s.connections.drain() {
            let _ = c.outbox.send(Vec::new());
        }
        s.sessions.clear();
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(server): State<Arc<Server>>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let headers: HashMap<String, String> = req
        .headers()
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();

    let auth = AuthInfo {
        path,
        query: parse_query(&query),
        headers,
    };

    let (mock, auth_cb) = {
        let s = server.state.lock().await;
        (s.options.mock, s.options.auth_cb.clone())
    };

    let identity = if mock {
        Some(serde_json::json!({"id": "mock", "mock": true}))
    } else if let Some(cb) = auth_cb {
        cb(&auth)
    } else {
        None
    };

    let Some(identity) = identity else {
        return axum::response::Response::builder()
            .status(401)
            .body(axum::body::Body::empty())
            .unwrap();
    };

    ws.on_upgrade(move |socket| handle_socket(socket, server, identity))
}

fn parse_query(q: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in q.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

async fn handle_socket(socket: WebSocket, server: Arc<Server>, identity: Value) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let writer = tokio::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            if bytes.is_empty() {
                break;
            }
            if sink.send(Message::Binary(bytes)).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    let conn_id = {
        let mut s = server.state.lock().await;
        s.next_id += 1;
        let id = s.next_id;
        let session_id = random_session_id();
        let recovery = s.options.recovery;
        s.connections.insert(
            id,
            Connection {
                identity: identity.clone(),
                session_id: session_id.clone(),
                namespaces: ["/".to_string()].into_iter().collect(),
                offset: 0,
                offline: false,
                outbox: tx.clone(),
            },
        );
        if recovery {
            s.sessions.insert(
                session_id.clone(),
                Session {
                    identity: identity.clone(),
                    namespaces: ["/".to_string()].into_iter().collect(),
                    rooms: HashMap::new(),
                    offset: 0,
                    buffered: Vec::new(),
                    conn_id: id,
                    offline: false,
                    gen: 0,
                },
            );
        }
        if let Some(ns) = s.namespaces.get_mut("/") {
            ns.members.insert(id);
        }
        id
    };

    {
        let s = server.state.lock().await;
        if s.options.recovery {
            if let Some(conn) = s.connections.get(&conn_id) {
                let session_id = conn.session_id.clone();
                let frame = Frame::new("rennari_session", serde_json::json!({"id": session_id}));
                if let Ok(bytes) = encode(&frame, s.options.serializer) {
                    let _ = conn.outbox.send(bytes);
                }
            }
        }
    }

    if let Some(cb) = &server.on_connect {
        let ctx = Context {
            server: server.state.clone(),
            conn_id,
            namespace: "/".to_string(),
        };
        cb(&ctx, &identity);
    }

    while let Some(msg) = stream.next().await {
        let Ok(msg) = msg else { break };
        match msg {
            Message::Binary(bytes) => {
                on_message(&server, conn_id, &bytes).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    on_close(&server, conn_id).await;

    if let Some(cb) = &server.on_disconnect {
        let ctx = Context {
            server: server.state.clone(),
            conn_id,
            namespace: "/".to_string(),
        };
        cb(&ctx, &identity);
    }

    writer.abort();
}

async fn on_message(server: &Arc<Server>, conn_id: i64, bytes: &[u8]) {
    let (serializer, max_bytes, mock) = {
        let s = server.state.lock().await;
        (s.options.serializer, s.options.max_bytes, s.options.mock)
    };

    let frame = match decode(bytes, serializer, max_bytes) {
        Ok(f) => f,
        Err(_) => {
            close_with(server, conn_id, 4002).await;
            return;
        }
    };

    if frame.event == "rennari_resume" {
        handle_resume(server, conn_id, &frame.data).await;
        return;
    }
    if frame.event == "rennari_nsp" {
        handle_namespace_join(server, conn_id, &frame.data).await;
        return;
    }

    let nsp_name = if frame.namespace.is_empty() {
        "/".to_string()
    } else {
        frame.namespace.clone()
    };

    let (handler_opt, joined) = {
        let s = server.state.lock().await;
        let joined = s
            .connections
            .get(&conn_id)
            .map(|c| c.namespaces.contains(&nsp_name))
            .unwrap_or(false);
        let handler = s
            .namespaces
            .get(&nsp_name)
            .and_then(|n| n.handlers.get(&frame.event).cloned());
        (handler, joined)
    };

    if !joined {
        close_with(server, conn_id, 4005).await;
        return;
    }

    let Some(handler) = handler_opt else {
        if mock {
            return;
        }
        close_with(server, conn_id, 4003).await;
        return;
    };

    let ctx = Context {
        server: server.state.clone(),
        conn_id,
        namespace: nsp_name.clone(),
    };
    let reply = handler(&ctx, frame.data.clone());

    if let Some(r) = reply {
        send_to_connection(server, conn_id, "reply", &r, &nsp_name).await;
    }
    if let Some(id) = frame.id {
        send_to_connection(
            server,
            conn_id,
            "rennari_ack",
            &serde_json::json!({"id": id}),
            "",
        )
        .await;
    }
}

async fn handle_namespace_join(server: &Arc<Server>, conn_id: i64, data: &Value) {
    let name = data
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("/")
        .to_string();
    let mut s = server.state.lock().await;
    if !s.namespaces.contains_key(&name) {
        drop(s);
        close_with(server, conn_id, 4005).await;
        return;
    }
    if let Some(conn) = s.connections.get_mut(&conn_id) {
        conn.namespaces.insert(name.clone());
    }
    if let Some(ns) = s.namespaces.get_mut(&name) {
        ns.members.insert(conn_id);
    }
    let serializer = s.options.serializer;
    if let Some(conn) = s.connections.get(&conn_id) {
        let frame = Frame::new("rennari_nsp_ok", serde_json::json!({"name": name}));
        if let Ok(bytes) = encode(&frame, serializer) {
            let _ = conn.outbox.send(bytes);
        }
    }
}

async fn handle_resume(server: &Arc<Server>, conn_id: i64, data: &Value) {
    let mut s = server.state.lock().await;
    if !s.options.recovery {
        drop(s);
        close_with(server, conn_id, 4006).await;
        return;
    }

    let sid = data
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let offset = data.get("offset").and_then(|v| v.as_i64()).unwrap_or(-1);
    let serializer = s.options.serializer;

    let (namespaces, rooms, buffered, sess_offset, old_conn_id) = match s.sessions.get(&sid) {
        Some(sess) if sess.offline => (
            sess.namespaces.clone(),
            sess.rooms.clone(),
            sess.buffered.clone(),
            sess.offset,
            sess.conn_id,
        ),
        _ => {
            if let Some(conn) = s.connections.get(&conn_id) {
                let frame = Frame::new(
                    "rennari_resume_failed",
                    serde_json::json!({"reason": "unknown_session"}),
                );
                if let Ok(bytes) = encode(&frame, serializer) {
                    let _ = conn.outbox.send(bytes);
                }
            }
            return;
        }
    };

    if let Some(old_sid) = s.connections.get(&conn_id).map(|c| c.session_id.clone()) {
        s.sessions.remove(&old_sid);
    }
    if old_conn_id != conn_id {
        s.connections.remove(&old_conn_id);
    }

    if let Some(conn) = s.connections.get_mut(&conn_id) {
        conn.session_id = sid.clone();
        conn.offset = sess_offset;
        for n in &namespaces {
            conn.namespaces.insert(n.clone());
        }
    }
    for n in &namespaces {
        if let Some(ns) = s.namespaces.get_mut(n) {
            ns.members.insert(conn_id);
        }
    }
    for (n, rs) in &rooms {
        for r in rs {
            if let Some(ns) = s.namespaces.get_mut(n) {
                ns.rooms.entry(r.clone()).or_default().insert(conn_id);
            }
        }
    }

    if let Some(conn) = s.connections.get(&conn_id) {
        for (off, bytes) in &buffered {
            if *off > offset {
                let _ = conn.outbox.send(bytes.clone());
            }
        }
        let frame = Frame::new(
            "rennari_resumed",
            serde_json::json!({"offset": sess_offset, "session_id": sid}),
        );
        if let Ok(bytes) = encode(&frame, serializer) {
            let _ = conn.outbox.send(bytes);
        }
    }

    if let Some(sess) = s.sessions.get_mut(&sid) {
        sess.buffered.clear();
        sess.conn_id = conn_id;
        sess.offline = false;
        sess.gen += 1;
    }
}

async fn on_close(server: &Arc<Server>, conn_id: i64) {
    let mut s = server.state.lock().await;

    let Some(conn) = s.connections.get(&conn_id) else {
        return;
    };
    let namespaces = conn.namespaces.clone();
    let session_id = conn.session_id.clone();

    let mut rooms_by_nsp: HashMap<String, Vec<String>> = HashMap::new();
    for n in &namespaces {
        if let Some(ns) = s.namespaces.get(n) {
            let rooms: Vec<String> = ns
                .rooms
                .iter()
                .filter(|(_, m)| m.contains(&conn_id))
                .map(|(r, _)| r.clone())
                .collect();
            if !rooms.is_empty() {
                rooms_by_nsp.insert(n.clone(), rooms);
            }
        }
    }
    for n in &namespaces {
        if let Some(ns) = s.namespaces.get_mut(n) {
            ns.members.remove(&conn_id);
            for m in ns.rooms.values_mut() {
                m.remove(&conn_id);
            }
            ns.rooms.retain(|_, m| !m.is_empty());
        }
    }

    let recovery = s.options.recovery;
    let window = s.options.recovery_window_secs;
    let server_clone = server.clone();

    if recovery {
        if let Some(sess) = s.sessions.get_mut(&session_id) {
            sess.namespaces = namespaces;
            sess.rooms = rooms_by_nsp;
            sess.conn_id = conn_id;
            sess.offline = true;
            sess.gen += 1;
            let gen = sess.gen;
            let sid = session_id.clone();
            drop(s);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(window)).await;
                let mut s = server_clone.state.lock().await;
                if let Some(cur) = s.sessions.get(&sid) {
                    if cur.offline && cur.gen == gen {
                        let cid = cur.conn_id;
                        s.sessions.remove(&sid);
                        s.connections.remove(&cid);
                    }
                }
            });
            return;
        }
    }
    s.connections.remove(&conn_id);
}

async fn close_with(server: &Arc<Server>, conn_id: i64, _code: u16) {
    let s = server.state.lock().await;
    if let Some(conn) = s.connections.get(&conn_id) {
        let _ = conn.outbox.send(Vec::new());
    }
}

async fn send_to_connection(
    server: &Arc<Server>,
    conn_id: i64,
    event: &str,
    payload: &Value,
    nsp: &str,
) -> bool {
    let mut s = server.state.lock().await;

    let (session_id, offline) = match s.connections.get(&conn_id) {
        Some(c) => (c.session_id.clone(), c.offline),
        None => return false,
    };

    if !offline {
        let recovery = s.options.recovery;
        let serializer = s.options.serializer;
        let offset = if recovery {
            if let Some(conn) = s.connections.get_mut(&conn_id) {
                conn.offset += 1;
                let o = conn.offset;
                if let Some(sess) = s.sessions.get_mut(&session_id) {
                    sess.offset = o;
                }
                Some(o)
            } else {
                None
            }
        } else {
            None
        };

        let frame = Frame {
            event: event.to_string(),
            data: payload.clone(),
            id: None,
            offset,
            namespace: nsp.to_string(),
        };
        let bytes = match encode(&frame, serializer) {
            Ok(b) => b,
            Err(_) => return false,
        };
        if let Some(conn) = s.connections.get(&conn_id) {
            return conn.outbox.send(bytes).is_ok();
        }
        return false;
    }

    if !s.options.recovery {
        return false;
    }
    let serializer = s.options.serializer;
    if let Some(sess) = s.sessions.get_mut(&session_id) {
        sess.offset += 1;
        let off = sess.offset;
        let frame = Frame {
            event: event.to_string(),
            data: payload.clone(),
            id: None,
            offset: Some(off),
            namespace: nsp.to_string(),
        };
        if let Ok(bytes) = encode(&frame, serializer) {
            sess.buffered.push((off, bytes));
            return true;
        }
    }
    false
}

fn random_session_id() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

impl Context {
    pub fn conn_id(&self) -> i64 {
        self.conn_id
    }

    pub async fn join(&self, room: &str) {
        let mut s = self.server.lock().await;
        if let Some(ns) = s.namespaces.get_mut(&self.namespace) {
            if ns.members.contains(&self.conn_id) {
                ns.rooms
                    .entry(room.to_string())
                    .or_default()
                    .insert(self.conn_id);
            }
        }
    }

    pub async fn leave(&self, room: &str) {
        let mut s = self.server.lock().await;
        if let Some(ns) = s.namespaces.get_mut(&self.namespace) {
            if let Some(m) = ns.rooms.get_mut(room) {
                m.remove(&self.conn_id);
                if m.is_empty() {
                    ns.rooms.remove(room);
                }
            }
        }
    }

    pub async fn members_of(&self, room: &str) -> Vec<i64> {
        let s = self.server.lock().await;
        s.namespaces
            .get(&self.namespace)
            .and_then(|ns| ns.rooms.get(room))
            .map(|m| {
                let mut v: Vec<i64> = m.iter().copied().collect();
                v.sort();
                v
            })
            .unwrap_or_default()
    }

    pub async fn rooms(&self) -> Vec<String> {
        let s = self.server.lock().await;
        s.namespaces
            .get(&self.namespace)
            .map(|ns| {
                let mut v: Vec<String> = ns
                    .rooms
                    .iter()
                    .filter(|(_, m)| m.contains(&self.conn_id))
                    .map(|(r, _)| r.clone())
                    .collect();
                v.sort();
                v
            })
            .unwrap_or_default()
    }

    async fn server_handle(&self) -> Arc<Server> {
        Arc::new(Server {
            state: self.server.clone(),
            on_connect: None,
            on_disconnect: None,
            on_error: None,
        })
    }

    pub async fn send_to_self(&self, event: &str, payload: &Value) -> bool {
        let srv = self.server_handle().await;
        send_to_connection(&srv, self.conn_id, event, payload, &self.namespace).await
    }

    pub async fn send_to(&self, to_id: i64, event: &str, payload: &Value) -> bool {
        let srv = self.server_handle().await;
        send_to_connection(&srv, to_id, event, payload, &self.namespace).await
    }

    pub async fn broadcast(&self, event: &str, payload: &Value) -> usize {
        let members: Vec<i64> = {
            let s = self.server.lock().await;
            s.namespaces
                .get(&self.namespace)
                .map(|ns| ns.members.iter().copied().collect())
                .unwrap_or_default()
        };
        let srv = self.server_handle().await;
        let mut n = 0;
        for id in members {
            if send_to_connection(&srv, id, event, payload, &self.namespace).await {
                n += 1;
            }
        }
        n
    }

    pub async fn broadcast_to_room(&self, room: &str, event: &str, payload: &Value) -> usize {
        let members = self.members_of(room).await;
        let srv = self.server_handle().await;
        let mut n = 0;
        for id in members {
            if send_to_connection(&srv, id, event, payload, &self.namespace).await {
                n += 1;
            }
        }
        n
    }
}
