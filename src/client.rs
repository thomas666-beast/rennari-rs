use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::protocol::{DEFAULT_MAX_BYTES, Frame, ProtocolError, Serializer, decode, encode};

pub type ClientHandler = Arc<dyn Fn(Value) + Send + Sync>;

pub struct ClientOptions {
    pub serializer: Serializer,
    pub max_bytes: usize,
    pub ack_timeout: Duration,
    pub retries: u32,
    pub extra_headers: Vec<(String, String)>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            serializer: Serializer::Msgpack,
            max_bytes: DEFAULT_MAX_BYTES,
            ack_timeout: Duration::from_secs(5),
            retries: 3,
            extra_headers: Vec::new(),
        }
    }
}

struct State {
    handlers: HashMap<String, HashMap<String, ClientHandler>>,
    pending: HashMap<i64, oneshot::Sender<Value>>,
    next_id: i64,
    session_id: Option<String>,
    last_offset: i64,
}

pub struct Client {
    url: String,
    options: ClientOptions,
    state: Arc<Mutex<State>>,
    tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
    reader: Option<JoinHandle<()>>,
}

impl Client {
    pub fn new(url: impl Into<String>, options: ClientOptions) -> Self {
        Self {
            url: url.into(),
            options,
            state: Arc::new(Mutex::new(State {
                handlers: HashMap::new(),
                pending: HashMap::new(),
                next_id: 0,
                session_id: None,
                last_offset: -1,
            })),
            tx: None,
            reader: None,
        }
    }

    pub async fn on(&self, event: &str, handler: ClientHandler) {
        let mut s = self.state.lock().await;
        s.handlers
            .entry("/".to_string())
            .or_default()
            .insert(event.to_string(), handler);
    }

    pub async fn on_namespace(&self, nsp: &str, event: &str, handler: ClientHandler) {
        let mut s = self.state.lock().await;
        s.handlers
            .entry(nsp.to_string())
            .or_default()
            .insert(event.to_string(), handler);
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut request = self.url.as_str().into_client_request()?;
        for (k, v) in &self.options.extra_headers {
            request.headers_mut().insert(
                k.parse::<http::header::HeaderName>()?,
                v.parse::<http::header::HeaderValue>()?,
            );
        }

        let (ws, _) = connect_async(request).await?;
        let (mut sink, mut stream) = ws.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let writer = tokio::spawn(async move {
            while let Some(bytes) = rx.recv().await {
                if sink.send(Message::Binary(bytes)).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });

        let state = self.state.clone();
        let serializer = self.options.serializer;
        let max_bytes = self.options.max_bytes;

        let reader = tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                let Ok(msg) = msg else { break };
                if let Message::Binary(bytes) = msg
                    && let Ok(frame) = decode(&bytes, serializer, max_bytes)
                {
                    dispatch_frame(state.clone(), frame).await;
                }
            }
            let _ = writer.await;
        });

        self.tx = Some(tx.clone());
        self.reader = Some(reader);

        let namespaces: Vec<String> = {
            let s = self.state.lock().await;
            s.handlers
                .keys()
                .filter(|k| k.as_str() != "/")
                .cloned()
                .collect()
        };
        for nsp in namespaces {
            let frame = Frame::new("rennari_nsp", serde_json::json!({"name": nsp}));
            let bytes = encode(&frame, self.options.serializer)?;
            let _ = tx.send(bytes);
        }

        Ok(())
    }

    pub async fn disconnect(&mut self) {
        if let Some(tx) = self.tx.take() {
            drop(tx);
        }
        if let Some(reader) = self.reader.take() {
            reader.abort();
        }
    }

    pub async fn emit(&self, event: &str, payload: Value) -> Result<(), ProtocolError> {
        self.emit_namespace("/", event, payload).await
    }

    pub async fn emit_namespace(
        &self,
        nsp: &str,
        event: &str,
        payload: Value,
    ) -> Result<(), ProtocolError> {
        let tx = self.tx.as_ref().ok_or(ProtocolError::EmptyInput)?;
        let frame = Frame::new(event, payload).with_namespace(nsp);
        let bytes = encode(&frame, self.options.serializer)?;
        tx.send(bytes).map_err(|_| ProtocolError::EmptyInput)
    }

    pub async fn emit_with_ack(
        &self,
        event: &str,
        payload: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.emit_with_ack_namespace("/", event, payload).await
    }

    pub async fn emit_with_ack_namespace(
        &self,
        nsp: &str,
        event: &str,
        payload: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| Box::<dyn std::error::Error + Send + Sync>::from("not connected"))?
            .clone();

        let mut attempts = 0;
        loop {
            attempts += 1;
            let id = {
                let mut s = self.state.lock().await;
                s.next_id += 1;
                s.next_id
            };
            let (ack_tx, ack_rx) = oneshot::channel();
            {
                let mut s = self.state.lock().await;
                s.pending.insert(id, ack_tx);
            }
            let frame = Frame::new(event, payload.clone())
                .with_id(id)
                .with_namespace(nsp);
            let bytes = encode(&frame, self.options.serializer)?;
            if tx.send(bytes).is_err() {
                return Err("not connected".into());
            }

            match tokio::time::timeout(self.options.ack_timeout, ack_rx).await {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(_)) => return Err("ack channel closed".into()),
                Err(_) => {
                    let mut s = self.state.lock().await;
                    s.pending.remove(&id);
                    if attempts > self.options.retries {
                        return Err(format!("ack timeout for '{event}'").into());
                    }
                }
            }
        }
    }

    pub async fn session_id(&self) -> Option<String> {
        self.state.lock().await.session_id.clone()
    }

    pub async fn last_offset(&self) -> i64 {
        self.state.lock().await.last_offset
    }
}

async fn dispatch_frame(state: Arc<Mutex<State>>, frame: Frame) {
    if let Some(off) = frame.offset {
        let mut s = state.lock().await;
        if off > s.last_offset {
            s.last_offset = off;
        }
    }

    if frame.event == "rennari_session" {
        if let Some(sid) = frame.data.get("id").and_then(|v| v.as_str()) {
            let mut s = state.lock().await;
            s.session_id = Some(sid.to_string());
        }
        return;
    }
    if frame.event == "rennari_nsp_ok" {
        return;
    }
    if frame.event == "rennari_resumed" {
        if let Some(sid) = frame.data.get("session_id").and_then(|v| v.as_str()) {
            let mut s = state.lock().await;
            s.session_id = Some(sid.to_string());
        }
        return;
    }
    if frame.event == "rennari_resume_failed" {
        let mut s = state.lock().await;
        s.session_id = None;
        s.last_offset = -1;
        return;
    }
    if frame.event == "rennari_ack" {
        if let Some(id) = frame.data.get("id").and_then(|v| v.as_i64()) {
            let mut s = state.lock().await;
            if let Some(tx) = s.pending.remove(&id) {
                let _ = tx.send(frame.data.clone());
            }
        }
        return;
    }

    let nsp = if frame.namespace.is_empty() {
        "/".to_string()
    } else {
        frame.namespace.clone()
    };

    let handler = {
        let s = state.lock().await;
        s.handlers
            .get(&nsp)
            .and_then(|m| m.get(&frame.event))
            .cloned()
    };
    if let Some(h) = handler {
        h(frame.data);
    }
}
