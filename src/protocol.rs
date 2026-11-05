use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

pub const DEFAULT_MAX_BYTES: usize = 64 * 1024;

fn event_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Za-z0-9_.:-]{1,64}$").unwrap())
}

fn nsp_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^/[A-Za-z0-9_.:/-]{0,63}$").unwrap())
}

fn id_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[0-9]{1,20}$").unwrap())
}

fn offset_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[0-9]+$").unwrap())
}

pub fn valid_event_name(name: &str) -> bool {
    event_re().is_match(name)
}

pub fn valid_namespace_name(name: &str) -> bool {
    nsp_re().is_match(name)
}

pub fn valid_id(id: i64) -> bool {
    id >= 0 && id_re().is_match(&id.to_string())
}

pub fn valid_offset(o: i64) -> bool {
    o >= 0 && offset_re().is_match(&o.to_string())
}

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub event: String,
    pub data: serde_json::Value,
    pub id: Option<i64>,
    pub offset: Option<i64>,
    pub namespace: String,
}

impl Frame {
    pub fn new(event: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            event: event.into(),
            data,
            id: None,
            offset: None,
            namespace: String::new(),
        }
    }

    pub fn with_id(mut self, id: i64) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_namespace(mut self, nsp: impl Into<String>) -> Self {
        self.namespace = nsp.into();
        self
    }
}

#[derive(Debug)]
pub enum ProtocolError {
    InvalidEventName,
    InvalidNamespace,
    InvalidId,
    InvalidOffset,
    UnsupportedSerializer(String),
    EmptyInput,
    TooLarge,
    NotAMap,
    Malformed(String),
    Serialize(String),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::InvalidEventName => write!(f, "invalid event name"),
            ProtocolError::InvalidNamespace => write!(f, "invalid namespace"),
            ProtocolError::InvalidId => write!(f, "invalid id"),
            ProtocolError::InvalidOffset => write!(f, "invalid offset"),
            ProtocolError::UnsupportedSerializer(s) => write!(f, "unsupported serializer {s:?}"),
            ProtocolError::EmptyInput => write!(f, "empty input"),
            ProtocolError::TooLarge => write!(f, "frame exceeds max_bytes"),
            ProtocolError::NotAMap => write!(f, "not a map"),
            ProtocolError::Malformed(s) => write!(f, "malformed: {s}"),
            ProtocolError::Serialize(s) => write!(f, "serialize: {s}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Serializer {
    Msgpack,
    Json,
}

impl Serializer {
    pub fn from_name(name: Option<&str>) -> Result<Self, ProtocolError> {
        match name {
            None | Some("msgpack") => Ok(Serializer::Msgpack),
            Some("json") => Ok(Serializer::Json),
            Some(other) => Err(ProtocolError::UnsupportedSerializer(other.to_string())),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RawFrame {
    e: String,
    d: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    o: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n: Option<String>,
}

pub fn encode(frame: &Frame, serializer: Serializer) -> Result<Vec<u8>, ProtocolError> {
    if !valid_event_name(&frame.event) {
        return Err(ProtocolError::InvalidEventName);
    }
    if let Some(id) = frame.id
        && !valid_id(id)
    {
        return Err(ProtocolError::InvalidId);
    }
    if let Some(off) = frame.offset
        && !valid_offset(off)
    {
        return Err(ProtocolError::InvalidOffset);
    }

    let nsp = if frame.namespace.is_empty() || frame.namespace == "/" {
        None
    } else {
        if !valid_namespace_name(&frame.namespace) {
            return Err(ProtocolError::InvalidNamespace);
        }
        Some(frame.namespace.clone())
    };

    let raw = RawFrame {
        e: frame.event.clone(),
        d: frame.data.clone(),
        id: frame.id,
        o: frame.offset,
        n: nsp,
    };

    match serializer {
        Serializer::Msgpack => {
            rmp_serde::to_vec_named(&raw).map_err(|e| ProtocolError::Serialize(e.to_string()))
        }
        Serializer::Json => {
            serde_json::to_vec(&raw).map_err(|e| ProtocolError::Serialize(e.to_string()))
        }
    }
}

pub fn decode(
    bytes: &[u8],
    serializer: Serializer,
    max_bytes: usize,
) -> Result<Frame, ProtocolError> {
    if bytes.is_empty() {
        return Err(ProtocolError::EmptyInput);
    }
    if bytes.len() > max_bytes {
        return Err(ProtocolError::TooLarge);
    }

    let raw: RawFrame = match serializer {
        Serializer::Msgpack => {
            rmp_serde::from_slice(bytes).map_err(|e| ProtocolError::Malformed(e.to_string()))?
        }
        Serializer::Json => {
            serde_json::from_slice(bytes).map_err(|e| ProtocolError::Malformed(e.to_string()))?
        }
    };

    if !valid_event_name(&raw.e) {
        return Err(ProtocolError::InvalidEventName);
    }
    if let Some(id) = raw.id
        && !valid_id(id)
    {
        return Err(ProtocolError::InvalidId);
    }
    if let Some(off) = raw.o
        && !valid_offset(off)
    {
        return Err(ProtocolError::InvalidOffset);
    }
    let namespace = match raw.n {
        Some(ref n) if valid_namespace_name(n) => n.clone(),
        Some(_) => return Err(ProtocolError::InvalidNamespace),
        None => String::new(),
    };

    Ok(Frame {
        event: raw.e,
        data: raw.d,
        id: raw.id,
        offset: raw.o,
        namespace,
    })
}
