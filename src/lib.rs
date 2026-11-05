pub mod protocol;

pub use protocol::{
    decode, encode, valid_event_name, valid_id, valid_namespace_name, valid_offset, Frame,
    ProtocolError, Serializer, DEFAULT_MAX_BYTES,
};

pub const VERSION: &str = "0.1.0";
