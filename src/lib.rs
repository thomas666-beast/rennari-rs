pub mod client;
pub mod protocol;
pub mod server;

pub use client::{Client, ClientHandler, ClientOptions};
pub use protocol::{
    decode, encode, valid_event_name, valid_id, valid_namespace_name, valid_offset, Frame,
    ProtocolError, Serializer, DEFAULT_MAX_BYTES,
};
pub use server::{AuthInfo, Context, Handler, Options, Server};

pub const VERSION: &str = "0.1.0";
