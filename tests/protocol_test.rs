use rennari::protocol::{decode, encode, valid_event_name, Frame, ProtocolError, Serializer};

fn from_hex(s: &str) -> Vec<u8> {
    let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    hex::decode(clean).unwrap()
}

#[test]
fn decode_perl_smile() {
    let bytes = from_hex("82a165a5736d696c65a16481a16e01");
    let f = decode(&bytes, Serializer::Msgpack, 65536).unwrap();
    assert_eq!(f.event, "smile");
    assert_eq!(f.data["n"], 1);
}

#[test]
fn decode_perl_smile_id() {
    let bytes = from_hex("83a16481a16e01a269642aa165a5736d696c65");
    let f = decode(&bytes, Serializer::Msgpack, 65536).unwrap();
    assert_eq!(f.event, "smile");
    assert_eq!(f.id, Some(42));
}

#[test]
fn decode_perl_smile_offset() {
    let bytes = from_hex("83a165a5736d696c65a16f07a16481a16e01");
    let f = decode(&bytes, Serializer::Msgpack, 65536).unwrap();
    assert_eq!(f.event, "smile");
    assert_eq!(f.offset, Some(7));
}

#[test]
fn decode_perl_chat() {
    let bytes = from_hex("82a16481a474657874a568656c6c6fa165a463686174");
    let f = decode(&bytes, Serializer::Msgpack, 65536).unwrap();
    assert_eq!(f.event, "chat");
    assert_eq!(f.data["text"], "hello");
}

#[test]
fn round_trip_msgpack() {
    let f = Frame::new("chat", serde_json::json!({"text": "hi"}))
        .with_id(42)
        .with_offset(7)
        .with_namespace("/chat");
    let bytes = encode(&f, Serializer::Msgpack).unwrap();
    let g = decode(&bytes, Serializer::Msgpack, 65536).unwrap();
    assert_eq!(g.event, "chat");
    assert_eq!(g.id, Some(42));
    assert_eq!(g.offset, Some(7));
    assert_eq!(g.namespace, "/chat");
    assert_eq!(g.data["text"], "hi");
}

#[test]
fn round_trip_json() {
    let f = Frame::new("chat", serde_json::json!({"text": "hi"}));
    let bytes = encode(&f, Serializer::Json).unwrap();
    let g = decode(&bytes, Serializer::Json, 65536).unwrap();
    assert_eq!(g.event, "chat");
    assert_eq!(g.data["text"], "hi");
}

#[test]
fn rejects_garbage() {
    let bytes = vec![0xff, 0xff, 0xff];
    assert!(decode(&bytes, Serializer::Msgpack, 65536).is_err());
}

#[test]
fn rejects_empty() {
    assert!(decode(&[], Serializer::Msgpack, 65536).is_err());
}

#[test]
fn valid_names() {
    for s in ["smile", "chat.message", "ns:evt", "a-b_c.1:2"] {
        assert!(valid_event_name(s), "{s} should be valid");
    }
    for s in ["", "bad\n", "with space", "ünïcode"] {
        assert!(!valid_event_name(s), "{s} should be invalid");
    }
}

#[test]
fn encode_rejects_bad_names() {
    assert!(matches!(
        encode(&Frame::new("", serde_json::json!({})), Serializer::Msgpack),
        Err(ProtocolError::InvalidEventName)
    ));
    assert!(matches!(
        encode(
            &Frame::new("bad\n", serde_json::json!({})),
            Serializer::Msgpack
        ),
        Err(ProtocolError::InvalidEventName)
    ));
}
