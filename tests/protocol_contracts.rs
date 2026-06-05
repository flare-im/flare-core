use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::encryption::EncryptionAlgorithm;
use flare_core::common::message::MessageParser;
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{Command, FrameBuilder, Reliability, SerializationFormat, ping};

fn ping_frame(message_id: &str) -> flare_core::common::protocol::Frame {
    FrameBuilder::new()
        .with_message_id(message_id.to_string())
        .with_reliability(Reliability::AtLeastOnce)
        .with_timestamp(42)
        .with_command(Command {
            r#type: Some(Type::System(ping())),
        })
        .build()
}

#[test]
fn json_parser_round_trips_public_frame_contract() {
    let parser = MessageParser::json();
    let frame = ping_frame("json-contract");

    let encoded = parser.serialize(&frame).expect("json frame should encode");
    let decoded = parser.parse(&encoded).expect("json frame should decode");

    assert_eq!(decoded.message_id, "json-contract");
    assert_eq!(decoded.reliability, Reliability::AtLeastOnce as i32);
    assert!(decoded.command.is_some());
}

#[test]
fn protobuf_parser_round_trips_public_frame_contract() {
    let parser = MessageParser::protobuf();
    let frame = ping_frame("protobuf-contract");

    let encoded = parser
        .serialize(&frame)
        .expect("protobuf frame should encode");
    let decoded = parser
        .parse(&encoded)
        .expect("protobuf frame should decode");

    assert_eq!(decoded.message_id, "protobuf-contract");
    assert_eq!(decoded.timestamp, 42);
    assert!(decoded.command.is_some());
}

#[test]
fn strict_parser_rejects_uncompressed_payload_when_gzip_is_required() {
    let plain_parser = MessageParser::json();
    let strict_gzip_parser = MessageParser::new(
        SerializationFormat::Json,
        CompressionAlgorithm::Gzip,
        EncryptionAlgorithm::None,
    );
    let frame = ping_frame("strict-compression-contract");
    let uncompressed = plain_parser
        .serialize(&frame)
        .expect("plain json frame should encode");

    let error = strict_gzip_parser
        .parse_with_fallback(&uncompressed, false)
        .expect_err("strict gzip parser should reject uncompressed data");

    assert!(
        error.to_string().contains("strict") || error.to_string().contains("严格模式"),
        "strict failure should explain strict parsing mode: {error}"
    );
}
