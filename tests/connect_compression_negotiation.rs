//! 压缩只协商客户端解得开的（W7-S2 发现 4）。
//!
//! 本机回环（只绑 127.0.0.1）起一个默认压缩为 gzip 的真实 WebSocket 服务端，用裸 WebSocket 客户端
//! 发 CONNECT。没有声明能解 gzip 的客户端（例如编译时没开 gzip 特性）以前也被协商成 gzip，此后的帧
//! 包括停机 CLOSE 都解不开；现在协商为 none，CLOSE 按 JSON、不压缩编码。声明了 `accept_compression`
//! 的客户端照旧用服务端的默认压缩。

#![cfg(all(
    feature = "server",
    feature = "websocket",
    feature = "compression-gzip",
    not(target_arch = "wasm32")
))]

use async_trait::async_trait;
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::message::{MessageParser, PRE_NEGOTIATION_PARSER};
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::system_command::Type as SystemType;
use flare_core::common::protocol::{
    Frame, Reliability, SerializationFormat, SystemCommand, connect, frame_with_system_command,
};
use flare_core::common::shutdown::is_server_shutdown_close;
use flare_core::server::{FlareServer, FlareServerBuilder, ServerEventHandler};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

struct NoopHandler;

#[async_trait]
impl ServerEventHandler for NoopHandler {}

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a probe TCP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

async fn start_gzip_server(port: u16) -> FlareServer {
    let address = format!("127.0.0.1:{port}");
    let server = FlareServerBuilder::new(address.clone(), Arc::new(NoopHandler))
        .with_protocols(vec![TransportProtocol::WebSocket])
        .with_protocol_address(TransportProtocol::WebSocket, address)
        .with_default_format(SerializationFormat::Json)
        .with_default_compression(CompressionAlgorithm::Gzip)
        .with_shutdown_close_grace(Duration::from_millis(200))
        .build()
        .expect("build WebSocket server");
    server.start().await.expect("start WebSocket server");
    server
}

fn system_command(frame: &Frame) -> Option<&SystemCommand> {
    match frame.command.as_ref()?.r#type.as_ref()? {
        CommandType::System(command) => Some(command),
        _ => None,
    }
}

/// 发 CONNECT，返回 CONNECT_ACK 里协商出的压缩与 WebSocket 连接。
async fn connect_and_negotiate(
    port: u16,
    metadata: HashMap<String, Vec<u8>>,
) -> (
    String,
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) {
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}"))
        .await
        .expect("raw WebSocket handshake");
    let connect_frame = frame_with_system_command(
        connect(SerializationFormat::Json, metadata),
        Reliability::AtLeastOnce,
    );
    socket
        .send(Message::Binary(
            PRE_NEGOTIATION_PARSER
                .serialize(&connect_frame)
                .expect("encode CONNECT")
                .into(),
        ))
        .await
        .expect("send CONNECT");
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("CONNECT_ACK in time")
            .expect("socket open")
            .expect("read CONNECT_ACK");
        let data = match message {
            Message::Binary(data) => data.to_vec(),
            Message::Text(text) => text.as_bytes().to_vec(),
            _ => continue,
        };
        let frame = PRE_NEGOTIATION_PARSER
            .parse(&data)
            .expect("CONNECT_ACK is JSON");
        if let Some(command) = system_command(&frame)
            && command.r#type == SystemType::ConnectAck as i32
        {
            return (command.compression.clone(), socket);
        }
    }
}

/// 停机：读到 CLOSE 系统帧为止，返回它的原始字节。
async fn shutdown_close_bytes(
    server: FlareServer,
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Vec<u8> {
    let stop = tokio::spawn(async move { server.stop().await });
    let data = loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("frames before the close")
            .expect("socket open")
            .expect("read frame");
        match message {
            Message::Binary(data) => break data.to_vec(),
            Message::Text(text) => break text.as_bytes().to_vec(),
            Message::Close(_) => panic!("transport closed before the CLOSE system frame"),
            _ => {}
        }
    };
    stop.await.expect("stop task").expect("stop server");
    data
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_declares_no_compression_is_not_sent_gzip() {
    let port = free_tcp_port();
    let server = start_gzip_server(port).await;

    let (compression, mut socket) = connect_and_negotiate(port, HashMap::new()).await;
    assert_eq!(compression, "none");

    // 只认 JSON、不解压的客户端也能解停机 CLOSE。
    let data = shutdown_close_bytes(server, &mut socket).await;
    let strict = MessageParser::new(
        SerializationFormat::Json,
        CompressionAlgorithm::None,
        flare_core::common::encryption::EncryptionAlgorithm::None,
    );
    let frame = strict
        .parse_with_fallback(&data, false)
        .expect("a client without gzip must decode the shutdown CLOSE");
    let command = system_command(&frame).expect("system command");
    assert!(is_server_shutdown_close(command));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_accepts_gzip_gets_the_server_default() {
    let port = free_tcp_port();
    let server = start_gzip_server(port).await;

    let metadata = HashMap::from([(
        flare_core::common::constants::metadata_keys::ACCEPT_COMPRESSION.to_string(),
        b"gzip".to_vec(),
    )]);
    let (compression, mut socket) = connect_and_negotiate(port, metadata).await;
    assert_eq!(compression, "gzip");

    let data = shutdown_close_bytes(server, &mut socket).await;
    assert!(data.starts_with(&[0x1f, 0x8b]), "negotiated gzip applies");
    let negotiated = MessageParser::new(
        SerializationFormat::Json,
        CompressionAlgorithm::Gzip,
        flare_core::common::encryption::EncryptionAlgorithm::None,
    );
    let frame = negotiated.parse(&data).expect("decode the shutdown CLOSE");
    assert!(is_server_shutdown_close(
        system_command(&frame).expect("system command")
    ));
}
