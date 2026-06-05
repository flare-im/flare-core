use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
#[cfg(feature = "quic")]
use crate::transport::quic::QUICTransport;
#[cfg(feature = "tcp")]
use crate::transport::tcp::TCPTransport;
#[cfg(feature = "websocket")]
use crate::transport::websocket::WebSocketTransport;
#[cfg(any(feature = "websocket", feature = "tcp"))]
use tokio::net::TcpStream;
#[cfg(feature = "websocket")]
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// An enumeration of supported transport types.
pub enum TransportType {
    WebSocket,
    QUIC,
    TCP,
}

#[allow(clippy::large_enum_variant)]
pub enum StreamWrapper {
    #[cfg(feature = "websocket")]
    WebSocket(Box<WebSocketStream<MaybeTlsStream<TcpStream>>>),
    #[cfg(feature = "quic")]
    QUIC {
        send: quinn::SendStream,
        recv: quinn::RecvStream,
    },
    #[cfg(feature = "tcp")]
    TCP(TcpStream),
}

/// A factory for creating transport layer connections.
pub struct TransportFactory;

impl TransportFactory {
    pub fn create_connection(
        transport_type: TransportType,
        stream: StreamWrapper,
    ) -> Result<Box<dyn Connection>> {
        match (transport_type, stream) {
            #[cfg(feature = "websocket")]
            (TransportType::WebSocket, StreamWrapper::WebSocket(ws_stream)) => {
                Ok(Box::new(WebSocketTransport::new(*ws_stream)))
            }
            #[cfg(feature = "quic")]
            (TransportType::QUIC, StreamWrapper::QUIC { send, recv }) => {
                Ok(Box::new(QUICTransport::new(send, recv)))
            }
            #[cfg(feature = "tcp")]
            (TransportType::TCP, StreamWrapper::TCP(tcp_stream)) => {
                Ok(Box::new(TCPTransport::new(tcp_stream)))
            }
            #[allow(unreachable_patterns)]
            _ => Err(FlareError::protocol_error(
                "Mismatched transport type and stream",
            )),
        }
    }
}
