use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
use crate::transport::quic::QUICTransport;
use crate::transport::tcp::TCPTransport;
use crate::transport::websocket::WebSocketTransport;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// An enumeration of supported transport types.
pub enum TransportType {
    WebSocket,
    QUIC,
    TCP,
}

#[allow(clippy::large_enum_variant)]
pub enum StreamWrapper {
    WebSocket(Box<WebSocketStream<MaybeTlsStream<TcpStream>>>),
    QUIC {
        send: quinn::SendStream,
        recv: quinn::RecvStream,
    },
    TCP(TcpStream),
}

/// A factory for creating transport layer connections.
///
/// This factory provides a unified interface for creating different types of
/// transport connections, such as WebSocket, QUIC, or TCP.
pub struct TransportFactory;

impl TransportFactory {
    /// Creates a new transport connection based on the specified type and stream.
    ///
    /// # Arguments
    ///
    /// * `transport_type` - The type of transport to create.
    /// * `stream` - A stream object that the transport will use for communication.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Box<dyn Connection>` on success, or an error if
    /// the connection could not be created.
    pub fn create_connection(
        transport_type: TransportType,
        stream: StreamWrapper,
    ) -> Result<Box<dyn Connection>> {
        match (transport_type, stream) {
            (TransportType::WebSocket, StreamWrapper::WebSocket(ws_stream)) => {
                let ws_stream = *ws_stream;
                Ok(Box::new(WebSocketTransport::new(ws_stream)))
            }
            (TransportType::QUIC, StreamWrapper::QUIC { send, recv }) => {
                Ok(Box::new(QUICTransport::new(send, recv)))
            }
            (TransportType::TCP, StreamWrapper::TCP(tcp_stream)) => {
                Ok(Box::new(TCPTransport::new(tcp_stream)))
            }
            _ => Err(FlareError::protocol_error(
                "Mismatched transport type and stream",
            )),
        }
    }
}
