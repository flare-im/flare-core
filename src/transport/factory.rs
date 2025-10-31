use crate::transport::connection::Connection;
use crate::transport::quic::QUICTransport;
use crate::transport::tcp::TCPTransport;
use crate::transport::websocket::WebSocketTransport;
use std::error::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// An enumeration of supported transport types.
pub enum TransportType {
    WebSocket,
    QUIC,
    TCP,
}

pub enum StreamWrapper {
    WebSocket(WebSocketStream<MaybeTlsStream<TcpStream>>),
    QUIC(quinn::Connection),
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
    ) -> Result<Box<dyn Connection>, Box<dyn Error>> {
        match (transport_type, stream) {
            (TransportType::WebSocket, StreamWrapper::WebSocket(ws_stream)) => {
                Ok(Box::new(WebSocketTransport::new(ws_stream)))
            }
            (TransportType::QUIC, StreamWrapper::QUIC(quic_conn)) => {
                Ok(Box::new(QUICTransport::new(quic_conn)))
            }
            (TransportType::TCP, StreamWrapper::TCP(tcp_stream)) => {
                Ok(Box::new(TCPTransport::new(tcp_stream)))
            }
            _ => Err("Mismatched transport type and stream".into()),
        }
    }
}