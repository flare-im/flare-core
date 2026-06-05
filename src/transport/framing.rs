//! Length-prefixed frame codec for raw byte transports (TCP, QUIC bi-stream).
//!
//! Wire format: `4-byte big-endian u32 length` + `payload`.
//! Maximum frame size defaults to 10 MiB (aligned with QUIC transport).

use crate::common::error::{FlareError, Result};
use crate::transport::events::ConnectionEvent;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum allowed payload size for a single frame.
pub const MAX_FRAME_LENGTH: usize = 10 * 1024 * 1024;

/// Classify peer disconnect I/O errors vs other failures.
pub fn event_from_io_error(err: &std::io::Error) -> ConnectionEvent {
    match err.kind() {
        std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::ConnectionAborted
        | std::io::ErrorKind::BrokenPipe
        | std::io::ErrorKind::UnexpectedEof => {
            ConnectionEvent::Disconnected(format!("TCP peer disconnected: {err}"))
        }
        _ => ConnectionEvent::Error(FlareError::io(err.to_string())),
    }
}

/// Read one length-prefixed frame from an async reader.
///
/// Returns an empty vector on clean EOF before any length prefix bytes (stream end).
pub async fn read_length_prefixed<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut length_buf = [0u8; 4];
    match reader.read_exact(&mut length_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(Vec::new()),
        Err(e) => return Err(FlareError::io(e.to_string())),
    }

    let length = u32::from_be_bytes(length_buf) as usize;
    if length == 0 {
        return Ok(Vec::new());
    }
    if length > MAX_FRAME_LENGTH {
        return Err(FlareError::io(format!(
            "Frame too large: {length} bytes (max {MAX_FRAME_LENGTH})"
        )));
    }

    let mut payload = vec![0u8; length];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|e| FlareError::io(e.to_string()))?;
    Ok(payload)
}

/// Write one length-prefixed frame.
pub async fn write_length_prefixed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> Result<()> {
    let length = data.len();
    if length > MAX_FRAME_LENGTH {
        return Err(FlareError::io(format!(
            "Frame too large: {length} bytes (max {MAX_FRAME_LENGTH})"
        )));
    }

    let length_bytes = (length as u32).to_be_bytes();
    writer
        .write_all(&length_bytes)
        .await
        .map_err(|e| FlareError::io(e.to_string()))?;
    writer
        .write_all(data)
        .await
        .map_err(|e| FlareError::io(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn round_trip_length_prefixed_frame() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        let payload = b"flare-tcp-framing";

        write_length_prefixed(&mut client, payload)
            .await
            .expect("write");
        client.flush().await.expect("flush");

        let decoded = read_length_prefixed(&mut server).await.expect("read");
        assert_eq!(decoded, payload);
    }

    #[tokio::test]
    async fn clean_eof_returns_empty() {
        let (client, mut server) = tokio::io::duplex(1024);
        drop(client);
        let decoded = read_length_prefixed(&mut server).await.expect("read");
        assert!(decoded.is_empty());
    }

    #[test]
    fn peer_reset_is_disconnected_event() {
        let err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(matches!(
            event_from_io_error(&err),
            ConnectionEvent::Disconnected(_)
        ));
    }
}
