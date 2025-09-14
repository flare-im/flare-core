# Client Examples

This directory contains various client examples demonstrating how to use the flare-core library.

## Examples

### 1. QUIC Client (`quic_client.rs`)
Demonstrates how to create a QUIC client connection and communicate with a QUIC server.

Features:
- QUIC protocol connection
- Protobuf serialization
- Heartbeat mechanism
- Message sending and receiving

### 2. WebSocket Client (`websocket_client.rs`)
Demonstrates how to create a WebSocket client connection and communicate with a WebSocket server.

Features:
- WebSocket protocol connection
- Protobuf serialization
- Heartbeat mechanism
- Message sending and receiving

### 3. Protocol Race Client (`protocol_race_client.rs`)
Demonstrates how to simultaneously attempt connections using both QUIC and WebSocket protocols, automatically selecting the faster one.

Features:
- Protocol racing (QUIC vs WebSocket)
- Automatic protocol selection based on connection speed
- Fallback mechanism if one protocol fails

## Usage

To run any of the examples:

```bash
# Run QUIC client example
cargo run --example quic_client

# Run WebSocket client example
cargo run --example websocket_client

# Run protocol race client example
cargo run --example protocol_race_client
```

## Configuration

Each example can be configured by modifying the connection parameters in the source code:

- Server address and port
- Serialization format
- Heartbeat interval and timeout
- Reconnection settings