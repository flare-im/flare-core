# Server Examples

This directory contains various server examples demonstrating how to use the flare-core library.

## Examples

### 1. QUIC Server (`quic_server.rs`)
Demonstrates how to create a QUIC server that listens for QUIC client connections.

Features:
- QUIC protocol server
- Protobuf serialization
- Connection management
- Heartbeat mechanism
- Message handling

### 2. WebSocket Server (`websocket_server.rs`)
Demonstrates how to create a WebSocket server that listens for WebSocket client connections.

Features:
- WebSocket protocol server
- Protobuf serialization
- Connection management
- Heartbeat mechanism
- Message handling

### 3. Dual Protocol Server (`dual_protocol_server.rs`)
Demonstrates how to create a server that simultaneously supports both QUIC and WebSocket protocols, allowing clients to seamlessly switch between protocols.

Features:
- Dual protocol support (QUIC and WebSocket)
- Automatic protocol detection
- Seamless client switching between protocols
- Connection management for both protocols
- Heartbeat mechanism
- Message handling

## Usage

To run any of the examples:

```bash
# Run QUIC server example
cargo run --example quic_server

# Run WebSocket server example
cargo run --example websocket_server

# Run dual protocol server example
cargo run --example dual_protocol_server
```

## Configuration

Each example can be configured by modifying the server parameters in the source code:

- Listening addresses and ports
- Serialization format
- Heartbeat interval and timeout
- Maximum connections
- Connection timeout settings