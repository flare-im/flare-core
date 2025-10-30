use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::accept_async;
use futures::{StreamExt, SinkExt};

#[tokio::main]
async fn main() {
    // 监听 WebSocket 端口
    let listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();
    println!("✅ WebSocket server listening on ws://127.0.0.1:9001");

    while let Ok((stream, addr)) = listener.accept().await {
        println!("🔗 New connection from: {}", addr);
        tokio::spawn(handle_connection(stream));
    }
}

async fn handle_connection(stream: TcpStream) {
    // 将 TCP 流升级为 WebSocket
    let ws_stream = accept_async(stream).await.unwrap();
    println!("🤝 WebSocket handshake success!");

    let (mut write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                println!("📨 Received: {}", text);
                // 回显消息
                write.send(Message::Text(format!("Echo: {}", text))).await.unwrap();
            }
            Ok(Message::Binary(bin)) => {
                println!("📦 Binary message ({} bytes)", bin.len());
            }
            Ok(Message::Close(frame)) => {
                println!("❌ Connection closed: {:?}", frame);
                break;
            }
            _ => {}
        }
    }
}
