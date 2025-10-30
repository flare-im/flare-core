use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use futures::{SinkExt, StreamExt};

#[tokio::main]
async fn main() {
    let url = "ws://127.0.0.1:9001";
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("✅ Connected to {}", url);

    // 发送文本消息
    ws_stream.send(Message::Text("Hello Server!".into())).await.unwrap();

    // 接收消息
    if let Some(msg) = ws_stream.next().await {
        println!("📨 Received: {:?}", msg);
    }

    ws_stream.close(None).await.unwrap();
}
