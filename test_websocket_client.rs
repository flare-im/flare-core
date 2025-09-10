use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 连接到WebSocket服务器
    let url = "ws://127.0.0.1:8080";
    let (ws_stream, _) = connect_async(url).await?;
    println!("已连接到WebSocket服务器");
    
    let (mut write, mut read) = ws_stream.split();
    
    // 发送测试消息
    let test_message = "Hello, Flare Core Server!";
    write.send(Message::Text(test_message.into())).await?;
    println!("已发送消息: {}", test_message);
    
    // 等待回复
    if let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => println!("收到回复: {}", text),
            Ok(Message::Binary(data)) => println!("收到二进制数据: {} bytes", data.len()),
            _ => println!("收到其他类型的消息"),
        }
    }
    
    Ok(())
}