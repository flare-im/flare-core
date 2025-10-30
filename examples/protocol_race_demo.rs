use flare_core::common::connections::config::ConnectionConfig;
use flare_core::common::connections::enums::Transport;
use flare_core::client::protocol_racer::RacingHandle;
use flare_core::common::connections::traits::ConnectionEvent;
use flare_core::common::protocol::frame::Frame;
use flare_core::common::error::FlareError;
use std::sync::Arc;

struct PrintEvents;
impl ConnectionEvent for PrintEvents {
    fn on_connected(&self) {
        println!("connected");
    }
    fn on_disconnected(&self, reason: Option<String>) {
        println!("disconnected: {:?}", reason);
    }
    fn on_error(&self, err: FlareError) {
        println!("error: {:?}", err);
    }
    fn on_message_received(&self, _frame: Frame) {
        println!("message received");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 尝试同时连 WS 与 QUIC，谁先连上用谁
    let base = ConnectionConfig::default();
    let addresses = vec![
        "127.0.0.1:9001".to_string(), // WebSocket server
        "127.0.0.1:9002".to_string(), // QUIC server
    ];
    let protocols = vec![Transport::WebSocket, Transport::Quic];

    let handler: Arc<dyn ConnectionEvent> = Arc::new(PrintEvents);
    let racer = RacingHandle::new(base, addresses, protocols, Some(handler), 2_000);
    racer.start().await.map_err(|e| {
        println!("racer start error: {:?}", e);
        e
    })?;

    if let Some(conn) = racer.get_connection().await {
        // 示例：发送一帧（这里仅演示调用接口，具体帧编码由上层完成）
        let _ = conn.send_bytes(vec![1, 2, 3, 4]);
    }

    // 等待一会观察事件输出
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    Ok(())
}


