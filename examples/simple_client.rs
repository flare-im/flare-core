use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use flare_core::transport::factory::{StreamWrapper, TransportFactory, TransportType};
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_tungstenite::connect_async;

struct MyObserver {
    is_connected: Arc<AtomicBool>,
}

impl ConnectionObserver for MyObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                println!("Connection established");
                self.is_connected.store(true, Ordering::SeqCst);
            }
            ConnectionEvent::Disconnected(reason) => {
                println!("Connection closed: {}", reason);
                self.is_connected.store(false, Ordering::SeqCst);
            }
            ConnectionEvent::Message(data) => {
                println!("Received message: {:?}", data);
            }
            ConnectionEvent::Error(err) => {
                println!("Error: {}", err);
                self.is_connected.store(false, Ordering::SeqCst);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = "wss://echo.websocket.org";
    let (ws_stream, _) = connect_async(url).await?;
    let stream = StreamWrapper::WebSocket(ws_stream);

    let mut client = TransportFactory::create_connection(TransportType::WebSocket, stream).unwrap();
    let is_connected = Arc::new(AtomicBool::new(false));
    let observer = Arc::new(MyObserver { is_connected: Arc::clone(&is_connected) });
    client.add_observer(observer);

    let mut count = 0;
    while is_connected.load(Ordering::SeqCst) && count <= 5 {
        let msg = format!("Hello, world! {}", count);
        if client.send(msg.as_bytes()).await.is_err() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        count += 1;
    }

    if is_connected.load(Ordering::SeqCst) {
        client.close().await?;
    }

    Ok(())
}