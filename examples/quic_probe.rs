//! 非交互 QUIC 连通性探测：`cargo run --example quic_probe -- quic://host:port [ca-cert-file]`
//!
//! 只做建连（TLS 握手 + 协商），用来区分三种失败：
//! UDP 不通（超时）、证书不被信任（invalid peer certificate）、服务端拒绝。
use flare_core::client::HybridClient;
use flare_core::client::{Client, ClientConfig};
use flare_core::common::config_types::TlsConfig;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args.next().unwrap_or_else(|| "quic://127.0.0.1:60052".to_string());
    let ca = args.next();
    let mut config = ClientConfig::new(url.clone()).quic();
    if let Some(ca) = ca {
        config.tls = TlsConfig::none().with_ca_cert(PathBuf::from(ca));
    }
    let started = std::time::Instant::now();
    match tokio::time::timeout(
        std::time::Duration::from_secs(8),
        HybridClient::connect_with_race(config),
    )
    .await
    {
        Ok(Ok(mut client)) => {
            println!(
                "QUIC_PROBE ok url={url} elapsed_ms={}",
                started.elapsed().as_millis()
            );
            let _ = client.disconnect().await;
        }
        Ok(Err(e)) => {
            println!(
                "QUIC_PROBE error url={url} elapsed_ms={} err={e}",
                started.elapsed().as_millis()
            );
            std::process::exit(2);
        }
        Err(_) => {
            println!("QUIC_PROBE timeout url={url} (UDP 很可能不通)");
            std::process::exit(3);
        }
    }
    Ok(())
}
