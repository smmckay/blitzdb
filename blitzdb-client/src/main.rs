use blitzdb_client::BlitzClient;
use blitzdb_common::logging;
use boomphf::Mphf;
use std::fs::File;
use log::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::setup_logging();

    let mut args = std::env::args().skip(1);
    let mph_path =
        args.next().expect("Usage: blitzdb-client <mph-file> <key> [seed] [gossip-port]");
    let key = args.next().expect("Usage: blitzdb-client <mph-file> <key> [seed] [gossip-port]");
    let seed = args.next().unwrap_or_else(|| "127.0.0.1:10000".to_string());
    let gossip_port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10001);

    let mph: Mphf<Vec<u8>> =
        bincode::deserialize_from(File::open(&mph_path).expect("Failed to open .mph file"))
            .expect("Failed to deserialize MPH");
    info!("Loaded MPH from {mph_path}");

    let client = BlitzClient::connect(mph, &seed, gossip_port).await?;
    let value = client.get(key.as_bytes()).await?;
    info!("{key} = {}", String::from_utf8_lossy(&value));

    client.shutdown().await
}
