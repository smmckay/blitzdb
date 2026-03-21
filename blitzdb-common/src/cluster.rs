use std::time::Duration;
use anyhow::Context;
use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig};
use chitchat::transport::UdpTransport;
use crate::CLUSTER_ID;

pub async fn start_chitchat(service: &str, listen_addr: std::net::SocketAddr, seed_nodes: Vec<String>, initial_key_values: Vec<(String, String)>) -> anyhow::Result<ChitchatHandle> {
    // Discover server via chitchat.
    let name = hostname::get()?;
    let hostname = name.to_string_lossy();
    let gossip_port = listen_addr.port();
    let chitchat_id = ChitchatId::new(
        format!("{service}-{hostname}:{gossip_port}"),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        listen_addr,
    );
    let config = ChitchatConfig {
        chitchat_id,
        cluster_id: CLUSTER_ID.to_string(),
        gossip_interval: Duration::from_millis(200),
        listen_addr,
        seed_nodes,
        failure_detector_config: FailureDetectorConfig::default(),
        marked_for_deletion_grace_period: Duration::from_secs(60),
        catchup_callback: None,
        extra_liveness_predicate: None,
    };
    chitchat::spawn_chitchat(config, initial_key_values, &UdpTransport)
        .await
        .context("Failed to spawn chitchat")
}