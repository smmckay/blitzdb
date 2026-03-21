use blitzdb_common::*;
use blitzdb_fabric::FabricEndpoint;
use boomphf::Mphf;
use std::fs::File;
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mph_path =
        args.next().expect("Usage: blitzdb-client <mph-file> <key> [seed] [gossip-port]");
    let key = args.next().expect("Usage: blitzdb-client <mph-file> <key> [seed] [gossip-port]");
    let seed = args.next().unwrap_or_else(|| "127.0.0.1:10000".to_string());
    let gossip_port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10001);
    let gossip_addr: SocketAddr = format!("0.0.0.0:{gossip_port}").parse().unwrap();

    // Load the MPH from disk.
    let mph: Mphf<Vec<u8>> =
        bincode::deserialize_from(File::open(&mph_path).expect("Failed to open .mph file"))
            .expect("Failed to deserialize MPH");
    println!("Loaded MPH from {mph_path}");


    let handle = cluster::start_chitchat("client", gossip_addr, vec![seed.to_string()], vec![]).await?;
    println!("Chitchat started, seed={seed}, waiting for server...");

    let (index_mr_key, heap_mr_key, ep_addr_bytes): (u64, u64, Vec<u8>) = loop {
        let chitchat = handle.chitchat();
        let guard = chitchat.lock().await;
        let mut found = None;
        for node_id in guard.live_nodes() {
            if let Some(state) = guard.node_state(node_id) {
                if let (Some(imk), Some(hmk), Some(ea)) = (
                    state.get(KEY_INDEX_MR_KEY),
                    state.get(KEY_HEAP_MR_KEY),
                    state.get(KEY_EP_ADDR),
                ) {
                    found = Some((
                        imk.parse::<u64>().expect("invalid index_mr_key"),
                        hmk.parse::<u64>().expect("invalid heap_mr_key"),
                        hex::decode(ea).expect("invalid ep_addr hex"),
                    ));
                    break;
                }
            }
        }
        drop(guard);
        if let Some(result) = found {
            break result;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    println!("Discovered server: index_mr_key=0x{index_mr_key:X}, heap_mr_key=0x{heap_mr_key:X}");

    // Initialize libfabric via the async endpoint wrapper.
    let endpoint = FabricEndpoint::new()?;
    let fi_addr = endpoint.av_insert(&ep_addr_bytes).expect("fi_av_insert failed");

    // Compute slot via MPH.
    let key_bytes = key.as_bytes().to_vec();
    let slot = mph.hash(&key_bytes) as usize;
    println!("MPH slot for \"{key}\": {slot}");

    // Step 1: async read of 12-byte index entry → (heap_offset: u64, value_len: u32).
    let index_bytes = endpoint
        .read(fi_addr, index_mr_key, (slot * 12) as u64, 12)
        .await
        .expect("fi_read (index) failed");
    let value_offset = u64::from_le_bytes(index_bytes[0..8].try_into().unwrap());
    let value_len = u32::from_le_bytes(index_bytes[8..12].try_into().unwrap()) as usize;
    println!("Index entry: heap_offset={value_offset}, value_len={value_len}");

    // Step 2: async read of value from heap.
    let value_bytes = endpoint
        .read(fi_addr, heap_mr_key, value_offset, value_len)
        .await
        .expect("fi_read (heap) failed");
    println!("{key} = {}", String::from_utf8_lossy(&value_bytes));

    handle.shutdown().await?;

    Ok(())
}
