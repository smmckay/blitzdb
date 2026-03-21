use blitzdb_common::*;
use blitzdb_fabric::FabricEndpoint;
use ofi_libfabric_sys::bindgen as ffi;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let prefix = args.next().expect("Usage: blitzdb-server <prefix> [gossip-port]");
    let gossip_port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10000);
    let gossip_addr: SocketAddr = format!("0.0.0.0:{gossip_port}").parse().unwrap();

    // Load index and heap files.
    let index_data =
        std::fs::read(format!("{prefix}.index")).expect("Failed to read .index file");
    let heap_data = std::fs::read(format!("{prefix}.heap")).expect("Failed to read .heap file");
    let n = index_data.len() / 12;
    println!("Loaded index: {n} entries ({} bytes)", index_data.len());
    println!("Loaded heap: {} bytes", heap_data.len());

    // Initialize libfabric. FabricEndpoint owns the resources and starts the CQ driver.
    let endpoint = FabricEndpoint::new()?;
    println!("Libfabric initialized (tcp provider, RDM endpoint, RMA enabled)");

    // Register memory regions (read-only for remote clients).
    let index_mr_guard = endpoint.mr_reg(0xBDB1, &index_data, ffi::FI_REMOTE_READ as u64)?;
    let index_mr_key = index_mr_guard.mr_key;
    println!("Registered index MR: key=0x{index_mr_key:X}");

    let heap_mr_key = endpoint.mr_reg(0xBDB2, &heap_data, ffi::FI_REMOTE_READ as u64)?;
    let heap_mr_key = heap_mr_key.mr_key;
    println!("Registered heap MR:  key=0x{heap_mr_key:X}");

    let ep_addr = endpoint.get_ep_addr()?;

    // Advertise via chitchat.
    let initial_key_values = vec![
        (KEY_INDEX_MR_KEY.to_string(), index_mr_key.to_string()),
        (KEY_HEAP_MR_KEY.to_string(), heap_mr_key.to_string()),
        (KEY_NUM_KEYS.to_string(), n.to_string()),
        (KEY_EP_ADDR.to_string(), hex::encode(&ep_addr)),
    ];
    let handle = cluster::start_chitchat("server", gossip_addr, vec![], initial_key_values).await?;
    println!("Chitchat listening on {gossip_addr}");
    println!("Server ready. Waiting for clients...");

    tokio::signal::ctrl_c().await?;
    println!("\nShutting down...");
    handle.shutdown().await?;

    Ok(())
}
