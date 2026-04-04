use blitzdb_common::*;
use clap::Parser;
use ofi_libfabric_sys::bindgen as ffi;
use std::net::SocketAddr;
use log::info;

#[derive(Parser)]
#[command(about = "BlitzDB server")]
struct Args {
    /// Path prefix for .index, .heap, .mph files
    prefix: String,

    /// Dataset name
    #[arg(long)]
    dataset: String,

    /// Seed node address for gossip cluster
    #[arg(long, env = "BLITZDB_SEED")]
    seed: Option<String>,

    /// Gossip port
    #[arg(long, default_value_t = 10000)]
    gossip_port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::setup_logging();

    let args = Args::parse();
    let prefix = &args.prefix;
    let dataset = &args.dataset;
    let seed_nodes: Vec<String> = args.seed.into_iter().collect();
    let gossip_addr: SocketAddr = format!("0.0.0.0:{}", args.gossip_port).parse().unwrap();

    // Load index, heap, and mph files.
    let index_data =
        std::fs::read(format!("{prefix}.index")).expect("Failed to read .index file");
    let heap_data = std::fs::read(format!("{prefix}.heap")).expect("Failed to read .heap file");
    let mph_data = std::fs::read(format!("{prefix}.mph")).expect("Failed to read .mph file");
    let n = index_data.len() / 12;
    info!("Loaded index: {n} entries ({} bytes)", index_data.len());
    info!("Loaded heap: {} bytes", heap_data.len());
    info!("Loaded MPH: {} bytes", mph_data.len());

    // Initialize libfabric. FabricEndpoint owns the resources and starts the CQ driver.
    let endpoint = FabricEndpoint::new(1024)?;
    info!("Libfabric initialized");

    // Register memory regions (read-only for remote clients).
    let index_mr_guard = endpoint.mr_reg(0xBDB1, &index_data, ffi::FI_REMOTE_READ as u64)?;
    let index_mr_key = index_mr_guard.mr_key;
    info!("Registered index MR: key=0x{index_mr_key:X}");

    let heap_mr_guard = endpoint.mr_reg(0xBDB2, &heap_data, ffi::FI_REMOTE_READ as u64)?;
    let heap_mr_key = heap_mr_guard.mr_key;
    info!("Registered heap MR:  key=0x{heap_mr_key:X}");

    let mph_mr_guard = endpoint.mr_reg(0xBDB3, &mph_data, ffi::FI_REMOTE_READ as u64)?;
    let mph_mr_key = mph_mr_guard.mr_key;
    info!("Registered MPH MR:   key=0x{mph_mr_key:X}");

    let ep_addr = endpoint.get_ep_addr()?;

    // Advertise via chitchat.
    let initial_key_values = vec![
        (KEY_DATASET.to_string(), dataset.clone()),
        (KEY_INDEX_MR_KEY.to_string(), index_mr_key.to_string()),
        (KEY_HEAP_MR_KEY.to_string(), heap_mr_key.to_string()),
        (KEY_MPH_MR_KEY.to_string(), mph_mr_key.to_string()),
        (KEY_MPH_LEN.to_string(), mph_data.len().to_string()),
        (KEY_NUM_KEYS.to_string(), n.to_string()),
        (KEY_EP_ADDR.to_string(), hex::encode(&ep_addr)),
    ];
    let handle = cluster::start_chitchat("server", gossip_addr, seed_nodes, initial_key_values).await?;
    info!("Chitchat listening on {gossip_addr}");
    info!("Server ready. Waiting for clients...");
    let _ = sd_notify::notify(&[sd_notify::NotifyState::Ready]);

    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    handle.shutdown().await?;

    Ok(())
}
