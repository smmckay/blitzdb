use arrow_array::BinaryArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use blitzdb_client::BlitzClient;
use parquet::arrow::ArrowWriter;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

const NUM_KEYS: u64 = 100_000_000;
const BATCH_SIZE: u64 = 1_000_000;
const NUM_CLIENTS: usize = 8;
const REPORT_INTERVAL: Duration = Duration::from_secs(10);
const TEST_DURATION: Duration = Duration::from_secs(60);

fn gen_key(index: u64) -> Vec<u8> {
    let mut rng = SmallRng::seed_from_u64(index);
    let len = rng.random_range(8..=32);
    let mut key = vec![0u8; len];
    rng.fill(&mut key[..]);
    key
}

fn gen_value(index: u64) -> Vec<u8> {
    let mut rng = SmallRng::seed_from_u64(index);
    let key_len: usize = rng.random_range(8..=32);
    let mut skip = vec![0u8; key_len];
    rng.fill(&mut skip[..]);
    let len = rng.random_range(32..=128);
    let mut val = vec![0u8; len];
    rng.fill(&mut val[..]);
    val
}

fn write_load_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
    ]));
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut batch_start = 0u64;
    while batch_start < NUM_KEYS {
        let batch_end = (batch_start + BATCH_SIZE).min(NUM_KEYS);
        let count = (batch_end - batch_start) as usize;

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(count);
        let mut values: Vec<Vec<u8>> = Vec::with_capacity(count);
        for i in batch_start..batch_end {
            keys.push(gen_key(i));
            values.push(gen_value(i));
        }

        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BinaryArray::from(key_refs)),
                Arc::new(BinaryArray::from(val_refs)),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();

        batch_start = batch_end;
        if batch_start % (BATCH_SIZE * 10) == 0 {
            eprintln!("  wrote {batch_start}/{NUM_KEYS} rows to parquet");
        }
    }
    writer.close().unwrap();
    eprintln!("  parquet complete: {NUM_KEYS} rows");
}

fn free_udp_port() -> u16 {
    UdpSocket::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_for_dataset(ds: &blitzdb_client::Dataset, timeout: Duration) {
    let key = gen_key(0);
    let start = tokio::time::Instant::now();
    loop {
        if let Ok(Some(_)) = ds.get(&key).await {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for dataset to become available");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn run_loadtest(seed: &str) -> anyhow::Result<()> {
    eprintln!("=== BlitzDB Load Test (remote) ===");
    eprintln!("Seed: {seed}");
    eprintln!("Keys: {NUM_KEYS}, Clients: {NUM_CLIENTS}, Duration: {}s", TEST_DURATION.as_secs());

    let client_port = free_udp_port();
    let client = BlitzClient::connect(seed, client_port).await?;

    let ds = client.dataset("load");
    eprintln!("Waiting for dataset discovery...");
    wait_for_dataset(&ds, Duration::from_secs(60)).await;
    eprintln!("  dataset discovered, launching clients\n");

    let stop = Arc::new(AtomicBool::new(false));
    let counters: Vec<Arc<AtomicU64>> =
        (0..NUM_CLIENTS).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let mut handles = Vec::new();
    for task_id in 0..NUM_CLIENTS {
        let ds = client.dataset("load");
        let stop = stop.clone();
        let counter = counters[task_id].clone();
        handles.push(tokio::spawn(async move {
            let mut rng = SmallRng::seed_from_u64(1000 + task_id as u64);
            while !stop.load(Ordering::Relaxed) {
                let hit = rng.random_bool(0.5);
                let index: u64 = rng.random_range(0..NUM_KEYS);
                let key = if hit {
                    gen_key(index)
                } else {
                    gen_key(index + NUM_KEYS)
                };
                let _ = ds.get(&key).await;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let num_reports = TEST_DURATION.as_secs() / REPORT_INTERVAL.as_secs();
    let mut prev: Vec<u64> = vec![0; NUM_CLIENTS];
    for tick in 1..=num_reports {
        tokio::time::sleep(REPORT_INTERVAL).await;
        let elapsed = tick * REPORT_INTERVAL.as_secs();
        let mut parts = Vec::new();
        for (i, ctr) in counters.iter().enumerate() {
            let cur = ctr.load(Ordering::Relaxed);
            let delta = cur - prev[i];
            let ops_sec = delta / REPORT_INTERVAL.as_secs();
            parts.push(format!("task {i}: {ops_sec} ops/s"));
            prev[i] = cur;
        }
        eprintln!("[{elapsed}s] {}", parts.join(" | "));
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    client.shutdown().await?;
    eprintln!("\n=== Load test complete ===");
    Ok(())
}

fn print_usage() -> ! {
    eprintln!("Usage:");
    eprintln!("  blitzdb-loadtest --generate <dir>          Generate parquet and prepare dataset");
    eprintln!("  blitzdb-loadtest <server-ip>:<gossip-port> Run load test against remote server");
    std::process::exit(1);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        print_usage();
    }

    if args[0] == "--generate" {
        let dir = args.get(1).unwrap_or_else(|| {
            eprintln!("--generate requires a directory argument");
            std::process::exit(1);
        });
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir)?;

        let parquet_path = dir.join("load.parquet");
        eprintln!("Phase 1: Generating parquet at {}...", parquet_path.display());
        write_load_parquet(&parquet_path);

        let prefix = dir.join("load");
        eprintln!("Phase 2: Preparing dataset (MPH + index + heap)...");
        blitzdb_prepare::prepare(&parquet_path, &prefix)?;
        eprintln!("  prepare complete");
        eprintln!("Dataset files written to {}", dir.display());
        return Ok(());
    }

    let seed = &args[0];
    run_loadtest(seed).await
}
