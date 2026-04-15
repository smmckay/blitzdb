use arrow_array::BinaryArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use blitzdb_client::BlitzClient;
use parquet::arrow::ArrowWriter;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::net::UdpSocket;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const NUM_KEYS: u64 = 100_000_000;
const BATCH_SIZE: u64 = 1_000_000;
const NUM_CLIENTS: usize = 8;
const REPORT_INTERVAL: Duration = Duration::from_secs(10);
const TEST_DURATION: Duration = Duration::from_secs(60);

fn cargo_bin(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push(name);
    path
}

fn free_udp_port() -> u16 {
    UdpSocket::bind("0.0.0.0:0").unwrap().local_addr().unwrap().port()
}

fn gen_key(index: u64) -> Vec<u8> {
    let mut rng = SmallRng::seed_from_u64(index);
    let len = rng.random_range(8..=32);
    let mut key = vec![0u8; len];
    rng.fill(&mut key[..]);
    key
}

fn gen_value(index: u64) -> Vec<u8> {
    let mut rng = SmallRng::seed_from_u64(index);
    // Advance past the key generation
    let key_len: usize = rng.random_range(8..=32);
    let mut skip = vec![0u8; key_len];
    rng.fill(&mut skip[..]);
    // Now generate value
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
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

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

struct ServerHandle {
    child: std::process::Child,
}

impl ServerHandle {
    fn spawn_prepared(
        dir: &TempDir,
        prefix: &Path,
        dataset: &str,
        gossip_port: u16,
    ) -> Self {
        let notify_sock_path = dir.path().join(format!("notify-{dataset}.sock"));
        let notify_sock = UnixDatagram::bind(&notify_sock_path).unwrap();
        notify_sock
            .set_read_timeout(Some(Duration::from_secs(120)))
            .unwrap();

        let child = Command::new(cargo_bin("blitzdb-server"))
            .arg(prefix.to_str().unwrap())
            .arg("--dataset")
            .arg(dataset)
            .arg(gossip_port.to_string())
            .env("NOTIFY_SOCKET", &notify_sock_path)
            .env("RUST_LOG", "error")
            .spawn()
            .expect("failed to spawn blitzdb-server");

        let mut buf = [0u8; 256];
        let n = notify_sock
            .recv(&mut buf)
            .expect("timed out waiting for READY=1 from server");
        let msg = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(msg.contains("READY=1"), "unexpected notify message: {msg:?}");

        Self { child }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

async fn wait_for_dataset(ds: &blitzdb_client::Dataset, timeout: Duration) {
    let key = gen_key(0);
    let start = tokio::time::Instant::now();
    let mut buf = ds.get_recv_buffer().expect("get_recv_buffer");
    loop {
        if let Ok(Some(_)) = ds.get(&key, &mut buf).await {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for dataset to become available");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[test]
#[ignore]
fn loadtest() {
    eprintln!("=== BlitzDB Load Test ===");
    eprintln!("Keys: {NUM_KEYS}, Clients: {NUM_CLIENTS}, Duration: {}s", TEST_DURATION.as_secs());

    let dir = TempDir::new().unwrap();

    // Phase 1: Generate parquet
    eprintln!("\nPhase 1: Generating parquet...");
    let parquet_path = dir.path().join("load.parquet");
    write_load_parquet(&parquet_path);

    // Phase 2: Prepare dataset
    eprintln!("\nPhase 2: Preparing dataset (MPH + index + heap)...");
    let prefix = dir.path().join("load");
    blitzdb_prepare::prepare(&parquet_path, &prefix).expect("blitzdb-prepare failed");
    eprintln!("  prepare complete");

    // Phase 3: Start server
    eprintln!("\nPhase 3: Starting server...");
    let gossip_port = free_udp_port();
    let _server = ServerHandle::spawn_prepared(&dir, &prefix, "load", gossip_port);
    eprintln!("  server ready on gossip port {gossip_port}");

    // Phase 4: Run load test
    eprintln!("\nPhase 4: Running load test...");
    let seed = format!("127.0.0.1:{gossip_port}");
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client_port = free_udp_port();
        let client = BlitzClient::connect(&seed, client_port)
            .await
            .expect("BlitzClient::connect failed");

        let ds = client.dataset("load");
        wait_for_dataset(&ds, Duration::from_secs(30)).await;
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
                let mut buf = ds.get_recv_buffer().expect("get_recv_buffer");
                while !stop.load(Ordering::Relaxed) {
                    let hit = rng.random_bool(0.5);
                    let index: u64 = rng.random_range(0..NUM_KEYS);
                    let key = if hit {
                        gen_key(index)
                    } else {
                        gen_key(index + NUM_KEYS)
                    };
                    let _ = ds.get(&key, &mut buf).await;
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

        client.shutdown().await.expect("shutdown failed");
    });

    eprintln!("\n=== Load test complete ===");
}
