use arrow_array::BinaryArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use blitzdb_client::BlitzClient;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::net::UdpSocket;
use std::os::unix::net::UnixDatagram;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// Locate a workspace binary from the integration test's working directory.
/// Integration tests run from `target/debug/deps/`; binaries live in `target/debug/`.
fn cargo_bin(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push(name);
    path
}

/// Bind an ephemeral UDP socket to find a free port, then release it.
fn free_udp_port() -> u16 {
    UdpSocket::bind("0.0.0.0:0").unwrap().local_addr().unwrap().port()
}

fn write_parquet(dir: &TempDir, keys: Vec<&[u8]>, values: Vec<&[u8]>, name: &str) -> PathBuf {
    let path = dir.path().join(format!("{name}.parquet"));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BinaryArray::from(keys)),
            Arc::new(BinaryArray::from(values)),
        ],
    )
    .unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

struct ServerHandle {
    child: std::process::Child,
}

impl ServerHandle {
    fn spawn(
        dir: &TempDir,
        keys: Vec<&[u8]>,
        values: Vec<&[u8]>,
        dataset: &str,
        gossip_port: u16,
        seed: Option<u16>,
    ) -> Self {
        let parquet_path = write_parquet(dir, keys, values, dataset);
        let prefix = dir.path().join(dataset);
        blitzdb_prepare::prepare(&parquet_path, &prefix).expect("blitzdb-prepare failed");

        let notify_sock_path = dir.path().join(format!("notify-{dataset}.sock"));
        let notify_sock = UnixDatagram::bind(&notify_sock_path).unwrap();
        notify_sock
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();

        let mut cmd = Command::new(cargo_bin("blitzdb-server"));
        cmd.arg(prefix.to_str().unwrap())
            .arg("--dataset").arg(dataset)
            .arg("--gossip-port").arg(gossip_port.to_string())
            .env("NOTIFY_SOCKET", &notify_sock_path)
            .env("RUST_LOG", "info");

        if let Some(seed_port) = seed {
            cmd.arg("--seed").arg(format!("127.0.0.1:{seed_port}"));
        }

        let child = cmd.spawn().expect("failed to spawn blitzdb-server");

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

#[test]
fn integration() {
    let dir = TempDir::new().unwrap();

    let gossip_port1 = free_udp_port();
    let gossip_port2 = free_udp_port();

    let _server1 = ServerHandle::spawn(
        &dir,
        vec![b"hello", b"foo", b"blitz", b"rdma", b"fast"],
        vec![b"world", b"bar", b"fast", b"network", b"speed"],
        "ds1",
        gossip_port1,
        None,
    );

    let _server2 = ServerHandle::spawn(
        &dir,
        vec![b"alpha", b"beta", b"gamma"],
        vec![b"one", b"two", b"three"],
        "ds2",
        gossip_port2,
        Some(gossip_port1),
    );

    let seed = format!("127.0.0.1:{gossip_port1}");
    let client_gossip_port = free_udp_port();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = BlitzClient::connect(&seed, client_gossip_port)
            .await
            .expect("BlitzClient::connect failed");

        // Wait for the background watcher to discover servers
        let ds1 = client.dataset("ds1");
        wait_for_dataset(&ds1, b"hello", Duration::from_secs(10)).await;

        // Query ds1
        for (key, want) in [("hello", "world"), ("foo", "bar"), ("blitz", "fast"), ("rdma", "network"), ("fast", "speed")] {
            let value = ds1.get(key.as_bytes()).await.expect("get failed");
            assert!(value.is_some(), "ds1 lookup '{key}' returned no value");
            assert_eq!(value.unwrap(), want.as_bytes(), "ds1 lookup '{key}' wrong value");
        }
        let value = ds1.get(b"nonexistent").await.expect("get failed");
        assert!(value.is_none(), "ds1 lookup 'nonexistent' returned a value");

        // Query ds2
        let ds2 = client.dataset("ds2");
        wait_for_dataset(&ds2, b"alpha", Duration::from_secs(10)).await;

        for (key, want) in [("alpha", "one"), ("beta", "two"), ("gamma", "three")] {
            let value = ds2.get(key.as_bytes()).await.expect("get failed");
            assert!(value.is_some(), "ds2 lookup '{key}' returned no value");
            assert_eq!(value.unwrap(), want.as_bytes(), "ds2 lookup '{key}' wrong value");
        }

        // ds1 keys should not exist in ds2
        let value = ds2.get(b"hello").await.expect("get failed");
        assert!(value.is_none(), "ds2 should not contain ds1 key 'hello'");

        client.shutdown().await.expect("shutdown failed");
    });
}

#[test]
fn late_join() {
    let dir = TempDir::new().unwrap();

    let gossip_port1 = free_udp_port();
    let seed = format!("127.0.0.1:{gossip_port1}");

    // Start server1 so the client has a seed to connect to
    let _server1 = ServerHandle::spawn(
        &dir,
        vec![b"hello"],
        vec![b"world"],
        "ds1",
        gossip_port1,
        None,
    );

    let client_gossip_port = free_udp_port();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = BlitzClient::connect(&seed, client_gossip_port)
            .await
            .expect("BlitzClient::connect failed");

        // ds_late doesn't exist yet — get() should return Ok(None)
        let ds_late = client.dataset("ds_late");
        let value = ds_late.get(b"alpha").await.expect("get failed");
        assert!(value.is_none(), "ds_late should return None before server is up");

        // Now start a server publishing ds_late
        let gossip_port2 = free_udp_port();
        let _server2 = ServerHandle::spawn(
            &dir,
            vec![b"alpha", b"beta"],
            vec![b"one", b"two"],
            "ds_late",
            gossip_port2,
            Some(gossip_port1),
        );

        // Poll until the watcher picks up the new server
        wait_for_dataset(&ds_late, b"alpha", Duration::from_secs(10)).await;

        let value = ds_late.get(b"alpha").await.expect("get failed");
        assert_eq!(value.unwrap(), b"one");

        let value = ds_late.get(b"beta").await.expect("get failed");
        assert_eq!(value.unwrap(), b"two");

        client.shutdown().await.expect("shutdown failed");
    });
}

/// Poll until a dataset returns a value for the given key, or panic after timeout.
async fn wait_for_dataset(ds: &blitzdb_client::Dataset, key: &[u8], timeout: Duration) {
    let start = tokio::time::Instant::now();
    loop {
        if let Ok(Some(_)) = ds.get(key).await {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for dataset to become available");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
