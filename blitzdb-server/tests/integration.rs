use arrow_array::BinaryArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
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

fn write_parquet(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("test.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
    ]));
    let keys: Vec<&[u8]> = vec![b"hello", b"foo", b"blitz", b"rdma", b"fast"];
    let values: Vec<&[u8]> = vec![b"world", b"bar", b"fast", b"network", b"speed"];
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

#[test]
fn integration() {
    let dir = TempDir::new().unwrap();

    // 1. Generate parquet fixture in-process.
    let parquet_path = write_parquet(&dir);
    let prefix = dir.path().join("test");

    // 2. Run blitzdb-prepare to produce .mph / .index / .heap.
    let status = Command::new(cargo_bin("blitzdb-prepare"))
        .arg(&parquet_path)
        .arg(&prefix)
        .status()
        .unwrap();
    assert!(status.success(), "blitzdb-prepare failed");

    // 3. Create the NOTIFY_SOCKET listener before spawning the server.
    let notify_sock_path = dir.path().join("notify.sock");
    let notify_sock = UnixDatagram::bind(&notify_sock_path).unwrap();
    notify_sock
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();

    // 4. Spawn blitzdb-server on an ephemeral gossip port.
    let gossip_port = free_udp_port();
    let mut server = Command::new(cargo_bin("blitzdb-server"))
        .arg(prefix.to_str().unwrap())
        .arg(gossip_port.to_string())
        .env("NOTIFY_SOCKET", &notify_sock_path)
        .env("RUST_LOG", "error")
        .spawn()
        .expect("failed to spawn blitzdb-server");

    // 5. Block until READY=1 arrives (or timeout).
    let mut buf = [0u8; 256];
    let n = notify_sock
        .recv(&mut buf)
        .expect("timed out waiting for READY=1 from server");
    let msg = std::str::from_utf8(&buf[..n]).unwrap();
    assert!(msg.contains("READY=1"), "unexpected notify message: {msg:?}");

    // 6. Run one client lookup per key-value pair.
    let mph_path = dir.path().join("test.mph");
    let seed = format!("127.0.0.1:{gossip_port}");
    let cases = [
        ("hello", "world"),
        ("foo", "bar"),
        ("blitz", "fast"),
        ("rdma", "network"),
        ("fast", "speed"),
    ];

    for (key, want_value) in cases {
        let client_gossip_port = free_udp_port();
        let output = Command::new(cargo_bin("blitzdb-client"))
            .arg(mph_path.to_str().unwrap())
            .arg(key)
            .arg(&seed)
            .arg(client_gossip_port.to_string())
            .env("RUST_LOG", "info")
            .output()
            .expect("failed to run blitzdb-client");

        // env_logger writes to stderr; the key lookup result line is logged with info!.
        let stderr = String::from_utf8_lossy(&output.stderr);
        let want = format!("{key} = {want_value}");
        assert!(
            stderr.contains(&want),
            "lookup '{key}': expected '{want}' in client stderr\n--- stderr ---\n{stderr}"
        );
    }

    // 7. Shut down the server.
    server.kill().ok();
    server.wait().ok();
}
