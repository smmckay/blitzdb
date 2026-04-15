use blitzdb_common::*;
use boomphf::Mphf;
use chitchat::ChitchatHandle;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use log::{info, warn};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use blitzdb_common::endpoint::FabricRecvBuffer;

struct ServerConn {
    fi_addr: u64,
    index_mr_key: u64,
    heap_mr_key: u64,
    mph: Mphf<Vec<u8>>,
}

pub struct BlitzClient {
    endpoint: Arc<FabricEndpoint>,
    servers: Arc<RwLock<HashMap<String, ServerConn>>>,
    _handle: ChitchatHandle,
    _watcher: JoinHandle<()>,
}

pub struct Dataset {
    name: String,
    endpoint: Arc<FabricEndpoint>,
    servers: Arc<RwLock<HashMap<String, ServerConn>>>,
}

impl BlitzClient {
    pub async fn connect(seed: &str, gossip_port: u16) -> anyhow::Result<Self> {
        let gossip_addr: SocketAddr = format!("0.0.0.0:{gossip_port}").parse()?;
        let handle =
            cluster::start_chitchat("client", gossip_addr, vec![seed.to_string()], vec![])
                .await?;
        info!("Chitchat started, seed={seed}");

        let endpoint = Arc::new(FabricEndpoint::new(4096)?);
        let servers: Arc<RwLock<HashMap<String, ServerConn>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let watcher = spawn_watcher(handle.chitchat(), endpoint.clone(), servers.clone());

        Ok(Self {
            endpoint,
            servers,
            _handle: handle,
            _watcher: watcher,
        })
    }

    pub fn dataset(&self, name: &str) -> Dataset {
        Dataset {
            name: name.to_string(),
            endpoint: self.endpoint.clone(),
            servers: self.servers.clone(),
        }
    }

    pub fn get_recv_buffer(&'_ self) -> Result<FabricRecvBuffer<'_>> {
        self.endpoint.get_recv_buffer()
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        self._watcher.abort();
        self._handle.shutdown().await?;
        Ok(())
    }
}

fn spawn_watcher(
    chitchat: Arc<tokio::sync::Mutex<chitchat::Chitchat>>,
    endpoint: Arc<FabricEndpoint>,
    servers: Arc<RwLock<HashMap<String, ServerConn>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Track which chitchat node IDs we've already processed, mapped to their dataset name.
        let mut known_nodes: HashMap<String, String> = HashMap::new();

        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;

            let mut live_node_ids = HashSet::new();
            let mut new_servers: Vec<(String, String, u64, u64, u64, usize, Vec<u8>)> = Vec::new();

            {
                let guard = chitchat.lock().await;
                for node_id in guard.live_nodes() {
                    let id_str = node_id.node_id.clone();
                    live_node_ids.insert(id_str.clone());

                    if known_nodes.contains_key(&id_str) {
                        continue;
                    }

                    if let Some(state) = guard.node_state(node_id) {
                        if let (Some(ds), Some(imk), Some(hmk), Some(mmk), Some(ml), Some(ea)) = (
                            state.get(KEY_DATASET),
                            state.get(KEY_INDEX_MR_KEY),
                            state.get(KEY_HEAP_MR_KEY),
                            state.get(KEY_MPH_MR_KEY),
                            state.get(KEY_MPH_LEN),
                            state.get(KEY_EP_ADDR),
                        ) {
                            if let (Ok(imk), Ok(hmk), Ok(mmk), Ok(ml), Ok(ea)) = (
                                imk.parse::<u64>(),
                                hmk.parse::<u64>(),
                                mmk.parse::<u64>(),
                                ml.parse::<usize>(),
                                hex::decode(ea),
                            ) {
                                new_servers.push((
                                    id_str,
                                    ds.to_string(),
                                    imk,
                                    hmk,
                                    mmk,
                                    ml,
                                    ea,
                                ));
                            }
                        }
                    }
                }
            }

            // Handle new servers (outside chitchat lock since av_insert + RDMA read are slow)
            for (node_id, dataset, index_mr_key, heap_mr_key, mph_mr_key, mph_len, ep_addr_bytes) in
                new_servers
            {
                match connect_server(&endpoint, index_mr_key, heap_mr_key, mph_mr_key, mph_len, &ep_addr_bytes).await {
                    Ok(conn) => {
                        info!("Discovered server for dataset '{dataset}': index_mr_key=0x{index_mr_key:X}, heap_mr_key=0x{heap_mr_key:X}");
                        servers.write().await.insert(dataset.clone(), conn);
                        known_nodes.insert(node_id, dataset);
                    }
                    Err(e) => {
                        warn!("Failed to connect to server for dataset '{dataset}': {e}");
                    }
                }
            }

            // Handle departed servers
            let departed: Vec<String> = known_nodes
                .keys()
                .filter(|id| !live_node_ids.contains(*id))
                .cloned()
                .collect();
            for node_id in departed {
                if let Some(dataset) = known_nodes.remove(&node_id) {
                    info!("Server for dataset '{dataset}' departed");
                    servers.write().await.remove(&dataset);
                }
            }
        }
    })
}

async fn connect_server(
    endpoint: &FabricEndpoint,
    index_mr_key: u64,
    heap_mr_key: u64,
    mph_mr_key: u64,
    mph_len: usize,
    ep_addr_bytes: &[u8],
) -> anyhow::Result<ServerConn> {
    let fi_addr = endpoint.av_insert(ep_addr_bytes)?;
    let mph_bytes = endpoint.bulk_read(fi_addr, mph_mr_key, 0, mph_len).await?;
    let mph: Mphf<Vec<u8>> = bincode::deserialize(&mph_bytes)?;
    info!("Fetched MPH ({mph_len} bytes)");
    Ok(ServerConn {
        fi_addr,
        index_mr_key,
        heap_mr_key,
        mph,
    })
}

impl Dataset {
    pub async fn get<'a>(&self, key: &[u8], buf: &mut FabricRecvBuffer<'a>) -> anyhow::Result<Option<&'a [u8]>> {
        let guard = self.servers.read().await;
        let conn = match guard.get(&self.name) {
            Some(c) => c,
            None => return Ok(None),
        };

        let maybe_slot = conn.mph.try_hash(key);
        if maybe_slot.is_none() {
            return Ok(None);
        }
        let slot = maybe_slot.unwrap() as usize;
        info!("MPH slot for key: {slot}");

        let (offset, len) = {
            let entry_sz = size_of::<IndexEntry>();
            let index_entry = self
                .endpoint
                .read_value::<IndexEntry>(conn.fi_addr, conn.index_mr_key, (slot * entry_sz) as u64, buf)
                .await?;
            info!("Index entry: {index_entry}");

            if index_entry.matches(key) == false {
                return Ok(None);
            }

            (index_entry.offset, index_entry.len)
        };


        let value_bytes = self
            .endpoint
            .read(conn.fi_addr, conn.heap_mr_key, offset, len as usize, buf)
            .await?;

        Ok(Some(value_bytes))
    }

    pub fn get_recv_buffer(&'_ self) -> Result<FabricRecvBuffer<'_>> {
        self.endpoint.get_recv_buffer()
    }
}
