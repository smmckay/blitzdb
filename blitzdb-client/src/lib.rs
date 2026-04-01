use blitzdb_common::*;
use boomphf::Mphf;
use chitchat::ChitchatHandle;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use log::info;

struct ServerConn {
    fi_addr: u64,
    index_mr_key: u64,
    heap_mr_key: u64,
    mph: Mphf<Vec<u8>>,
}

pub struct BlitzClient {
    endpoint: FabricEndpoint,
    servers: HashMap<String, ServerConn>,
    _handle: ChitchatHandle,
}

pub struct Dataset<'a> {
    endpoint: &'a FabricEndpoint,
    conn: &'a ServerConn,
}

impl BlitzClient {
    pub async fn connect(seed: &str, gossip_port: u16) -> anyhow::Result<Self> {
        let gossip_addr: SocketAddr = format!("0.0.0.0:{gossip_port}").parse()?;
        let handle =
            cluster::start_chitchat("client", gossip_addr, vec![seed.to_string()], vec![])
                .await?;
        info!("Chitchat started, seed={seed}, waiting for servers...");

        // Discover all servers. Wait until at least one is found, then do one
        // extra pass to catch any stragglers.
        let mut discovered: HashMap<String, (u64, u64, u64, usize, Vec<u8>)> = HashMap::new();
        let mut extra_passes = 0;
        loop {
            {
                let chitchat = handle.chitchat();
                let guard = chitchat.lock().await;
                for node_id in guard.live_nodes() {
                    if let Some(state) = guard.node_state(node_id) {
                        if let (Some(ds), Some(imk), Some(hmk), Some(mmk), Some(ml), Some(ea)) = (
                            state.get(KEY_DATASET),
                            state.get(KEY_INDEX_MR_KEY),
                            state.get(KEY_HEAP_MR_KEY),
                            state.get(KEY_MPH_MR_KEY),
                            state.get(KEY_MPH_LEN),
                            state.get(KEY_EP_ADDR),
                        ) {
                            if !discovered.contains_key(ds) {
                                discovered.insert(ds.to_string(), (
                                    imk.parse::<u64>()?,
                                    hmk.parse::<u64>()?,
                                    mmk.parse::<u64>()?,
                                    ml.parse::<usize>()?,
                                    hex::decode(ea)?,
                                ));
                            }
                        }
                    }
                }
            }
            if !discovered.is_empty() {
                extra_passes += 1;
                if extra_passes >= 3 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let endpoint = FabricEndpoint::new()?;
        let mut servers = HashMap::new();
        for (dataset, (index_mr_key, heap_mr_key, mph_mr_key, mph_len, ep_addr_bytes)) in discovered {
            info!("Discovered server for dataset '{dataset}': index_mr_key=0x{index_mr_key:X}, heap_mr_key=0x{heap_mr_key:X}");
            let fi_addr = endpoint.av_insert(&ep_addr_bytes)?;
            let mph_bytes = endpoint.read(fi_addr, mph_mr_key, 0, mph_len).await?;
            let mph: Mphf<Vec<u8>> = bincode::deserialize(&mph_bytes)?;
            info!("Fetched MPH for dataset '{dataset}' ({mph_len} bytes)");
            servers.insert(dataset, ServerConn { fi_addr, index_mr_key, heap_mr_key, mph });
        }

        Ok(Self { endpoint, servers, _handle: handle })
    }

    pub fn dataset(&self, name: &str) -> Option<Dataset<'_>> {
        self.servers.get(name).map(|conn| Dataset { endpoint: &self.endpoint, conn })
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        self._handle.shutdown().await?;
        Ok(())
    }
}

impl Dataset<'_> {
    pub async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let maybe_slot = self.conn.mph.try_hash(key);
        if maybe_slot.is_none() {
            return Ok(None);
        }
        let slot = maybe_slot.unwrap() as usize;
        info!("MPH slot for key: {slot}");

        let entry_sz = size_of::<IndexEntry>();
        let index_entry = self
            .endpoint
            .readT::<IndexEntry>(self.conn.fi_addr, self.conn.index_mr_key, (slot * entry_sz) as u64)
            .await?;
        info!("Index entry: {index_entry}");

        if index_entry.matches(key) == false {
            return Ok(None);
        }

        let value_bytes = self
            .endpoint
            .read(self.conn.fi_addr, self.conn.heap_mr_key, index_entry.offset, index_entry.len as usize)
            .await?;

        Ok(Some(value_bytes))
    }
}
