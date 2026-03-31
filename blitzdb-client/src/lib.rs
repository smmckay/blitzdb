use blitzdb_common::*;
use boomphf::Mphf;
use chitchat::ChitchatHandle;
use std::net::SocketAddr;
use std::time::Duration;
use log::info;

pub struct BlitzClient {
    endpoint: FabricEndpoint,
    fi_addr: u64,
    index_mr_key: u64,
    heap_mr_key: u64,
    mph: Mphf<Vec<u8>>,
    _handle: ChitchatHandle,
}

impl BlitzClient {
    pub async fn connect(seed: &str, gossip_port: u16) -> anyhow::Result<Self> {
        let gossip_addr: SocketAddr = format!("0.0.0.0:{gossip_port}").parse()?;
        let handle =
            cluster::start_chitchat("client", gossip_addr, vec![seed.to_string()], vec![])
                .await?;
        info!("Chitchat started, seed={seed}, waiting for server...");

        let (index_mr_key, heap_mr_key, mph_mr_key, mph_len, ep_addr_bytes) = loop {
            let chitchat = handle.chitchat();
            let guard = chitchat.lock().await;
            let mut found = None;
            for node_id in guard.live_nodes() {
                if let Some(state) = guard.node_state(node_id) {
                    if let (Some(imk), Some(hmk), Some(mmk), Some(ml), Some(ea)) = (
                        state.get(KEY_INDEX_MR_KEY),
                        state.get(KEY_HEAP_MR_KEY),
                        state.get(KEY_MPH_MR_KEY),
                        state.get(KEY_MPH_LEN),
                        state.get(KEY_EP_ADDR),
                    ) {
                        found = Some((
                            imk.parse::<u64>()?,
                            hmk.parse::<u64>()?,
                            mmk.parse::<u64>()?,
                            ml.parse::<usize>()?,
                            hex::decode(ea)?,
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
        info!("Discovered server: index_mr_key=0x{index_mr_key:X}, heap_mr_key=0x{heap_mr_key:X}, mph_mr_key=0x{mph_mr_key:X}");

        let endpoint = FabricEndpoint::new()?;
        let fi_addr = endpoint.av_insert(&ep_addr_bytes)?;

        let mph_bytes = endpoint.read(fi_addr, mph_mr_key, 0, mph_len).await?;
        let mph: Mphf<Vec<u8>> = bincode::deserialize(&mph_bytes)?;
        info!("Fetched and deserialized MPH ({mph_len} bytes)");

        Ok(Self { endpoint, fi_addr, index_mr_key, heap_mr_key, mph, _handle: handle })
    }

    pub async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let maybe_slot = self.mph.try_hash(key);
        if maybe_slot.is_none() {
            return Ok(None);
        }
        let slot = maybe_slot.unwrap() as usize;
        info!("MPH slot for key: {slot}");

        let entry_sz = size_of::<IndexEntry>();
        let index_entry = self
            .endpoint
            .readT::<IndexEntry>(self.fi_addr, self.index_mr_key, (slot * entry_sz) as u64)
            .await?;
        info!("Index entry: {index_entry}");

        if index_entry.matches(key) == false {
            return Ok(None);
        }

        let value_bytes = self
            .endpoint
            .read(self.fi_addr, self.heap_mr_key, index_entry.offset, index_entry.len as usize)
            .await?;

        Ok(Some(value_bytes))
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        self._handle.shutdown().await?;
        Ok(())
    }
}
