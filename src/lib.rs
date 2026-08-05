use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub static SYNC_FOLDER_LOCATION: &str = "/home/ishank/bixsync";

pub const PORT: u16 = 2637;
pub const PEERS_FILE: &str = "peers.json";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Peer {
    pub last_seen: u64,
}

pub type PeerMap = HashMap<String, Peer>;