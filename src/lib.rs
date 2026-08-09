#![allow(nonstandard_style)]

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::{IpAddr, UdpSocket},
    sync::LazyLock,
};

pub static SYNC_FOLDER_LOCATION: &str = "/home/ishank/bixsync";

pub const PORT: u16 = 2637;
pub const PEERS_FILE: &str = "peers.json";

pub static mut Peers: Vec<String> = Vec::new();

pub static SelfIpAddr: LazyLock<IpAddr> = LazyLock::new(|| {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("8.8.8.8:80").unwrap();
    socket.local_addr().unwrap().ip()
});

#[derive(Clone, Copy, PartialEq)]
pub enum SyncState {
    Receiving,
    Active,
}

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub files: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize)]
pub enum SyncMessage {
    RequestManifest,
    Manifest(Manifest),
    RequestFile(String),
    FileContent(String, Vec<u8>),
}