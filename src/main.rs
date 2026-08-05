#![allow(nonstandard_style)]

use bixsync::*;
use notify::{Event, RecursiveMode, Watcher};
use std::{
    collections::HashMap,
    fs, io,
    net::{TcpListener, UdpSocket},
    path::Path,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

// Folder watcher
fn FolderWatcher() -> notify::Result<()> {
    let (tx, rx) = mpsc::channel::<notify::Result<Event>>();

    let mut watcher = notify::recommended_watcher(tx)?;

    watcher.watch(Path::new(SYNC_FOLDER_LOCATION), RecursiveMode::Recursive)?;

    for res in rx {
        match res {
            Ok(event) => println!("File event: {:?}", event),
            Err(e) => println!("Watch error: {:?}", e),
        }
    }

    Ok(())
}

// Loading peers
fn LoadPears() -> PeerMap {
    if let Ok(text) = fs::read_to_string(PEERS_FILE) {
        serde_json::from_str(&text).unwrap_or_default()
    } else {
        HashMap::new()
    }
}

// Saving peers
fn SavePeers(peers: &PeerMap) {
    if let Ok(json) = serde_json::to_string_pretty(peers) {
        let _ = fs::write(PEERS_FILE, json);
    }
}

// UDP broadcaster
fn Broadcaster() {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    socket.set_broadcast(true).unwrap();

    loop {
        let msg = format!(r#"{{"app":"bixsync","port":{}}}"#, PORT);

        let _ = socket.send_to(msg.as_bytes(), format!("255.255.255.255:{}", PORT));

        thread::sleep(Duration::from_secs(5));
    }
}

// UDP discovery listner
fn DiscoveryListner(peers: Arc<Mutex<PeerMap>>) {
    let socket = UdpSocket::bind(format!("0.0.0.0:{}", PORT)).unwrap();

    let mut buf = [0u8; 1024];

    println!("UDP listener started");

    loop {
        let (size, sender) = socket.recv_from(&mut buf).unwrap();

        let msg = String::from_utf8_lossy(&buf[..size]);

        if !msg.contains("\"app\":\"bixsync\"") {
            continue;
        }

        let ip = sender.ip().to_string();

        println!("Discovered {} -> {}", ip, msg);

        let mut map = peers.lock().unwrap();

        map.insert(
            ip,
            Peer {
                last_seen: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
        );

        SavePeers(&map);
    }
}

// -------------
// Main function
fn main() -> io::Result<()> {
    let peers = Arc::new(Mutex::new(LoadPears()));

    // Folder watcher
    thread::spawn(|| {
        println!("Starting folder watcher...");
        FolderWatcher().unwrap();
    });

    // UDP Broadcaster
    thread::spawn(|| {
        println!("Starting broadcaster...");
        Broadcaster();
    });

    // UDP discovery listner
    {
        let peers = peers.clone();
        thread::spawn(move || {
            DiscoveryListner(peers);
        });
    }

    // TCP server
    println!("TCP server listening on {}", PORT);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT))?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("TCP connection from {}", stream.peer_addr()?);
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}
