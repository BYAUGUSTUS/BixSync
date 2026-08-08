#![allow(nonstandard_style)]

use bixsync::*;
use notify::{Event, EventKind, RecursiveMode, Watcher, event::ModifyKind};
use std::{fs, io, net::UdpSocket, path::Path, sync::mpsc, thread, time::Duration};

// Loading peers
fn LoadPears() -> Vec<String> {
    if let Ok(text) = fs::read_to_string(PEERS_FILE) {
        serde_json::from_str(&text).unwrap_or_default()
    } else {
        vec![]
    }
}

// Saving peers
fn SavePeers(peers: &[String]) {
    if let Ok(json) = serde_json::to_string_pretty(peers) {
        let _ = fs::write(PEERS_FILE, json);
    }
}

// Folder watcher
fn FolderWatcher() -> notify::Result<()> {
    println!("Folder watcher started");

    let (tx, rx) = mpsc::channel::<notify::Result<Event>>();

    let mut watcher = notify::recommended_watcher(tx)?;

    watcher.watch(Path::new(SYNC_FOLDER_LOCATION), RecursiveMode::Recursive)?;

    for res in rx {
        match res {
            // Special case for modify event
            Ok(event) => match event.kind {
                EventKind::Create(_)
                | EventKind::Modify(ModifyKind::Data(_))
                | EventKind::Modify(ModifyKind::Name(_))
                | EventKind::Remove(_) => {
                    println!("Updated and ready for sync: {:?}", event.paths);
                }

                _ => {}
            },
            Err(e) => println!("Watch error: {:?}", e),
        }
    }

    Ok(())
}

// UDP broadcaster
fn Broadcaster() {
    let SOCKET = UdpSocket::bind("0.0.0.0:0").unwrap();

    println!("UDP broadcaster started");

    SOCKET.set_broadcast(true).unwrap();

    loop {
        let msg = format!(r#"{{"app":"bixsync","port":{}}}"#, PORT);

        let _ = SOCKET.send_to(msg.as_bytes(), format!("255.255.255.255:{}", PORT));

        thread::sleep(Duration::from_secs(5));
    }
}

// UDP discovery listner
fn DiscoveryListner() {
    let mut peers = LoadPears();

    let SOCKET = UdpSocket::bind(format!("0.0.0.0:{}", PORT)).unwrap();

    let mut buf = [0u8; 1024];

    println!("UDP listener started");

    loop {
        let (size, sender) = SOCKET.recv_from(&mut buf).unwrap();

        let msg = String::from_utf8_lossy(&buf[..size]);

        if !msg.contains("\"app\":\"bixsync\"") {
            continue;
        }

        let ip = sender.ip().to_string();

        if ip == SelfIpAddr.to_string() || peers.contains(&ip) {
            continue;
        }

        println!("Discovered {} -> {}", ip, msg);

        peers.push(ip);

        SavePeers(&peers);
    }
}

// -------------
// Main function
fn main() -> io::Result<()> {
    // Folder watcher
    thread::spawn(FolderWatcher);

    // UDP Broadcaster
    thread::spawn(Broadcaster);

    // UDP discovery listner
    thread::spawn(DiscoveryListner);

    loop {
        thread::park();
    }
}
