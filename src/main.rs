#![allow(nonstandard_style)]

use bixsync::*;
use notify::{Event, EventKind, RecursiveMode, Watcher, event::ModifyKind};
use std::{
    collections::HashMap,
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream, UdpSocket},
    path::{Path, PathBuf},
    sync::{Arc, RwLock, mpsc},
    thread,
    time::Duration,
};

// Send message
fn SendMessage(STREAM: &mut TcpStream, MSG: &SyncMessage) {
    let JSON = serde_json::to_string(MSG).unwrap();
    let SIZE = JSON.len() as u32;
    let _ = STREAM.write_all(&SIZE.to_be_bytes());
    let _ = STREAM.write_all(JSON.as_bytes());
}

// Receive message
fn ReceiveMessage(STREAM: &mut TcpStream) -> Option<SyncMessage> {
    let mut sizeBuf = [0u8; 4];
    if STREAM.read_exact(&mut sizeBuf).is_ok() {
        let SIZE = u32::from_be_bytes(sizeBuf) as usize;
        let mut dataBuf = vec![0u8; SIZE];
        if STREAM.read_exact(&mut dataBuf).is_ok() {
            if let Ok(MSG) = serde_json::from_slice(&dataBuf) {
                return Some(MSG);
            }
        }
    }
    None
}

// Load peers
fn LoadPears() -> Vec<String> {
    if let Ok(TEXT) = fs::read_to_string(PEERS_FILE) {
        serde_json::from_str(&TEXT).unwrap_or_default()
    } else {
        vec![]
    }
}

// Save peers
fn SavePeers(PEERS: &[String]) {
    if let Ok(JSON) = serde_json::to_string_pretty(PEERS) {
        let _ = fs::write(PEERS_FILE, JSON);
    }
}

// Generate Manifest
fn GenerateManifest() -> Manifest {
    let mut files = HashMap::new();
    if let Ok(ENTRIES) = fs::read_dir(SYNC_FOLDER_LOCATION) {
        for ENTRY in ENTRIES.flatten() {
            if let Ok(METADATA) = ENTRY.metadata() {
                if METADATA.is_file() {
                    let MODIFIED = METADATA
                        .modified()
                        .unwrap()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    files.insert(ENTRY.file_name().to_string_lossy().to_string(), MODIFIED);
                }
            }
        }
    }
    Manifest { files }
}

// TCP Sync Server
fn TcpSyncServer(STATE: Arc<RwLock<SyncState>>) {
    let LISTENER = TcpListener::bind(format!("0.0.0.0:{}", PORT + 1)).unwrap();
    println!("TCP Sync Server started");

    for STREAM in LISTENER.incoming() {
        if let Ok(mut stream) = STREAM {
            if let Some(MSG) = ReceiveMessage(&mut stream) {
                match MSG {
                    SyncMessage::RequestManifest => {
                        let MANIFEST = GenerateManifest();
                        SendMessage(&mut stream, &SyncMessage::Manifest(MANIFEST));
                    }
                    SyncMessage::RequestFile(FILE_NAME) => {
                        let PATH = Path::new(SYNC_FOLDER_LOCATION).join(&FILE_NAME);
                        if let Ok(CONTENT) = fs::read(PATH) {
                            SendMessage(&mut stream, &SyncMessage::FileContent(FILE_NAME, CONTENT));
                        }
                    }
                    SyncMessage::FileContent(FILE_NAME, CONTENT) => {
                        if *STATE.read().unwrap() == SyncState::Active {
                            let PATH = Path::new(SYNC_FOLDER_LOCATION).join(FILE_NAME);
                            
                            let mut shouldWrite = true;
                            if let Ok(EXISTING_CONTENT) = fs::read(&PATH) {
                                if EXISTING_CONTENT == CONTENT {
                                    shouldWrite = false;
                                }
                            }
                            
                            if shouldWrite {
                                let _ = fs::write(PATH, CONTENT);
                                println!("Received and saved updated file via network.");
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

// Request Initial sync
fn RequestInitialSync(PEERS: Vec<String>, STATE: Arc<RwLock<SyncState>>) {
    if PEERS.is_empty() {
        *STATE.write().unwrap() = SyncState::Active;
        println!("No peers found. State changed to ACTIVE.");
        return;
    }

    println!("Requesting sync from peers...");
    for PEER in PEERS {
        if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", PEER, PORT + 1)) {
            SendMessage(&mut stream, &SyncMessage::RequestManifest);
            
            if let Some(SyncMessage::Manifest(MANIFEST)) = ReceiveMessage(&mut stream) {
                println!("Received Manifest. Downloading missing files...");
                for (FILE_NAME, _) in MANIFEST.files {
                    let LOCAL_PATH = Path::new(SYNC_FOLDER_LOCATION).join(&FILE_NAME);
                    
                    if !LOCAL_PATH.exists() {
                        if let Ok(mut fileStream) = TcpStream::connect(format!("{}:{}", PEER, PORT + 1)) {
                            SendMessage(&mut fileStream, &SyncMessage::RequestFile(FILE_NAME.clone()));
                            
                            if let Some(SyncMessage::FileContent(_, CONTENT)) = ReceiveMessage(&mut fileStream) {
                                let _ = fs::write(LOCAL_PATH, CONTENT);
                                println!("Downloaded file: {}", FILE_NAME);
                            }
                        }
                    }
                }
                *STATE.write().unwrap() = SyncState::Active;
                println!("Sync complete. State changed to ACTIVE.");
                break;
            }
        }
    }
    *STATE.write().unwrap() = SyncState::Active;
}


// Sync files to peers
fn SyncFiles(PATHS: Vec<PathBuf>) {
    let PEERS = LoadPears();
    
    for PEER in PEERS {
        for PATH in &PATHS {
            if PATH.is_file() {
                if let Ok(CONTENT) = fs::read(PATH) {
                    let FILE_NAME = PATH.file_name().unwrap().to_string_lossy().to_string();
                    if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", PEER, PORT + 1)) {
                        SendMessage(&mut stream, &SyncMessage::FileContent(FILE_NAME.clone(), CONTENT));
                        println!("Pushed file update to {}: {}", PEER, FILE_NAME);
                    }
                }
            }
        }
    }
}

// Folder Watcher
fn FolderWatcher(STATE: Arc<RwLock<SyncState>>) -> notify::Result<()> {
    println!("Folder watcher started");

    let (TX, RX) = mpsc::channel::<notify::Result<Event>>();

    let mut watcher = notify::recommended_watcher(TX)?;

    watcher.watch(Path::new(SYNC_FOLDER_LOCATION), RecursiveMode::Recursive)?;

    for RES in RX {
        match RES {
            Ok(EVENT) => {
                if *STATE.read().unwrap() == SyncState::Receiving {
                    continue;
                }

                match EVENT.kind {
                    EventKind::Create(_)
                    | EventKind::Modify(ModifyKind::Data(_))
                    | EventKind::Modify(ModifyKind::Name(_))
                    | EventKind::Remove(_) => {
                        println!("Updated and ready for sync: {:?}", EVENT.paths);
                        SyncFiles(EVENT.paths.clone());
                    }
                    _ => {}
                }
            }
            Err(E) => println!("Watch error: {:?}", E),
        }
    }

    Ok(())
}

// Broadcaster
fn Broadcaster() {
    let SOCKET = UdpSocket::bind("0.0.0.0:0").unwrap();

    println!("UDP broadcaster started");

    SOCKET.set_broadcast(true).unwrap();

    loop {
        let MSG = format!(r#"{{"app":"bixsync","port":{}}}"#, PORT);

        let _ = SOCKET.send_to(MSG.as_bytes(), format!("255.255.255.255:{}", PORT));

        thread::sleep(Duration::from_secs(5));
    }
}

// Discovery listner
fn DiscoveryListner() {
    let mut peers = LoadPears();

    let SOCKET = UdpSocket::bind(format!("0.0.0.0:{}", PORT)).unwrap();

    let mut buf = [0u8; 1024];

    println!("UDP listener started");

    loop {
        let (SIZE, SENDER) = SOCKET.recv_from(&mut buf).unwrap();

        let MSG = String::from_utf8_lossy(&buf[..SIZE]);

        if !MSG.contains("\"app\":\"bixsync\"") {
            continue;
        }

        let IP = SENDER.ip().to_string();

        if IP == SelfIpAddr.to_string() || peers.contains(&IP) {
            continue;
        }

        println!("Discovered {} -> {}", IP, MSG);

        peers.push(IP);

        SavePeers(&peers);
    }
}

// -------------
// Main function
fn main() -> io::Result<()> {
    let DEVICE_STATE = Arc::new(RwLock::new(SyncState::Receiving));

    let SERVER_STATE = Arc::clone(&DEVICE_STATE);
    thread::spawn(move || {
        TcpSyncServer(SERVER_STATE);
    });

    let WATCHER_STATE = Arc::clone(&DEVICE_STATE);
    thread::spawn(move || {
        let _ = FolderWatcher(WATCHER_STATE);
    });

    let PEERS = LoadPears();
    let SYNC_STATE = Arc::clone(&DEVICE_STATE);
    thread::spawn(move || {
        RequestInitialSync(PEERS, SYNC_STATE);
    });

    thread::spawn(Broadcaster);

    thread::spawn(DiscoveryListner);

    loop {
        thread::park();
    }
}