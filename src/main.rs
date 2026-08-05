#![allow(nonstandard_style)]

use bixsync::*;
use notify::{Event, RecursiveMode, Result, Watcher};
use std::{path::Path, sync::mpsc, thread};

fn FolderWatcher() -> Result<()> {
    let (tx, rx) = mpsc::channel::<Result<Event>>();

    let mut watcher = notify::recommended_watcher(tx)?;

    watcher.watch(Path::new(SYNC_FOLDER_LOCATION), RecursiveMode::Recursive)?;

    for res in rx {
        match res {
            Ok(event) => println!("event: {:?}", event),
            Err(e) => println!("watch error: {:?}", e),
        }
    }

    Ok(())
}

fn main() {
    let folderWatcherRunner = thread::spawn(|| {
        println!("Starting folder watcher!!!");
        FolderWatcher().unwrap();
    });

    folderWatcherRunner.join().unwrap();
}
