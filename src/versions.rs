use std::sync::{Arc, LazyLock, Mutex};

use dashmap::DashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub static VERSIONS: LazyLock<Mutex<VersionKeeper>> = LazyLock::new(|| Mutex::new(VersionKeeper::new()));

pub struct VersionKeeper {
    tx: UnboundedSender<(String, String)>,
    rx: Option<UnboundedReceiver<(String, String)>>,
    storage: Arc<DashMap<String, usize>>,
}

impl VersionKeeper {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            tx,
            rx: Some(rx),
            storage: Arc::new(DashMap::new()),
        }
    }

    pub fn start_listening(&mut self) {
        let mut rx = self.rx.take().expect("Taking ownership");
        let storage = Arc::clone(&self.storage);

        tokio::spawn(async move {
            while let Some((map, key)) = rx.recv().await {
                let key = format!("{}", key);
                let mut count = storage.entry(key.clone()).or_insert(0);
                *count += 1;
                println!("Increasing ver for {} to {}", key, *count);
            }
        });
    }

    pub fn sender(&self) -> UnboundedSender<(String, String)> {
        self.tx.clone()
    }

    pub fn watch(&self, key: &str) -> Option<usize> {
        Some(*self.storage.entry(format!("{}", key)).or_insert(0))
    }

    pub fn is_version_same(&self, key: &str, version : usize) -> bool {
        *self.storage.entry(format!("{}", key)).or_insert(0) == version
    }
    
    // pub fn get_version(&self, map : &str,  key: &str) -> Option<usize> {
    //     self.storage.get(&format!("{}-{}", map, key)).map(|v| *v.value())
    // }
    
    pub fn print_storage(&self) {
        println!("{:#?}", self.storage);
    }
}