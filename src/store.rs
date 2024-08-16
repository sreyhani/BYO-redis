use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use tokio::sync::Mutex;

use tokio::time::sleep;
pub type StoreArc = Arc<Store>;
pub struct Store {
    data: Arc<Mutex<HashMap<String, String>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_multiple_keys(&self, map: HashMap<String, String>) {
        let mut data = self.data.lock().await;
        for (key, value) in map {
            data.insert(key, value);
        }
    }

    pub fn set_multiple_expires(&self, map: HashMap<String, Duration>) {
        for (key, expire) in map {
            self.set_expirey(key, expire);
        }
    }

    pub async fn set(&self, key: String, val: String) {
        let mut data = self.data.lock().await;
        data.insert(key, val);
    }

    pub async fn set_with_expire(&self, key: String, val: String, expire: Duration) {
        let key_clone = key.clone();
        let mut data = self.data.lock().await;
        data.insert(key, val);
        self.set_expirey(key_clone, expire);
    }

    fn set_expirey(&self, key: String, expire: Duration) {
        let data_clone = self.data.clone();
        tokio::spawn(async move {
            sleep(expire).await;
            let mut data = data_clone.lock().await;
            data.remove(&key);
        });
    }
    pub async fn get(&self, key: String) -> Option<String> {
        let data = self.data.lock().await;
        data.get(&key).cloned()
    }

    pub async fn get_matching_keys(&self, pattern: String) -> Vec<String> {
        assert_eq!(pattern, "*");
        let data = self.data.lock().await;
        data.keys().cloned().collect()
    }
}
