use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
pub type StoreArc = Arc<Mutex<Store>>;
pub struct Store {
    data: HashMap<String, String>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: HashMap::new(),
        }
    }
    pub fn set(&mut self, key: String, val: String) {
        self.data.insert(key, val);
    }

    pub fn get(&self, key: String) -> Option<&String> {
        self.data.get(&key)
    }
}
