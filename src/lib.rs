use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use file_log::{Log, LogOperation};

mod file_log;

pub type SimpleCollection = HashMap<Vec<u8>, u64>;
type Records = Arc<RwLock<SimpleCollection>>;

pub struct SimpleDB {
    records: Records,
    log: Mutex<Log>,
}

impl SimpleDB {
    pub fn new() -> Self {
        let mut log = Log::new("./data/").unwrap();
        let map = log.construct_in_memory_cache().unwrap();

        Self {
            records: Arc::new(RwLock::new(map)),
            log: Mutex::new(log),
        }
    }
}

impl SimpleDB {
    pub fn get<S: Into<Vec<u8>>>(&self, key: S) -> Option<Vec<u8>> {
        self.records.read().ok().and_then(|guard| {
            guard
                .get(&key.into())
                .map(|value| self.log.lock().unwrap().get_value(*value).unwrap().0)
        })
    }

    pub fn put<S: Into<Vec<u8>>>(&self, key: S, value: S) -> Option<Vec<u8>> {
        self.records.write().ok().and_then(|mut guard| {
            let key: Vec<u8> = key.into();
            let value: Vec<u8> = value.into();

            let position = self
                .log
                .lock()
                .unwrap()
                .append(LogOperation::Put(key.clone(), value.clone()))
                .unwrap();

            guard
                .insert(key, position)
                .map(|old_position| self.log.lock().unwrap().get_value(old_position).unwrap().0)
        })
    }

    pub fn delete<S: Into<Vec<u8>>>(&self, key: S) -> Option<Vec<u8>> {
        self.records.write().ok().and_then(|mut guard| {
            let key: Vec<u8> = key.into();

            let _ = self
                .log
                .lock()
                .unwrap()
                .append(LogOperation::Delete(key.clone()))
                .unwrap();

            guard
                .remove(&key)
                .map(|old_position| self.log.lock().unwrap().get_value(old_position).unwrap().0)
        })
    }
}
