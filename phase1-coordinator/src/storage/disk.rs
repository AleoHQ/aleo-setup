use crate::Storage;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Disk {}

impl Storage for Disk {
    /// Returns the value reference for a given key from storage, if it exists.
    #[inline]
    fn get(&self, key: &Key) -> Option<&Value> {
        self.storage.get(key)
    }

    /// Returns the mutable value reference for a given key from storage, if it exists.
    #[inline]
    fn get_mut(&mut self, key: &Key) -> Option<&mut Value> {
        self.storage.get_mut(key)
    }

    /// Inserts a new key value pair into storage,
    /// updating the current value for a given key if it exists.
    /// If successful, returns `true`. Otherwise, returns `false`.
    #[inline]
    fn insert(&mut self, key: Key, value: Value) -> bool {
        self.insert(key, value)
    }

    /// Removes a value from storage for a given key.
    /// If successful, returns `true`. Otherwise, returns `false`.
    #[inline]
    fn remove(&mut self, key: &Key) -> bool {
        self.remove(key)
    }

    /// Loads a new instance of `Storage`.
    #[inline]
    fn load() -> Self {
        Self {
            storage: HashMap::default(),
        }
    }

    /// Stores an instance of `Storage`.
    /// If successful, returns `true`. Otherwise, returns `false`.
    #[inline]
    fn store(&mut self) -> bool {
        // As this storage is in memory, we can always return `true`.
        true
    }
}
