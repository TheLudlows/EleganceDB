#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)]

// TODO(you): remove this lint after implementing this mod
use std::sync::Arc;
use bytes::{Buf, BufMut};
use crate::block::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: Vec<u8>,
    /// The corresponding value, can be empty
    value: Vec<u8>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_first();
        iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_key(key);
        iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        if self.key.is_empty()
        {
            println!("err block {}", self.block)
        }
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.value
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx +=1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }
    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }
    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        entry.advance(key_len);
        self.key.clear();
        self.key.put_slice(key);

        let val_len = entry.get_u16() as usize;
        let val = &entry[..val_len];
        entry.advance(val_len);
        self.value.clear();
        self.value.put_slice(val);
    }
}
