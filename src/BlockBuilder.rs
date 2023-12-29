use bytes::BufMut;
use super::block::Block;


static SIZE_OF_META: usize = 2;

/// Builds a block.
pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    capacity: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(block_size),
            offsets: vec![],
            capacity: block_size,
        }
    }

    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        if key.is_empty() {
            return false;
        }
        if self.cur_size() + key.len() + value.len() + 4 > self.capacity {
            return false;
        }
        // check order todo
        /* if !self.offsets.is_empty() {
             let last_off = *self.offsets.last().expect("Offset get last err") as usize;
             let last_put_entry = &self.data[last_off..];
             []
         }*/
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put_slice(key);
        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);
        return true;
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn build(self) -> Block {
        let b = Block::new(self.data, self.offsets);
        println!("{}", b);
        b
    }

    pub fn cur_size(&self) -> usize {
        self.data.len() + self.offsets.len() * 2 + SIZE_OF_META
    }
}
