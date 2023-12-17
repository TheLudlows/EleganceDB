#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;
use crate::block::iterator::BlockIterator;
use crate::iterators::StorageIterator;

use super::SsTable;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iterator: BlockIterator,
    //current block
    block_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_it = Self::seek_to_first_inner(&table)?;
        Ok(Self {
            table,
            block_iterator: block_it,
            block_idx: 0,
        })
    }
    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<BlockIterator> {
        Ok(BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?))
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: &[u8]) -> Result<BlockIterator> {
        let idx = table.find_block_idx(key);
        let block = table.read_block_cached(idx)?;
        Ok(BlockIterator::create_and_seek_to_first(block))
    }
    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.block_iterator = Self::seek_to_first_inner(&self.table)?;
        self.block_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let block_idx = table.find_block_idx(key);
        let block = table.read_block_cached(block_idx)?;
        let block_it = BlockIterator::create_and_seek_to_key(block, key);
        Ok(Self {
            table,
            block_iterator: block_it,
            block_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let idx = self.table.find_block_idx(key);
        let block = self.table.read_block_cached(idx)?;
        self.block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        self.block_idx = idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.block_iterator.value()
    }

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.block_iterator.key()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_iterator.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.block_iterator.next();
        if !self.block_iterator.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.num_of_blocks() {
                self.block_iterator = BlockIterator::create_and_seek_to_first(self.table.read_block_cached(self.block_idx)?);
            }
        }
        Ok(())
    }
}
