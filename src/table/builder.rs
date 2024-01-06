#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::mem;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut};
use crate::block::block_builder::BlockBuilder;

use super::{BlockCache, BlockMeta, FileObject, SsTable};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    data: Vec<u8>,
    pub(super) meta: Vec<BlockMeta>,
    block_builder: BlockBuilder,
    start_key: Vec<u8>,
    block_size: usize,
    // Add other fields you need.
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::default(),
            meta: Vec::default(),
            block_builder: BlockBuilder::new(block_size),
            start_key: Vec::default(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.start_key.is_empty() {
            self.start_key.put(key);
        }

        let ok = self.block_builder.add(key, value);
        if !ok {
            self.finish_block();
            let ok = self.block_builder.add(key, value);
            self.start_key.put(key);
            assert!(ok);
        }
    }

    fn finish_block(&mut self) {
        let block = mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        let meta = BlockMeta { offset: self.data.len() as u64, first_key: mem::take(&mut self.start_key).into() };
        self.data.put(block.build().encode());
        //println!("encode table data{:?}, len {}", self.data, self.data.len());
        self.meta.push(meta)
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        let mut size = self.data.len();
        for meta in &self.meta {
            size += meta.size()
        }
        size + mem::size_of::<u64>()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let meta_off = self.data.len() as u64;
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u64(meta_off);
        let sst  = SsTable {
            file: FileObject::create(path.as_ref(), self.data)?,
            block_metas: self.meta,
            block_meta_offset: meta_off,
            sst_id: id,
            block_cache,
        };
        println!("table is {}", sst);
        Ok(sst)
    }

    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
