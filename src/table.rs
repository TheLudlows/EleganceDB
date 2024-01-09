#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod


use std::fmt;
use std::fs::File;
use std::io::Write;
use std::mem::size_of;
use std::os::windows::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes};

use crate::block::Block;

mod iterator;
mod builder;
mod tests;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;


#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: u64,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut size = 0;
        for meta in block_meta {
            size += std::mem::size_of::<u64>();
            size += std::mem::size_of::<u16>();
            size += meta.first_key.len();
        }
        let original_len = buf.len();
        for meta in block_meta {
            buf.put_u64(meta.offset);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key)
        }
        assert_eq!(size, buf.len() - original_len);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut vec = vec![];
        while buf.has_remaining() {
            let offset = buf.get_u64();
            let len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(len);
            vec.push(BlockMeta { offset, first_key });
        }
        vec
    }

    pub fn size(&self) -> usize {
        size_of::<u64>() + size_of::<u16>() + self.first_key.len()
    }
}

/// A file object.
pub struct FileObject(File);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0; len as usize];
        let r = self.0.seek_read(&mut buf, offset)?;
        println!("read buf {:?}", buf);
        Ok(buf)
    }

    pub fn size(&self) -> u64 {
        self.0.metadata().expect("read file meta err").len()
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        // assert the parent directory exists
        let parent_dir = path.parent().unwrap();
        if !parent_dir.exists() {
            std::fs::create_dir_all(parent_dir).expect("[FileObject::create] create dir fail");
        }
        // create a new file and write the data
        let mut file = File::options().write(true).read(true).create(true).truncate(true).open(path).expect("[FileObject::create] create file fail");
        file.write_all(&data[..])
            .expect("[FileObject::create] write file fail");
        file.flush().expect("[FileObject::create] flush file fail");
        Ok(Self(file))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().write(true).read(true).open(path)?;
        Ok(Self(file))
    }
}

/// -------------------------------------------------------------------------------------------------------
/// |              Data Block             |             Meta Block              |          Extra          |
/// -------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Meta Block #1 | ... | Meta Block #N | Meta Block Offset (u64) |
/// -------------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: u64,
    sst_id: usize,
    block_cache: Option<Arc<BlockCache>>,
}

impl fmt::Display for SsTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "block meta {:?}, offset {:?}", &self.block_metas, self.block_meta_offset)
    }
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, file, None)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, file: FileObject, block_cache: Option<Arc<BlockCache>>) -> Result<Self> {
        let len = file.size();
        let raw_meta_off = file.read(len - size_of::<u64>() as u64, size_of::<u64>() as u64)?;
        let meta_off = (&raw_meta_off[..]).get_u64();
        let raw_meta = file.read(meta_off, len - size_of::<u64>() as u64 - meta_off)?;
        Ok(Self {
            file,
            block_metas: BlockMeta::decode_block_meta(&raw_meta[..]),
            block_meta_offset: meta_off,
            sst_id: id,
            block_cache,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        assert!(block_idx < self.block_metas.len());
        let block_off_start = self.block_metas[block_idx].offset;
        let block_off_end = self.block_metas
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |m| m.offset);
        let raw_block = self.file.read(block_off_start, block_off_end - block_off_start)?;

        let b = Block::decode(&raw_block);
        Ok(Arc::new(b))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref cache) = self.block_cache {
            cache.try_get_with((self.sst_id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        for meta in &self.block_metas {
            println!("{:?}", meta)
        }
        self.block_metas.partition_point(|e| e.first_key <= key).saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[test]
fn test() {
    let v = vec![1, 2, 3];
    let r = v.partition_point(|e| *e < 0).saturating_sub(1);
    println!("{}", r);
    let r = v.partition_point(|e| *e <= 1);
    println!("{}", r);
    let r = v.partition_point(|e| *e <= 2);
    println!("{}", r);
    let r = v.partition_point(|e| *e > 2);
    println!("{}", r);
}