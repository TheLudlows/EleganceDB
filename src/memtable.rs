use std::ops::Bound;

use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::iterators::StorageIterator;
use crate::map_bound;
use crate::skip_list::{KeyComparator, RangeRef, Skiplist};

pub struct MemTable<C: KeyComparator> {
    skl: Skiplist<C>,
    id: usize,
}

impl<C: KeyComparator> MemTable<C> {
    pub fn new(cap: usize, c: C) -> Self {
        Self {
            skl: Skiplist::with_capacity(c, cap as u32),
            id: 0,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.skl.get(key).map(|v| v.clone())
    }

    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let r = self.skl.put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(val));
        match r {
            None => { Ok(()) }
            Some(_) => { Err(anyhow!("put item error")) }
        }
    }

    pub fn scan(&self, left: Bound<&[u8]>, right: Bound<&[u8]>) -> MemTableIterator<C> {
        let (lower, upper) = (map_bound(left), map_bound(right));
        MemTableIterator::create(self, lower, upper)
    }
}

pub struct MemTableIterator<'a, C: KeyComparator> {
    iter: RangeRef<'a, C>,
    item: (Bytes, Bytes),
}

impl<'a, C: KeyComparator> MemTableIterator<'a, C> {
    pub fn create(mem_table: &'a MemTable<C>, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Self {
        Self {
            iter: mem_table.skl.range_ref(lower, upper),
            item: (Bytes::new(), Bytes::new()),
        }
    }
}

impl<'a, C: KeyComparator> StorageIterator for MemTableIterator<'a, C> {
    fn value(&self) -> &[u8] {
        &self.item.1[..]
    }

    fn key(&self) -> &[u8] {
        &self.item.0[..]
    }

    fn is_valid(&self) -> bool {
        self.iter.valid()
    }

    fn next(&mut self) -> Result<()> {
        let kv = self.iter.next();
        assert!(kv.is_some());
        let kv = kv.unwrap();
        let key = kv.0.clone();
        let val = kv.1.clone();
        self.item = (key, val);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::memtable::MemTable;
    use crate::skip_list::FixedLengthSuffixComparator;

    #[test]
    fn test_new() {
        let mem = MemTable::new(1024, FixedLengthSuffixComparator::new(8));
    }

    #[test]
    fn test_bytes()
    {
        let b = "abc".as_bytes();

        let bs1 = Bytes::new();
        let bs: Bytes = bs1.into();
        println!("{:p}", bs.as_ptr());
    }
}