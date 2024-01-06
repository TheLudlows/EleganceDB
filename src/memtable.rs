use bytes::Bytes;
use crate::skip_list::{KeyComparator, Skiplist};
use anyhow::{anyhow, Result};

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
            None => {Ok(())}
            Some(_) => {Err(anyhow!("put item error"))}
        }
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
        let bs:Bytes = bs1.into();
        println!("{:p}", bs.as_ptr());
    }

}