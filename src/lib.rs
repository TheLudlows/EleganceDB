extern crate core;

use std::collections::Bound;

use bytes::Bytes;

pub mod block;
mod table;
mod iterators;
mod skip_list;
mod memtable;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[test]
    fn test_checksum() {
        let checksum = crc32fast::hash(b"foo bar baz");
    }
}
