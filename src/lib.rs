extern crate core;

pub mod block;
mod table;
mod iterators;
mod skip_list;
mod memtable;

pub fn add(left: usize, right: usize) -> usize {
    left + right
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
