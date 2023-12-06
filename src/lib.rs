extern crate core;

pub mod block;
pub mod BlockBuilder;
mod table;

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
