mod arena;
mod list;
mod key;

pub use arena::*;
pub use key::*;
pub use list::*;

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;

pub trait Allocator {
    fn alloc(&self, alain: usize, size: usize) -> u32;
    fn len(&self) -> u32;
    fn capacity(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;
    use std::mem;
    use std::sync::atomic::AtomicPtr;
    use bytes::Bytes;
    use rand::{Rng};

    use crate::skip_list::{FixedLengthSuffixComparator, FlexibleCompartor};

    use super::{list::Skiplist};

    #[test]
    fn test_find_near() {
        let comp = FlexibleCompartor::new(8);
        let list = Skiplist::with_capacity(comp, 1 << 20);
        for i in 0..1000 {
            let key = Bytes::from(format!("{:05}{:08}", i * 10 + 5, 0));
            let value = Bytes::from(format!("{:05}", i));
            let r = list.put(key, value);
        }
        let mut cases = vec![
            ("00001", false, false, Some("00005")),
            ("00001", false, true, Some("00005")),
            ("00001", true, false, None),
            ("00001", true, true, None),
            ("00005", false, false, Some("00015")),
            ("00005", false, true, Some("00005")),
            ("00005", true, false, None),
            ("00005", true, true, Some("00005")),
            ("05555", false, false, Some("05565")),
            ("05555", false, true, Some("05555")),
            ("05555", true, false, Some("05545")),
            ("05555", true, true, Some("05555")),
            ("05558", false, false, Some("05565")),
            ("05558", false, true, Some("05565")),
            ("05558", true, false, Some("05555")),
            ("05558", true, true, Some("05555")),
            ("09995", false, false, None),
            ("09995", false, true, Some("09995")),
            ("09995", true, false, Some("09985")),
            ("09995", true, true, Some("09995")),
            ("59995", false, false, None),
            ("59995", false, true, None),
            ("59995", true, false, Some("09995")),
            ("59995", true, true, Some("09995")),
        ];
        for (i, (key, less, allow_equal, exp)) in cases.drain(..).enumerate() {
            let seek_key = Bytes::from(format!("{}{:08}", key, 0));
            let res = unsafe { list.find_near(&seek_key, less, allow_equal) };
            if exp.is_none() {
                assert!(res.is_null(), "{}", i);
                continue;
            }
            let e = format!("{}{:08}", exp.unwrap(), 0);
            assert_eq!(&unsafe { &*res }.key, e.as_bytes(), "{}", i);
        }
    }

    #[test]
    fn test_skl() {
        let comp = FlexibleCompartor::new(8);
        let skl = Skiplist::with_capacity(comp, 1024 * 1024);
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let _ = skl.put(format!("{}", rng.gen_range(0..10000)), "a");
        }
        unsafe { skl.println_list(); }
    }

    #[test]
    fn test_skl_res() {
        let comp = FlexibleCompartor::new(8);
        let skl = Skiplist::with_capacity(comp, 1024 * 1024);
        let mut rng = rand::thread_rng();
        let r = skl.put(format!("{}", rng.gen_range(0..10000)), "a");

        assert!(r.is_none())
    }

    #[test]
    fn test_bytes() {
        println!("{}", mem::size_of::<Bytes>());
        println!("{}", mem::size_of::<AtomicPtr<u8>>());
    }

    #[test]
    fn test_anena_align() {
        println!("{}", mem::align_of::<Vec<u64>>());
        println!("{}", mem::align_of::<u8>());
        let mut res = vec![];
        for _ in 0..100000 {
            let v: Vec<u8> = Vec::with_capacity(1);
            let p = v.as_ptr();
            assert_eq!(p as usize % 8, 0);
            res.push(v);
        }
        let _: Vec<u8> = Vec::with_capacity(1);
        let lay = Layout::array::<u8>(1);
        println!("{:?}", lay);
    }

    #[test]
    fn test_const_fn() {
        let c1 = FixedLengthSuffixComparator::new(1);
        let c2 = FixedLengthSuffixComparator::new(2);
        println!("{:p}, {:p}", &c1, &c2)
    }
}