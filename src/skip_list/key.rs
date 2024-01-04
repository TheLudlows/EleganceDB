use std::cmp;
use bytes::Bytes;
use std::cmp::Ordering;

pub trait KeyComparator {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FixedLengthSuffixComparator {
    len: usize,
}
#[derive(Default, Debug, Clone, Copy)]
pub struct FlexibleCompartor{
    len: usize
}
impl FlexibleCompartor {
    pub fn new(len: usize) -> Self {
        FlexibleCompartor {
            len
        }
    }
}

impl KeyComparator for FlexibleCompartor {

    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let max_len = cmp::min(cmp::min(self.len, lhs.len()), rhs.len());
        let(left, right) = (&lhs[0..max_len], &rhs[0..max_len]);
        match left.cmp(&right) {
            Ordering::Less =>  Ordering::Less,
            Ordering::Equal => lhs.cmp(&rhs),
            Ordering::Greater => Ordering::Greater
        }
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
       lhs.cmp(rhs) == Ordering::Equal
    }
}



impl FixedLengthSuffixComparator {
    pub const fn new(len: usize) -> FixedLengthSuffixComparator {
        FixedLengthSuffixComparator { len }
    }
}

impl KeyComparator for FixedLengthSuffixComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        if lhs.len() < self.len {
            panic!(
                "cannot compare with suffix {}: {:?}",
                self.len,
                Bytes::copy_from_slice(lhs)
            );
        }
        if rhs.len() < self.len {
            panic!(
                "cannot compare with suffix {}: {:?}",
                self.len,
                Bytes::copy_from_slice(rhs)
            );
        }
        let (l_p, l_s) = lhs.split_at(lhs.len() - self.len);
        let (r_p, r_s) = rhs.split_at(rhs.len() - self.len);
        let res = l_p.cmp(r_p);
        match res {
            Ordering::Greater | Ordering::Less => res,
            Ordering::Equal => l_s.cmp(r_s),
        }
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        let (l_p, _) = lhs.split_at(lhs.len() - self.len);
        let (r_p, _) = rhs.split_at(rhs.len() - self.len);
        l_p == r_p
    }
}
