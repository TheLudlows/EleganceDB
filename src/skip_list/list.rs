use super::Allocator;
use super::HEIGHT_INCREASE;
use super::arena::Arena;
use super::KeyComparator;
use super::MAX_HEIGHT;
use bytes::Bytes;
use rand::Rng;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::{mem, ptr, u32};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::Ordering::Relaxed;

// Uses C layout to make sure tower is at the bottom
#[derive(Debug)]
#[repr(C)]
pub struct Node {
    pub key: Bytes,
    value: Bytes,
    height: usize,
    tower: [AtomicU32; MAX_HEIGHT],
}

impl Node {
    fn alloc(arena: &Arena, key: Bytes, value: Bytes, height: usize) -> u32 {
        let align = mem::align_of::<Node>();
        let size = mem::size_of::<Node>();
        // Not all values in Node::tower will be utilized.
        let not_used = (MAX_HEIGHT - height - 1) * mem::size_of::<AtomicU32>();
        let node_offset = arena.alloc(align, size - not_used);
        unsafe {
            let node_ptr: *mut Node = arena.get_mut(node_offset);
            let node = &mut *node_ptr;
            ptr::write(&mut node.key, key);
            ptr::write(&mut node.value, value);
            node.height = height;
            ptr::write_bytes(node.tower.as_mut_ptr(), 0, height + 1);
        }
        node_offset
    }

    fn next_offset(&self, height: usize) -> u32 {
        self.tower[height].load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct SkiplistCore {
    height: AtomicUsize,
    head: NonNull<Node>,
    arena: Arena,
}

#[derive(Clone, Debug)]
pub struct Skiplist<C> {
    core: Arc<SkiplistCore>,
    c: C,
}

impl<C> Skiplist<C> {
    pub fn with_capacity(c: C, arena_size: u32) -> Skiplist<C> {
        let arena = Arena::with_capacity(arena_size as usize);
        let head_offset = Node::alloc(&arena, Bytes::new(), Bytes::new(), MAX_HEIGHT - 1);
        let head = unsafe { NonNull::new_unchecked(arena.get_mut(head_offset)) };
        Skiplist {
            core: Arc::new(SkiplistCore {
                height: AtomicUsize::new(0),
                head,
                arena,
            }),
            c,
        }
    }

    fn random_height(&self) -> usize {
        let mut rng = rand::thread_rng();
        for h in 0..(MAX_HEIGHT - 1) {
            if !rng.gen_ratio(HEIGHT_INCREASE, u32::MAX) {
                return h;
            }
        }
        MAX_HEIGHT - 1
    }

    fn height(&self) -> usize {
        self.core.height.load(Ordering::SeqCst)
    }

    pub fn println_list(&self) {
        let head = self.core.head.as_ptr();
        unsafe {
            let h = (*head).height;
            for i in (0..=h).rev() {
                let mut cur = head;
                print!("level {} ", i);
                while !cur.is_null() {
                    let node = &*cur;
                    print!("{} ", String::from_utf8_unchecked(node.key.to_vec()));
                    let ht = node.height;
                    if ht >= i {
                        cur = self.core.arena.get_mut(node.tower[i].load(Relaxed));
                    } else {
                        cur = ptr::null_mut::<Node>();
                    }
                }
                println!();
            }
        }
    }
}

impl<C: KeyComparator> Skiplist<C> {

    pub fn find_near_value(&self, key: &[u8], less: bool, allow_equal: bool) -> &Bytes {
        let ptr = self.find_near(key, less, allow_equal);
        unsafe { &(*ptr).value }
    }

        pub fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> *const Node {
        unsafe {
            let mut cursor: *const Node = self.core.head.as_ptr();
            let mut level = self.height();
            loop {
                let next_offset = (&*cursor).next_offset(level);
                if next_offset == 0 {
                    if level > 0 {
                        level -= 1;
                        continue;
                    }
                    if !less || cursor == self.core.head.as_ptr() {
                        return ptr::null();
                    }
                    return cursor;
                }
                let next_ptr: *mut Node = self.core.arena.get_mut(next_offset);
                let next = &*next_ptr;
                let res = self.c.compare_key(key, &next.key);
                if res == std::cmp::Ordering::Greater {
                    cursor = next_ptr;
                    continue;
                }
                if res == std::cmp::Ordering::Equal {
                    if allow_equal {
                        return next;
                    }
                    if !less {
                        let offset = next.next_offset(0);
                        return if offset != 0 {
                            self.core.arena.get_mut(offset)
                        } else {
                            ptr::null()
                        };
                    }
                    if level > 0 {
                        level -= 1;
                        continue;
                    }
                    if cursor == self.core.head.as_ptr() {
                        return ptr::null();
                    }
                    return cursor;
                }
                if level > 0 {
                    level -= 1;
                    continue;
                }
                if !less {
                    return next;
                }
                if cursor == self.core.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }
        }
    }

    unsafe fn find_splice_for_level(&self, key: &[u8], mut before: *mut Node, level: usize) -> (*mut Node, *mut Node) {
        loop {
            let next_offset = (&*before).next_offset(level);
            if next_offset == 0 {
                return (before, ptr::null_mut());
            }
            let next_ptr: *mut Node = self.core.arena.get_mut(next_offset);
            let next_node = &*next_ptr;
            match self.c.compare_key(&key, &next_node.key) {
                std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                std::cmp::Ordering::Less => return (before, next_ptr),
                _ => before = next_ptr,
            }
        }
    }

    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Option<(Bytes, Bytes)> {
        let (key, value) = (key.into(), value.into());
        let mut list_height = self.height();
        let mut prev = [ptr::null_mut(); MAX_HEIGHT + 1];
        let mut next = [ptr::null_mut(); MAX_HEIGHT + 1];
        prev[list_height + 1] = self.core.head.as_ptr();
        next[list_height + 1] = ptr::null_mut();
        for i in (0..=list_height).rev() {
            let (p, n) = unsafe { self.find_splice_for_level(&key, prev[i + 1], i) };
            prev[i] = p;
            next[i] = n;
            if p == n {
                unsafe {
                    if (*p).value != value {
                        return Some((key, value));
                    }
                }
                return None;
            }
        }

        let height = self.random_height();
        let node_offset = Node::alloc(&self.core.arena, key, value, height);
        while height > list_height {
            match self.core.height.compare_exchange_weak(
                list_height,
                height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(h) => list_height = h,
            }
        }
        let x: &mut Node = unsafe { &mut *self.core.arena.get_mut(node_offset) };
        for i in 0..=height {
            loop {
                if prev[i].is_null() {
                    assert!(i > 1);
                    let (p, n) =
                        unsafe { self.find_splice_for_level(&x.key, self.core.head.as_ptr(), i) };
                    prev[i] = p;
                    next[i] = n;
                    assert_ne!(p, n);
                }
                let next_offset = self.core.arena.offset(next[i]);
                x.tower[i].store(next_offset, Ordering::SeqCst);
                match unsafe { &*prev[i] }.tower[i].compare_exchange(
                    next_offset,
                    node_offset,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(_) => {
                        let (p, n) = unsafe { self.find_splice_for_level(&x.key, prev[i], i) };
                        if p == n {
                            assert_eq!(i, 0);
                            if unsafe { &*p }.value != x.value {
                                let key = mem::replace(&mut x.key, Bytes::new());
                                let value = mem::replace(&mut x.value, Bytes::new());
                                return Some((key, value));
                            }
                            unsafe {
                                ptr::drop_in_place(x);
                            }
                            return None;
                        }
                        prev[i] = p;
                        next[i] = n;
                    }
                }
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        let node = self.core.head.as_ptr();
        let next_offset = unsafe { (&*node).next_offset(0) };
        next_offset == 0
    }

    pub fn len(&self) -> usize {
        let mut node = self.core.head.as_ptr();
        let mut count = 0;
        loop {
            let next = unsafe { (&*node).next_offset(0) };
            if next != 0 {
                count += 1;
                node = unsafe { self.core.arena.get_mut(next) };
                continue;
            }
            return count;
        }
    }

    fn find_last(&self) -> *const Node {
        let mut node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            let next = unsafe { (&*node).next_offset(level) };
            if next != 0 {
                node = unsafe { self.core.arena.get_mut(next) };
                continue;
            }
            if level == 0 {
                if node == self.core.head.as_ptr() {
                    return ptr::null();
                }
                return node;
            }
            level -= 1;
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        let node = unsafe { self.find_near(key, false, true) };
        if node.is_null() {
            return None;
        }
        if self.c.same_key(&unsafe { &*node }.key, key) {
            return unsafe { Some(&(*node).value) };
        }
        None
    }

    pub fn iter_ref(&self) -> IterRef<'_, C> {
        IterRef {
            list: self,
            cursor: ptr::null(),
        }
    }

    pub fn range_ref(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> RangeRef<C> {
        let (l, r) = (map_bound(lower), map_bound(upper));
        RangeRef::create(self, (l, r))
    }

    pub fn mem_size(&self) -> u32 {
        self.core.arena.len()
    }
}
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}
impl Drop for SkiplistCore {
    fn drop(&mut self) {
        let mut node = self.head.as_ptr();
        loop {
            let next = unsafe { (&*node).next_offset(0) };
            if next != 0 {
                let next_ptr = unsafe { self.arena.get_mut(next) };
                unsafe {
                    ptr::drop_in_place(node);
                }
                node = next_ptr;
                continue;
            }
            unsafe { ptr::drop_in_place(node) };
            return;
        }
    }
}

impl<C> AsRef<Skiplist<C>> for Skiplist<C> {
    fn as_ref(&self) -> &Skiplist<C> {
        self
    }
}

unsafe impl<C: Send> Send for Skiplist<C> {}

unsafe impl<C: Sync> Sync for Skiplist<C> {}

pub struct IterRef<'a, C> {
    list: &'a Skiplist<C>,
    cursor: *const Node,
}

#[derive(Debug)]
pub struct RangeRef<'a, C: KeyComparator> {
    list: &'a Skiplist<C>,
    head: *const Node,
    tail: *const Node,
    start: Bound<Bytes>,
    end: Bound<Bytes>,
}

impl<'a, C: KeyComparator> RangeRef<'a, C> {
    pub fn create(skl: &'a Skiplist<C>, r: (Bound<Bytes>, Bound<Bytes>)) -> Self {
        let mut range_it = Self {
            list: skl,
            head: ptr::null(),
            tail: ptr::null(),
            start: r.0,
            end: r.1
        };
        let start = &range_it.start;
        let end = &range_it.end;
        match start {
            Bound::Included(ref start_key) => {
                range_it.head = range_it.list.find_near(start_key, false, true);
            }
            Bound::Excluded(ref start_key) => {
                range_it.head = range_it.list.find_near(start_key, false, false);
            }
            Bound::Unbounded => {}
        }
        match end {
            Bound::Included(ref end_key) => {
                range_it.tail = range_it.list.find_near(end_key, false, false);
            }
            Bound::Excluded(ref end_key) => {
                range_it.tail = range_it.list.find_near(end_key, false, true);
            }
            Bound::Unbounded => {}
        }
        range_it
    }
    pub fn valid(&self) -> bool {
        !self.head.is_null() && !self.head.eq(&self.tail)
    }

    pub fn key(&self) -> &Bytes {
        assert!(self.valid());
        unsafe { &(*self.head).key }
    }

    pub fn value(&self) -> &Bytes {
        assert!(self.valid());
        unsafe { &(*self.head).value }
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            let cursor_offset = (&*self.head).next_offset(0);
            self.head = self.list.core.arena.get_mut(cursor_offset);
        }
    }
}

impl<'a, C: KeyComparator> IterRef<'a, C> {
    pub fn valid(&self) -> bool {
        !self.cursor.is_null()
    }

    pub fn key(&self) -> &Bytes {
        assert!(self.valid());
        unsafe { &(*self.cursor).key }
    }

    pub fn value(&self) -> &Bytes {
        assert!(self.valid());
        unsafe { &(*self.cursor).value }
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            let cursor_offset = (&*self.cursor).next_offset(0);
            self.cursor = self.list.core.arena.get_mut(cursor_offset);
        }
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        unsafe {
            self.cursor = self.list.find_near(self.key(), true, false);
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.find_near(target, false, true);
        }
    }

    pub fn seek_for_prev(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.find_near(target, true, true);
        }
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            let cursor_offset = (&*self.list.core.head.as_ptr()).next_offset(0);
            self.cursor = self.list.core.arena.get_mut(cursor_offset);
        }
    }

    pub fn seek_to_last(&mut self) {
        self.cursor = self.list.find_last();
    }
}