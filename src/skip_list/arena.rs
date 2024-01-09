use std::{
    ptr,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};
use std::mem;

use super::Allocator;

#[derive(Debug)]
struct InnerArena {
    len: AtomicU32,
    cap: usize,
    pub ptr: *mut u8,
}

impl Drop for InnerArena {
    fn drop(&mut self) {
        unsafe {
            Vec::from_raw_parts(self.ptr as *mut u8, 0, self.cap);
        }
    }
}

#[derive(Debug)]
pub struct Arena {
    inner: Arc<InnerArena>,
}

impl Arena {
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        let mut v: Vec<u64> = Vec::with_capacity(cap >> 3);
        let ptr = v.as_mut_ptr() as *mut u8;
        mem::forget(v);
        Self {
            inner: Arc::new(InnerArena {
                len: AtomicU32::new(1),
                cap: cap >> 3 << 3,
                ptr,
            }),
        }
    }

    pub unsafe fn get_mut<T>(&self, off: u32) -> *mut T {
        if off == 0 {
            return ptr::null_mut();
        }
        self.inner.ptr.add(off as usize) as _
    }

    pub fn offset<T>(&self, ptr: *mut T) -> u32 {
        let ptr_addr = ptr as usize;
        let addr = self.inner.ptr as usize;
        if ptr_addr > addr + self.inner.cap || ptr_addr < addr {
            return 0;
        }
        (ptr_addr - addr) as u32
    }
}

impl Allocator for Arena {
    fn alloc(&self, align: usize, size: usize) -> u32 {
        assert_eq!(align & align - 1, 0, "align must be power of 2");
        let align_mask = align - 1;
        // Leave enough padding for align.
        let size = size + align_mask;
        // return 增加之前的地址
        let offset = self.inner.len.fetch_add(size as u32, Ordering::SeqCst);
        // (offset + align_mask) / align * align.
        let ptr_offset = (offset as usize + align_mask) & !align_mask;
        assert!(ptr_offset < self.inner.cap);
        return ptr_offset as u32;
    }

    fn len(&self) -> u32 {
        self.inner.len.load(Ordering::Acquire)
    }

    fn capacity(&self) -> usize {
        self.inner.cap
    }
}

#[cfg(test)]
mod test {
    use crate::skip_list::{Allocator, Arena};

    #[test]
    fn test() {
        let arena = super::Arena::with_capacity(1024);
        let align = 8;
        let size = 8;
        println!("{}", arena.inner.ptr as usize);
        let r = arena.alloc(align, size);
        println!("{}, {}", r, arena.len());
        let r = arena.alloc(align, size);
        println!("{}, {}", r, arena.len());
        let r = arena.alloc(align, size);
        println!("{}, {}", r, arena.len());
        println!("{}", std::mem::align_of::<Vec<u64>>());
        let n: usize = 10;
        println!("{}", !n);
        let n: usize = 8;
        println!("{}, {:b}", n, !n);
        let n: usize = 7;
        println!("{},{:b}", n, !n);
    }

    #[test]
    fn test_arena() {
        {
            let _ = Arena::with_capacity(1024);
        }
        println!("1");
    }
}
