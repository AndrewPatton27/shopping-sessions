//! Global allocator: large blocks come straight from mmap, 2 MiB aligned and
//! marked MADV_HUGEPAGE, so the gigabyte of scratch buffers is faulted in
//! 2 MiB at a time instead of 4 KiB at a time. Everything else is the system
//! allocator. Linux only; elsewhere this is the system allocator.

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::alloc::{GlobalAlloc, Layout, System};

pub struct Alloc;

#[cfg(target_os = "linux")]
mod sys {
    use std::ffi::c_void;
    extern "C" {
        pub fn mmap(addr: *mut c_void, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut c_void;
        pub fn munmap(addr: *mut c_void, len: usize) -> i32;
        pub fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
    }
    pub const PROT_RW: i32 = 0x1 | 0x2;
    pub const MAP_PRIVATE_ANON: i32 = 0x02 | 0x20;
    pub const MADV_HUGEPAGE: i32 = 14;
}

const HUGE: usize = 1 << 21;
/// Blocks at least this big take the mmap path.
const BIG: usize = 1 << 20;

#[inline]
fn round_huge(n: usize) -> usize {
    (n + HUGE - 1) & !(HUGE - 1)
}

#[cfg(target_os = "linux")]
unsafe fn big_alloc(size: usize) -> *mut u8 {
    use std::ffi::c_void;
    let rs = round_huge(size);
    let len = rs + HUGE;
    let p = sys::mmap(std::ptr::null_mut(), len, sys::PROT_RW, sys::MAP_PRIVATE_ANON, -1, 0);
    if p as isize == -1 {
        return std::ptr::null_mut();
    }
    let a = p as usize;
    let al = (a + HUGE - 1) & !(HUGE - 1);
    if al > a {
        sys::munmap(a as *mut c_void, al - a);
    }
    let tail = a + len - (al + rs);
    if tail > 0 {
        sys::munmap((al + rs) as *mut c_void, tail);
    }
    sys::madvise(al as *mut c_void, rs, sys::MADV_HUGEPAGE);
    al as *mut u8
}

#[cfg(target_os = "linux")]
unsafe fn big_free(p: *mut u8, size: usize) {
    sys::munmap(p as *mut std::ffi::c_void, round_huge(size));
}

unsafe impl GlobalAlloc for Alloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        #[cfg(target_os = "linux")]
        if l.size() >= BIG && l.align() <= 4096 {
            return big_alloc(l.size());
        }
        System.alloc(l)
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        #[cfg(target_os = "linux")]
        if l.size() >= BIG && l.align() <= 4096 {
            return big_alloc(l.size()); // fresh anonymous pages are zero
        }
        System.alloc_zeroed(l)
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        #[cfg(target_os = "linux")]
        if l.size() >= BIG && l.align() <= 4096 {
            return big_free(p, l.size());
        }
        System.dealloc(p, l)
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        #[cfg(target_os = "linux")]
        if (l.size() >= BIG || new >= BIG) && l.align() <= 4096 {
            if l.size() >= BIG && round_huge(new) == round_huge(l.size()) {
                return p;
            }
            let nl = Layout::from_size_align_unchecked(new, l.align());
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, l.size().min(new));
                self.dealloc(p, l);
            }
            return q;
        }
        System.realloc(p, l, new)
    }
}
