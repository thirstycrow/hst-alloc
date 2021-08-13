#![feature(allocator_api)]
#![feature(alloc_layout_extra)]
#![feature(slice_ptr_get)]
#![feature(nonnull_slice_from_raw_parts)]
#![feature(rustc_private)]
#![feature(core_intrinsics)]
#![feature(thread_local)]
#![feature(int_log)]
#![feature(maybe_uninit_uninit_array)]
#![feature(slice_ptr_len)]
#![feature(future_poll_fn)]
#![no_std]

#[cfg(test)]
#[macro_use]
extern crate std;

use core::alloc::{Allocator, GlobalAlloc, Layout};
use core::cell::{Cell, RefCell};
use core::future::{poll_fn, Future};
use core::intrinsics::unlikely;
use core::ops::{Deref, DerefMut};
use core::ptr::{null_mut, NonNull};
use core::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use core::task::Waker;

use libc::*;

use crate::local::LocalAllocator;

mod local;
#[cfg(test)]
mod tests;

const PAGE_SIZE: u32 = 4096;
const MEMORY_SPACE_SIZE: usize = 1 << 44;
const LOCAL_SPACE_SIZE_IN_BITS: usize = 36;
const MAX_SHARDS: usize = 256;

#[thread_local]
static LOCAL_ALLOCATOR: RefCell<Option<LocalAllocator>> = RefCell::new(None);
#[thread_local]
static LOG_ENABLED: Cell<bool> = Cell::new(false);

#[allow(clippy::declare_interior_mutable_const)]
const EMPTY_XCPU_FREE_LIST: CrossCpuFreeList = CrossCpuFreeList::default();

static XCPU_FREE_LIST: [CrossCpuFreeList; MAX_SHARDS] = [EMPTY_XCPU_FREE_LIST; MAX_SHARDS];

#[derive(Eq, PartialEq, Debug)]
pub enum MemoryLocation {
    Local(u8),
    Foreign(u8),
    Global,
}

struct Params {
    base: *mut u8,
    nr_shards: u8,
    shard_memory: usize,
}

#[repr(align(64))]
struct CrossCpuFreeList {
    head: AtomicPtr<CrossCpuFreeItem>,
    waker: RefCell<Option<Waker>>,
    drainer: AtomicPtr<Option<Waker>>,
}

unsafe impl Sync for CrossCpuFreeList {}

impl CrossCpuFreeList {
    const fn default() -> Self {
        Self {
            waker: RefCell::new(None),
            drainer: AtomicPtr::new(null_mut()),
            head: AtomicPtr::new(null_mut()),
        }
    }
}

struct CrossCpuFreeItem {
    next: *mut CrossCpuFreeItem,
}

pub struct HstAlloc<G> {
    global: G,
    params: AtomicPtr<Params>,
}

impl<G> HstAlloc<G> {
    pub const fn new(global: G) -> Self {
        Self {
            global,
            params: AtomicPtr::new(null_mut()),
        }
    }
}

impl<G: GlobalAlloc> HstAlloc<G> {
    pub fn initialize(&self, nr_shards: u8) {
        static CALLED: AtomicBool = AtomicBool::new(false);
        if CALLED
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
            .is_ok()
        {
            log::info!("Intializing HstAlloc with {} shards ...", nr_shards);

            unsafe {
                let params = self.global.alloc(Layout::new::<Params>());
                if params.is_null() {
                    panic!()
                }

                let mem_base = init_addr_space();
                let phys_pages = sysconf(_SC_PHYS_PAGES);
                let available_memory = phys_pages * PAGE_SIZE as i64;
                let usable_memory = calculate_memory(available_memory as _);
                let shard_memory = usable_memory / nr_shards as usize;

                log::info!("  mem base: {:?}", mem_base);
                log::info!("  phys pages: {}", phys_pages);
                log::info!(
                    "  avail mem: {} MiB ({} MiB per shard)",
                    usable_memory / (1 << 20),
                    shard_memory / (1 << 20)
                );

                let params = &mut *(params as *mut Params);
                params.base = mem_base;
                params.nr_shards = nr_shards;
                params.shard_memory = shard_memory;

                self.params.store(params, Ordering::Release);
            }
        } else {
            panic!("HstAlloc is already initialized!")
        }
    }

    pub fn create_local_allocator(&self, shard_id: u8) -> impl Future<Output = ()> {
        assert!(LOCAL_ALLOCATOR.borrow().is_none());
        unsafe {
            match self.params.load(Ordering::Acquire).as_ref() {
                None => panic!("HstAlloc is not initialized"),
                Some(params) => {
                    *LOCAL_ALLOCATOR.borrow_mut() = Some(LocalAllocator::create(shard_id, params));
                    poll_fn(|cx| {
                        let mut allocator = LOCAL_ALLOCATOR.borrow_mut();
                        let allocator = allocator.as_mut().unwrap();
                        allocator.poll_drain_cross_shard_freelist(cx)
                    })
                }
            }
        }
    }

    pub fn memory_location(&self, ptr: *mut u8) -> MemoryLocation {
        unsafe {
            match LOCAL_ALLOCATOR.borrow().deref() {
                Some(local) => local.memory_location(ptr),
                None => match self.params.load(Ordering::Acquire).as_ref() {
                    None => MemoryLocation::Global,
                    Some(params) => memory_location(ptr, params.base, None, params.nr_shards),
                },
            }
        }
    }

    #[inline]
    pub fn allocated_bytes(&self) -> Option<usize> {
        LOCAL_ALLOCATOR
            .borrow()
            .deref()
            .as_ref()
            .map(|local| local.allocated_bytes())
    }

    pub fn enable_log(&self) {
        LOG_ENABLED.set(true);
    }

    pub fn disable_log(&self) {
        LOG_ENABLED.set(false);
    }
}

unsafe impl<G: GlobalAlloc> GlobalAlloc for HstAlloc<G> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == 0 {
            return layout.align() as *mut u8;
        }
        match LOCAL_ALLOCATOR.borrow_mut().deref_mut() {
            Some(local_allocator) if layout.size() > 0 => match local_allocator.allocate(layout) {
                Ok(ptr) => ptr.as_mut_ptr(),
                Err(_) => null_mut(),
            },
            _ => self.global.alloc(layout),
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.size() == 0 {
            return;
        }
        match self.memory_location(ptr) {
            MemoryLocation::Global => self.global.dealloc(ptr, layout),
            MemoryLocation::Local(_) => {
                let mut local_allocator = LOCAL_ALLOCATOR.borrow_mut();
                let local_allocator = local_allocator.deref_mut().as_mut().unwrap();
                local_allocator.deallocate(NonNull::new_unchecked(ptr), layout)
            }
            MemoryLocation::Foreign(shard_id) => free_cross_shard(shard_id, ptr),
        }
    }
}

fn init_addr_space() -> *mut u8 {
    unsafe {
        let r = mmap(
            null_mut(),
            2 * MEMORY_SPACE_SIZE,
            PROT_NONE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
            -1,
            0,
        );
        if unlikely(r == MAP_FAILED) {
            panic!()
        }
        madvise(r, 2 * MEMORY_SPACE_SIZE, MADV_DONTDUMP);
        let offset = r.align_offset(MEMORY_SPACE_SIZE);
        let known = r.add(offset);
        munmap(r, offset);
        munmap(known.add(MEMORY_SPACE_SIZE), MEMORY_SPACE_SIZE - offset);
        known as _
    }
}

fn calculate_memory(available_memory: usize) -> usize {
    let reserved = ((0.07 * available_memory as f64) as usize).max(1536 << 20);
    let min_memory = 512 << 20;
    let mem = min_memory.max(available_memory - reserved);
    if mem > available_memory {
        panic!(
            "insufficient physical memory: needed {} available {}",
            mem, available_memory
        );
    }
    mem
}

fn align_up(base: *mut u8, len: usize, align: usize) -> usize {
    unsafe {
        let raw = base.add(len);
        len + raw.align_offset(align)
    }
}

fn align_down(base: *mut u8, len: usize, align: usize) -> usize {
    let mut aligned = align_up(base, len, align);
    if aligned > len {
        aligned -= align;
    }
    aligned
}

fn memory_location(
    ptr: *mut u8,
    mem_base: *mut u8,
    shard_id: Option<u8>,
    nr_shards: u8,
) -> MemoryLocation {
    unsafe {
        let mem_shard_id = ptr.offset_from(mem_base) >> LOCAL_SPACE_SIZE_IN_BITS;
        if unlikely(mem_shard_id < 0 || mem_shard_id >= nr_shards as _) {
            MemoryLocation::Global
        } else {
            let mem_shard_id = mem_shard_id as u8;
            if Some(mem_shard_id) == shard_id {
                MemoryLocation::Local(mem_shard_id)
            } else {
                MemoryLocation::Foreign(mem_shard_id)
            }
        }
    }
}

fn free_cross_shard(shard_id: u8, ptr: *mut u8) {
    unsafe {
        let p = ptr as *mut CrossCpuFreeItem;
        let CrossCpuFreeList { drainer, head, .. } = &XCPU_FREE_LIST[shard_id as usize];
        let mut old = head.load(Ordering::Relaxed);
        loop {
            (*p).next = old;
            match head.compare_exchange_weak(old, p, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,
                Err(current) => old = current,
            }
        }
        let drainer = drainer.swap(null_mut(), Ordering::AcqRel);
        if !drainer.is_null() {
            #[cfg(test)]
            log::info!("wake drainer: {:?}", drainer);
            let waker = (*drainer).take();
            waker.unwrap().wake_by_ref();
        }
    }
}
