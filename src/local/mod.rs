use core::alloc::Layout;
use core::alloc::{AllocError, Allocator};
use core::cell::{Cell, RefCell};
use core::intrinsics::{ceilf64, log2f64, transmute, unlikely};
use core::mem::{size_of, MaybeUninit};
use core::ptr::{null_mut, NonNull};
use core::sync::atomic::Ordering;
use core::task::{Context, Poll};

use libc::*;

use page::{Page, PageList, Pages};

use crate::local::small_pool::{idx_to_size, size_to_idx, FreeObject, SmallPool};
use crate::{
    align_down, align_up, CrossCpuFreeList, MemoryLocation, Params, LOCAL_SPACE_SIZE_IN_BITS,
    PAGE_SIZE, XCPU_FREE_LIST,
};

mod page;
mod small_pool;

const PAGE_BITS: u32 = PAGE_SIZE.log2();
const NR_SPAN_LISTS: usize = 32;
const NR_SMALL_POOLS: u32 = size_to_idx(4 * PAGE_SIZE) + 1;
const MAX_SMALL_ALLOCATION: u32 = idx_to_size(NR_SMALL_POOLS - 1);

pub(crate) struct LocalAllocator {
    mem_base: *mut u8,
    shard_id: u8,
    nr_shards: u8,
    inner: RefCell<LocalAllocatorImpl>,
    allocated: Cell<usize>,
}

unsafe impl Allocator for LocalAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let mut inner = self.inner.borrow_mut();
        let ptr = if layout.size() <= MAX_SMALL_ALLOCATION as _ {
            inner.allocate_small(layout)
        } else {
            inner.allocate_large(layout)
        };
        if let Ok(ptr) = &ptr {
            self.allocated.set(self.allocated.get() + ptr.len());
        }
        ptr
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, _: Layout) {
        self.free(ptr.as_ptr());
    }
}

impl LocalAllocator {
    pub(crate) fn create(shard_id: u8, params: &Params) -> Self {
        log::info!(
            "[shard {}] Initializing thread local allocator ...",
            shard_id
        );

        let mut allocator = LocalAllocatorImpl::new(params.base, shard_id);
        let shard_memory = align_down(allocator.base, params.shard_memory, PAGE_SIZE as _);
        allocator.initialize(shard_memory);

        log::info!(
            "[shard {}] Memory size: {} MiB",
            shard_id,
            shard_memory / (1 << 20)
        );
        log::info!(
            "[shard {}] Memory range: {:?}-{:?}",
            shard_id,
            allocator.base,
            unsafe { allocator.base.add(shard_memory) }
        );

        Self {
            mem_base: params.base,
            shard_id,
            nr_shards: params.nr_shards,
            inner: RefCell::new(allocator),
            allocated: Cell::new(0),
        }
    }

    pub(crate) fn memory_location(&self, ptr: *mut u8) -> MemoryLocation {
        super::memory_location(ptr, self.mem_base, Some(self.shard_id), self.nr_shards)
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.allocated.get()
    }

    fn free(&self, ptr: *mut u8) {
        let actual_size = self.inner.borrow_mut().free(ptr);
        self.allocated.set(self.allocated.get() - actual_size);
    }

    pub(crate) fn poll_drain_cross_shard_freelist(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        unsafe {
            #[cfg(test)]
            log::info!("[shard {}] drain free list", self.shard_id);
            let CrossCpuFreeList {
                head,
                waker,
                drainer,
            } = &XCPU_FREE_LIST[self.shard_id as usize];

            if head.load(Ordering::Relaxed).is_null() {
                let mut _waker = waker.borrow_mut();
                assert!(_waker.is_none());
                *_waker = Some(cx.waker().clone());
                drainer.swap(waker.as_ptr(), Ordering::Release);
            } else {
                let mut p = head.swap(null_mut(), Ordering::Acquire);
                while !p.is_null() {
                    let n = (*p).next;
                    #[cfg(test)]
                    log::info!("[shard {}] free item: {:?}", self.shard_id, p);
                    self.free(p as _);
                    p = n;
                }
                cx.waker().wake_by_ref();
            }

            Poll::Pending
        }
    }
}

pub struct LocalAllocatorImpl {
    base: *mut u8,
    nr_pages: u32,
    nr_free_pages: u32,
    pages: Pages,
    free_spans: [PageList; NR_SPAN_LISTS],
    small_pools: [SmallPool; NR_SMALL_POOLS as _],
}

impl LocalAllocatorImpl {
    fn new(mem_base: *mut u8, shard_id: u8) -> Self {
        unsafe {
            Self {
                base: mem_base.add((shard_id as usize) << LOCAL_SPACE_SIZE_IN_BITS),
                nr_pages: 0,
                nr_free_pages: 0,
                pages: Pages::default(),
                free_spans: Self::create_span_lists(),
                small_pools: Self::create_small_pools(),
            }
        }
    }

    fn create_span_lists() -> [PageList; NR_SPAN_LISTS] {
        let mut span_lists: [MaybeUninit<PageList>; NR_SPAN_LISTS as _] =
            MaybeUninit::uninit_array();
        for i in 0..span_lists.len() {
            span_lists[i].write(PageList::default());
        }
        unsafe { transmute(span_lists) }
    }

    fn create_small_pools() -> [SmallPool; NR_SMALL_POOLS as _] {
        let mut pools: [MaybeUninit<SmallPool>; NR_SMALL_POOLS as _] = MaybeUninit::uninit_array();
        for i in 0..pools.len() {
            pools[i].write(SmallPool::new(idx_to_size(i as _)));
        }
        unsafe { transmute(pools) }
    }

    fn initialize(&mut self, memory: usize) {
        unsafe {
            let r = mmap(
                self.base as _,
                memory,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED,
                -1,
                0,
            );
            if unlikely(r == MAP_FAILED) {
                panic!()
            }
            madvise(self.base as _, memory, MADV_HUGEPAGE);

            self.nr_pages = memory as u32 / PAGE_SIZE;
            self.pages = Pages::new(r as _);
            // Reserve pages for metadata
            let mut reserved = size_of::<Page>() * (self.nr_pages as usize + 1);
            reserved = align_up(self.base, reserved, PAGE_SIZE as _);
            let mut nr_reserved_pages = reserved as u32 / PAGE_SIZE;
            nr_reserved_pages = 1 << ceilf64(log2f64(nr_reserved_pages as _)) as u32;
            for i in 0..nr_reserved_pages {
                (*self.pages.at(i)).free = false;
            }
            (*self.pages.at(self.nr_pages)).free = false;
            self.free_span_unaligned(
                nr_reserved_pages as u32,
                self.nr_pages - nr_reserved_pages as u32,
            );
        }
    }

    fn free_span_no_merge(&mut self, span_start: u32, nr_pages: u32) {
        unsafe {
            assert!(nr_pages > 0);
            self.nr_free_pages += nr_pages;
            let span = self.pages.at(span_start as _);
            let span_end = self.pages.at((span_start + nr_pages - 1) as _);
            (*span).free = true;
            (*span_end).free = true;
            (*span).span_size = nr_pages;
            (*span_end).span_size = nr_pages;
            let idx = Self::index_of(nr_pages);
            self.free_spans[idx].push_front(self.pages, &mut *span);
        }
    }

    fn grow_span(&mut self, span_start: &mut u32, nr_pages: &mut u32, idx: usize) -> bool {
        unsafe {
            let which = ((*span_start >> idx) & 1) as i64; // 0=lower, 1=upper

            // locate first page of upper buddy or last page of lower buddy
            // examples: span_start = 0x10 nr_pages = 0x08 -> buddy = 0x18  (which = 0)
            //           span_start = 0x18 nr_pages = 0x08 -> buddy = 0x17  (which = 1)
            let delta = ((which ^ 1) << idx) | -which;
            let buddy = (*span_start as i64 + delta) as u32;
            let buddy_page = self.pages.at(buddy);
            if (*buddy_page).free && (*buddy_page).span_size == *nr_pages {
                self.free_spans[idx]
                    .erase(self.pages, &mut *self.pages.at(*span_start ^ *nr_pages));
                self.nr_free_pages -= *nr_pages;
                *span_start &= !*nr_pages;
                *nr_pages *= 2;
                return true;
            }
            return false;
        }
    }

    fn free_span(&mut self, mut span_start: u32, mut nr_pages: u32) {
        let mut idx = Self::index_of(nr_pages);
        while self.grow_span(&mut span_start, &mut nr_pages, idx) {
            idx += 1;
        }
        self.free_span_no_merge(span_start, nr_pages);
    }

    fn free_span_unaligned(&mut self, mut span_start: u32, mut nr_pages: u32) {
        while nr_pages != 0 {
            let mut start_nr_bits = 32;
            if span_start != 0 {
                start_nr_bits = span_start.trailing_zeros();
            }
            let size_nr_bits = nr_pages.trailing_zeros();
            let now = 1 << start_nr_bits.min(size_nr_bits);
            self.free_span(span_start, now);
            span_start += now;
            nr_pages -= now;
        }
    }

    // Smallest index i such that all spans stored in the index are >= pages.
    fn index_of(min_span_len: u32) -> usize {
        if min_span_len == 1 {
            return 0;
        }
        (u32::BITS - (min_span_len - 1).leading_zeros()) as _
    }

    fn to_page(&self, ptr: *mut u8) -> *mut Page {
        unsafe {
            let idx = ptr.offset_from(self.base) as u32 / PAGE_SIZE;
            self.pages.at(idx as _)
        }
    }

    fn allocate_large(&mut self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let size_in_pages = ((layout.size() + PAGE_SIZE as usize - 1) >> PAGE_BITS as usize) as u32;
        self.allocate_large_and_trim(size_in_pages)
    }

    fn allocate_large_and_trim(&mut self, nr_pages: u32) -> Result<NonNull<[u8]>, AllocError> {
        unsafe {
            match self.find_and_unlink_span(nr_pages) {
                None => Err(AllocError),
                Some(span) => {
                    let mut span_size = (*span).span_size;
                    let span_idx = self.pages.index_of(span);
                    self.nr_free_pages -= span_size;
                    while span_size > nr_pages * 2 {
                        span_size /= 2;
                        let other_span_idx = span_idx + span_size;
                        self.free_span_no_merge(other_span_idx, span_size);
                    }
                    let span_end = self.pages.at(span_idx + span_size - 1);
                    (*span).free = false;
                    (*span_end).free = false;
                    (*span).span_size = span_size;
                    (*span_end).span_size = span_size;
                    (*span).pool = null_mut();
                    let ptr = self.base.add((span_idx * PAGE_SIZE) as _);
                    Ok(NonNull::slice_from_raw_parts(
                        NonNull::new_unchecked(ptr),
                        span_size as usize * PAGE_SIZE as usize,
                    ))
                }
            }
        }
    }

    fn find_and_unlink_span(&mut self, nr_pages: u32) -> Option<*mut Page> {
        let mut idx = Self::index_of(nr_pages);
        if nr_pages >= (2 << idx) {
            return None;
        }
        while idx < NR_SPAN_LISTS && self.free_spans[idx].is_empty() {
            idx += 1;
        }
        if idx == NR_SPAN_LISTS {
            return None;
        }
        let span_list = &mut self.free_spans[idx];
        Some(span_list.pop_front(self.pages))
    }

    fn free_large(&mut self, ptr: *mut u8) -> usize {
        unsafe {
            let idx = ptr.offset_from(self.base) as u32 / PAGE_SIZE;
            let span = self.pages.at(idx as _);
            let nr_pages = (*span).span_size;
            self.free_span(idx, nr_pages);
            (nr_pages * PAGE_SIZE) as _
        }
    }

    fn free(&mut self, ptr: *mut u8) -> usize {
        unsafe {
            let span = self.to_page(ptr);
            if !(*span).pool.is_null() {
                let pool = (*span).pool;
                (*pool).deallocate(ptr, self)
            } else {
                self.free_large(ptr)
            }
        }
    }

    fn allocate_small(&mut self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        unsafe {
            let size = layout.size().max(size_of::<FreeObject>());
            let idx = small_pool::size_to_idx(size as _);
            let pool: *mut SmallPool = &mut self.small_pools[idx as usize] as *mut SmallPool;
            (*pool).allocate(layout.size() as _, self)
        }
    }
}
