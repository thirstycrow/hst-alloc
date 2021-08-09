use core::alloc::AllocError;
use core::mem::align_of;
use core::ptr::{null_mut, NonNull};

use libc::max_align_t;

use crate::local::page::{PageList, PageListLink};
use crate::local::LocalAllocatorImpl;

use super::PAGE_SIZE;

const IDX_FRAC_BITS: u32 = 2;
const IDX_FRAC_BITS_MASK: u32 = (1 << IDX_FRAC_BITS) - 1;

const fn align_up(v: u32, align: u32) -> u32 {
    (v + align - 1) & !(align - 1)
}

pub(crate) const fn idx_to_size(idx: u32) -> u32 {
    let mut size = (((1 << IDX_FRAC_BITS) | (idx & IDX_FRAC_BITS_MASK)) << (idx >> IDX_FRAC_BITS))
        >> IDX_FRAC_BITS;
    let max_align = align_of::<max_align_t>() as _;
    if size > max_align {
        size = align_up(size, max_align);
    }
    size
}

pub(crate) const fn size_to_idx(size: u32) -> u32 {
    let log2 = size.log2();
    ((size - 1) >> (log2 - IDX_FRAC_BITS)) + (log2 << IDX_FRAC_BITS) - IDX_FRAC_BITS_MASK
}

pub(crate) struct FreeObject {
    next: *mut FreeObject,
}

pub(crate) struct SmallPool {
    object_size: u32,
    least_span_size: u8,
    preferred_span_size: u8,
    free: *mut FreeObject,
    free_count: usize,
    min_free: u32,
    max_free: u32,
    span_list: PageList,
}

impl SmallPool {
    pub(crate) fn new(object_size: u32) -> Self {
        fn span_bytes(span_size: u8) -> u32 {
            span_size as u32 * PAGE_SIZE
        }

        fn waste(span_size: u8, object_size: u32) -> f32 {
            let span_bytes = span_bytes(span_size) as f32;
            span_bytes % object_size as f32 / span_bytes
        }

        let mut span_size: u8 = 1;
        while object_size > span_bytes(span_size) {
            span_size += 1;
        }
        let least_span_size = span_size;

        let mut min_waste = f32::MAX;
        let mut preferred_span_size = 0u8;
        while span_size <= 32 {
            if span_bytes(span_size) / object_size >= 4 {
                let w = waste(span_size, object_size);
                if w < min_waste {
                    min_waste = w;
                    preferred_span_size = span_size;
                    if w < 0.05 {
                        break;
                    }
                }
            }
            span_size += 1;
        }
        if preferred_span_size == 0 {
            preferred_span_size = least_span_size;
        }

        let max_free = 100.max(span_bytes(preferred_span_size) * 2 / object_size);
        let min_free = max_free / 2;

        Self {
            least_span_size,
            preferred_span_size,
            object_size,
            free: null_mut(),
            free_count: 0,
            min_free,
            max_free,
            span_list: PageList::default(),
        }
    }

    pub(crate) fn has_free_object(&self) -> bool {
        self.free != null_mut()
    }

    pub(crate) fn allocate(
        &mut self,
        size: u32,
        allocator: &mut LocalAllocatorImpl,
    ) -> Result<NonNull<[u8]>, AllocError> {
        assert!(self.object_size >= size);
        if !self.has_free_object() {
            self.add_more_objects(allocator);
        }
        if !self.has_free_object() {
            return Err(AllocError);
        }
        let obj = self.pop_free_object();
        unsafe {
            Ok(NonNull::slice_from_raw_parts(
                NonNull::new_unchecked(obj as *mut u8),
                self.object_size as _,
            ))
        }
    }

    fn push_free_object(&mut self, obj: *mut FreeObject) {
        unsafe {
            (*obj).next = self.free;
            self.free = obj;
            self.free_count += 1
        }
    }

    fn pop_free_object(&mut self) -> *mut FreeObject {
        unsafe {
            let obj = self.free;
            self.free = (*self.free).next;
            self.free_count -= 1;
            obj
        }
    }

    fn add_more_objects(&mut self, allocator: &mut LocalAllocatorImpl) {
        unsafe {
            let goal = (self.min_free + self.max_free) / 2;
            while !self.span_list.is_empty() && self.free_count < goal as _ {
                let span = self.span_list.pop_front(allocator.pages);
                while (*span).has_free_object() {
                    let obj = (*span).free_list;
                    (*span).free_list = (*obj).next;
                    self.push_free_object(obj);
                    (*span).nr_small_alloc += 1;
                }
            }
            while self.free_count < goal as _ {
                let mut span_size: u8 = self.preferred_span_size as _;
                let ptr = match allocator.allocate_large_and_trim(span_size as _) {
                    Ok(ptr) => ptr,
                    Err(_) => {
                        span_size = self.least_span_size as _;
                        match allocator.allocate_large_and_trim(span_size as _) {
                            Ok(ptr) => ptr,
                            Err(_) => return,
                        }
                    }
                };
                let data = ptr.as_mut_ptr();
                let span = allocator.to_page(data);
                span_size = (*span).span_size as _;
                for i in 0..span_size {
                    let page_in_span = span.add(i as _);
                    (*page_in_span).offset_in_span = i;
                    (*page_in_span).pool = self as _;
                }
                (*span).nr_small_alloc = 0;
                (*span).free_list = null_mut();
                let mut offset = 0;
                while offset <= span_size as u32 * PAGE_SIZE - self.object_size {
                    let obj = data.add(offset as _) as *mut FreeObject;
                    self.push_free_object(obj);
                    (*span).nr_small_alloc += 1;
                    offset += self.object_size;
                }
            }
        }
    }

    pub(crate) fn deallocate(&mut self, ptr: *mut u8, allocator: &mut LocalAllocatorImpl) {
        let obj: *mut FreeObject = ptr as _;
        self.push_free_object(obj);
        if self.free_count > self.max_free as _ {
            self.trim_free_list(allocator);
        }
    }

    fn trim_free_list(&mut self, allocator: &mut LocalAllocatorImpl) {
        unsafe {
            let goal = (self.min_free + self.max_free) / 2;
            while self.has_free_object() && self.free_count as u32 > goal {
                let obj = self.pop_free_object();
                let page = allocator.to_page(obj as _);
                let span = page.offset(-((*page).offset_in_span as isize));
                if !(*span).has_free_object() {
                    (*span).link = PageListLink::default();
                    self.span_list.push_front(allocator.pages, &mut *span);
                }
                (*obj).next = (*span).free_list;
                (*span).free_list = obj;
                (*span).nr_small_alloc -= 1;
                if (*span).nr_small_alloc == 0 {
                    self.span_list.erase(allocator.pages, &mut *span);
                    let span_idx = span.offset_from(allocator.pages.ptr());
                    allocator.free_span(span_idx as _, (*span).span_size);
                }
            }
        }
    }
}
