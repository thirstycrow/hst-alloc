use core::ptr::null_mut;

use crate::local::small_pool::{FreeObject, SmallPool};

#[derive(Debug)]
pub(crate) struct Page {
    pub(crate) free: bool,
    pub(crate) offset_in_span: u8,
    pub(crate) nr_small_alloc: u16,
    pub(crate) span_size: u32,
    pub(crate) link: PageListLink,
    pub(crate) pool: *mut SmallPool,
    pub(crate) free_list: *mut FreeObject,
}

impl Page {
    pub(crate) fn has_free_object(&self) -> bool {
        self.free_list != null_mut()
    }
}

#[derive(Copy, Clone)]
pub(crate) struct Pages(pub(crate) *mut Page);

impl Default for Pages {
    fn default() -> Self {
        Self(null_mut())
    }
}

impl Pages {
    pub(crate) fn new(base: *mut Page) -> Self {
        Self(base)
    }

    pub(crate) unsafe fn at(&self, idx: u32) -> *mut Page {
        self.0.add(idx as _)
    }

    pub(crate) fn ptr(&self) -> *mut Page {
        self.0
    }

    pub(crate) fn index_of(&self, page: *mut Page) -> u32 {
        unsafe { page.offset_from(self.0) as u32 }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PageListLink {
    prev: u32,
    next: u32,
}

impl PageListLink {
    pub(crate) fn has_prev(&self) -> bool {
        self.prev != 0
    }

    pub(crate) fn has_next(&self) -> bool {
        self.next != 0
    }
}

#[derive(Default)]
pub(crate) struct PageList {
    front: u32,
    back: u32,
}

impl PageList {
    pub(crate) fn is_empty(&self) -> bool {
        self.front == 0
    }

    pub(crate) fn erase(&mut self, pages: Pages, span: &mut Page) {
        unsafe {
            if span.link.has_next() {
                (*pages.at(span.link.next)).link.prev = span.link.prev;
            } else {
                self.back = span.link.prev;
            }
            if span.link.has_prev() {
                (*pages.at(span.link.prev)).link.next = span.link.next;
            } else {
                self.front = span.link.next;
            }
        }
    }

    pub(crate) fn push_front(&mut self, pages: Pages, span: &mut Page) {
        unsafe {
            let idx = (span as *mut Page).offset_from(pages.ptr()) as _;
            if !self.is_empty() {
                (*pages.at(self.front)).link.prev = idx;
            } else {
                self.back = idx;
            }
            span.link.next = self.front;
            span.link.prev = 0;
            self.front = idx;
        }
    }

    pub(crate) fn pop_front(&mut self, pages: Pages) -> *mut Page {
        unsafe {
            let front = pages.at(self.front);
            let next = (*front).link.next;
            if next != 0 {
                (*pages.at(next)).link.prev = 0;
            } else {
                self.back = 0;
            }
            self.front = next;
            front
        }
    }
}
