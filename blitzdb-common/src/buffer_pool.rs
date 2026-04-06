use std::collections::VecDeque;


pub(crate) struct BufferPool {
    offset_q: VecDeque<usize>,
    buf_base: *mut u8,
    buf_count: usize,
    buf_size: usize,
}

impl BufferPool {
    pub fn new(buf_count: usize, buf_size: usize) -> Self {
        let offset_q: VecDeque<usize> = (0..buf_count).map(|n| n * buf_size).collect();
        let buf_base = unsafe {
            std::alloc::alloc(std::alloc::Layout::from_size_align(buf_size * buf_count, buf_size).unwrap())
        };
        BufferPool {
            offset_q,
            buf_base,
            buf_count,
            buf_size,
        }
    }

    pub fn get_buffer(&mut self) -> Option<*mut u8> {
        self.offset_q.pop_front().map(|offset| unsafe { self.buf_base.add(offset) })
    }

    pub fn return_buffer(&mut self, buf: *mut u8) {
        let offset = unsafe { buf.offset_from(self.buf_base) as usize };
        self.offset_q.push_back(offset);
    }

    pub fn get_base(&self) -> *mut u8 {
        self.buf_base
    }

    pub fn get_len(&self) -> usize {
        self.buf_size * self.buf_count
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.buf_base, std::alloc::Layout::from_size_align(self.buf_size * self.buf_count, self.buf_size).unwrap());
        }
    }
}