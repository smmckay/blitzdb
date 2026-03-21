use ofi_libfabric_sys::bindgen as ffi;
use std::cell::UnsafeCell;
use std::ffi::c_int;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;
use blitzdb_common::FabricError;

// Sentinel values stored in Op::result.
pub(crate) const RESULT_PENDING: i32 = 0;
pub(crate) const RESULT_OK: i32 = 1;

/// Per-operation state shared between the issuing Future and the CQ polling thread.
///
/// SAFETY: `fi_context` MUST be the first field. Because this struct is `#[repr(C)]`
/// the pointer to the struct and the pointer to the `ctx` field are identical.
/// We pass `&op.ctx` as the `context` argument to `fi_read`/`fi_write`; the CQ
/// entry's `op_context` field will point back to `ctx`, which is also the start of
/// the `Op` allocation. The polling thread casts that pointer to `*const Op` and
/// reconstructs the `Arc<Op>` — no HashMap required.
#[repr(C)]
pub(crate) struct Op {
    /// Opaque storage for the provider in FI_CONTEXT mode. Must be first.
    pub ctx: UnsafeCell<ffi::fi_context>,
    /// Waker stored by the Future; called by the polling thread on completion.
    pub waker: Mutex<Option<Waker>>,
    /// 0 = pending, 1 = ok, negative = -errno from CQ error.
    pub result: AtomicI32,
}

// SAFETY: The fi_context field is written only by the provider (single thread),
// and the waker/result fields are protected by Mutex + AtomicI32 respectively.
unsafe impl Send for Op {}
unsafe impl Sync for Op {}

impl Op {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Op {
            ctx: UnsafeCell::new(unsafe { std::mem::zeroed() }),
            waker: Mutex::new(None),
            result: AtomicI32::new(RESULT_PENDING),
        })
    }

    /// Returns the fi_context pointer to pass as `context` to fi_read/fi_write.
    /// The caller must ensure the Arc stays alive until the CQ completion arrives.
    pub(crate) fn ctx_ptr(op: &Arc<Self>) -> *mut std::ffi::c_void {
        op.ctx.get() as *mut _
    }
}

/// Future for a one-sided `fi_read` that returns an owned buffer on completion.
pub struct ReadFuture {
    pub(crate) op: Arc<Op>,
    pub(crate) buf: Vec<u8>,
    pub(crate) ep: usize,         // *mut fid_ep cast to usize (Send-safe)
    pub(crate) cq: usize,         // *mut fid_cq (used only for EAGAIN retry loop)
    pub(crate) fi_addr: u64,
    pub(crate) remote_offset: u64,
    pub(crate) mr_key: u64,
    pub(crate) issued: bool,
}

impl Future for ReadFuture {
    type Output = Result<Vec<u8>, FabricError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Issue fi_read on first poll.
        if !self.issued {
            let ep = self.ep as *mut ffi::fid_ep;
            let buf_ptr = self.buf.as_mut_ptr() as *mut _;
            let len = self.buf.len();
            let fi_addr = self.fi_addr;
            let remote_offset = self.remote_offset;
            let mr_key = self.mr_key;
            let _cq = self.cq as *mut ffi::fid_cq;

            // Clone the Arc to give the poller thread its own reference.
            // Arc::into_raw bumps the refcount; the thread will call Arc::from_raw to reclaim it.
            let poller_ref = Arc::clone(&self.op);
            let ctx = Op::ctx_ptr(&poller_ref);
            std::mem::forget(poller_ref); // refcount stays elevated until poller reclaims it

            // Issue the read, retrying on EAGAIN (provider establishing connection).
            loop {
                match FabricError::from_ret(unsafe { ffi::fi_read(ep, buf_ptr, len, ptr::null_mut(), fi_addr, remote_offset, mr_key, ctx) as c_int }) {
                    Err(FabricError::Again) => {
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                    _ => break,
                }
            }
            self.issued = true;
        }

        // Register waker first, then check result — avoids lost-wakeup race.
        *self.op.waker.lock().unwrap() = Some(cx.waker().clone());

        match self.op.result.load(Ordering::SeqCst) {
            RESULT_PENDING => Poll::Pending,
            RESULT_OK => {
                // Take ownership of the buffer out of self before returning.
                // SAFETY: we own buf and the DMA is complete.
                let buf = std::mem::take(&mut self.buf);
                Poll::Ready(Ok(buf))
            }
            err => Poll::Ready(Err(FabricError::from_errno(err.abs() as u32))),
        }
    }
}
