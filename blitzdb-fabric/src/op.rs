use blitzdb_common::FabricError;
use ofi_libfabric_sys::bindgen as ffi;
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

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

pub struct ReadFuture {
    pub(crate) op: Arc<Op>,
    //pub(crate) ep: usize,         // *mut fid_ep cast to usize (Send-safe)
    //pub(crate) cq: usize,         // *mut fid_cq (used only for EAGAIN retry loop)
    //pub(crate) fi_addr: u64,
    //pub(crate) remote_offset: u64,
    //pub(crate) mr_key: u64,
    //pub(crate) issued: bool,
}

impl Future for ReadFuture {
    type Output = Result<(), FabricError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Register waker first, then check result — avoids lost-wakeup race.
        *self.op.waker.lock().unwrap() = Some(cx.waker().clone());

        match self.op.result.load(Ordering::SeqCst) {
            RESULT_PENDING => Poll::Pending,
            RESULT_OK => {
                Poll::Ready(Ok(()))
            }
            err => Poll::Ready(Err(FabricError::from_errno(err.abs() as u32))),
        }
    }
}
