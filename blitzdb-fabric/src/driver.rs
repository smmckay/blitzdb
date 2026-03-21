use crate::op::{Op, RESULT_OK};
use ofi_libfabric_sys::bindgen as ffi;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// Drives a libfabric completion queue on a dedicated polling thread.
///
/// Each completion routes back to the originating Future via the `op_context`
/// pointer stored in the CQ entry — the same pointer we passed as `context` to
/// `fi_read`/`fi_write`. No HashMap required.
pub(crate) struct CqDriver {
    stop: Arc<AtomicBool>,
    _thread: Option<JoinHandle<()>>,
}

impl CqDriver {
    /// Spawn the polling thread for the given CQ.
    pub(crate) fn spawn(cq: *mut ffi::fid_cq) -> anyhow::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let cq_addr = cq as usize;

        let handle = thread::Builder::new()
            .name("cq-poller".into())
            .spawn(move || {
                let cq = cq_addr as *mut ffi::fid_cq;
                while !stop2.load(Ordering::Relaxed) {
                    let mut entry: ffi::fi_cq_entry = unsafe { std::mem::zeroed() };
                    let ret =
                        unsafe { ffi::fi_cq_read(cq, &mut entry as *mut _ as *mut _, 1) };

                    if ret == 1 {
                        // Reconstruct the Arc<Op> from the context pointer.
                        // The Future issued Arc::into_raw (via mem::forget) to keep
                        // the refcount elevated; this call reclaims that reference.
                        let op = unsafe {
                            Arc::from_raw(entry.op_context as *const Op)
                        };
                        op.result.store(RESULT_OK, Ordering::SeqCst);
                        if let Some(waker) = op.waker.lock().unwrap().take() {
                            waker.wake();
                        }
                    } else if ret < 0 && ret != -(ffi::FI_EAGAIN as isize) {
                        // Error completion: peek at the error entry.
                        let mut err_entry: ffi::fi_cq_err_entry =
                            unsafe { std::mem::zeroed() };
                        let err_ret = unsafe {
                            ffi::fi_cq_readerr(cq, &mut err_entry, 0)
                        };
                        if err_ret == 1 {
                            let op = unsafe {
                                Arc::from_raw(err_entry.op_context as *const Op)
                            };
                            let code = -(err_entry.err as i32);
                            op.result.store(if code == 0 { -1 } else { code }, Ordering::SeqCst);
                            if let Some(waker) = op.waker.lock().unwrap().take() {
                                waker.wake();
                            }
                        }
                    } else {
                        // No completions available — yield the CPU briefly.
                        std::hint::spin_loop();
                    }
                }
            })
            .expect("Failed to spawn CQ polling thread");

        Ok(CqDriver { stop, _thread: Some(handle) })
    }

    pub(crate) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self._thread.take() {
            handle.join().unwrap();
        }
    }
}

impl Drop for CqDriver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}
