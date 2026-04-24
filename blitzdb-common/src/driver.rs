use std::collections::VecDeque;
use std::ffi::{c_int, c_void};
use crate::op::Op;
use ofi_libfabric_sys::bindgen as ffi;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use crate::FabricError;
use log::{error};
use crate::driver::SendResult::{Failed, Sent, WillRetry};

/// Drives a libfabric completion queue on a dedicated polling thread.
///
/// Each completion routes back to the originating Future via the `op_context`
/// pointer stored in the CQ entry — the same pointer we passed as `context` to
/// `fi_read`/`fi_write`. No HashMap required.
pub(crate) struct CqDriver {
    stop: Arc<AtomicBool>,
    _thread: Option<JoinHandle<()>>,
    pub tx: tokio::sync::mpsc::Sender<Request>,
}

pub (crate) const CQ_SIZE: usize = 1024;
const DISPATCH_BATCH_SIZE: usize = 64;

pub(crate) enum Request {
    Read {
        op: Arc<Op>,
        ep: usize, // *mut ffi::fid_ep
        buf_ptr: usize, // *mut c_void,
        len: usize,
        buf_desc: usize,
        fi_addr: u64,
        remote_offset: u64,
        mr_key: u64,
    },
}

impl Request {
    fn try_send(self, retry_q: &mut VecDeque<Request>) -> SendResult {
        match self {
            Request::Read {
                op, ep, buf_ptr, len, buf_desc, fi_addr, remote_offset, mr_key,
            } => {
                let op_ptr = Arc::into_raw(op);
                let ep = ep as *mut ffi::fid_ep;
                let buf_ptr = buf_ptr as *mut c_void;
                let desc_ptr = buf_desc as *mut c_void;
                match FabricError::from_ret(unsafe { ffi::fi_read(ep, buf_ptr, len, desc_ptr, fi_addr, remote_offset, mr_key, op_ptr as *mut _) as c_int }) {
                    Err(FabricError::Again) => {
                        let op = unsafe {
                            Arc::from_raw(op_ptr)
                        };
                        let ep = ep as usize;
                        let buf_ptr = buf_ptr as usize;
                        retry_q.push_back(Request::Read {
                            op, ep, buf_ptr, len, buf_desc,  fi_addr, remote_offset, mr_key,
                        });
                        WillRetry
                    },
                    Err(e) => {
                        unsafe { Arc::from_raw(op_ptr) }.complete(Err(e));
                        Failed
                    },
                    _ => Sent
                }
            }
        }
    }
}

enum SendResult {
    Sent,
    WillRetry,
    Failed
}

impl CqDriver {
    /// Spawn the polling thread for the given CQ.
    pub(crate) fn spawn(cq: *mut ffi::fid_cq) -> anyhow::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let cq_addr = cq as usize;

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Request>(256);

        let handle = thread::Builder::new()
            .name("cq-poller".into())
            .spawn(move || {
                let cq = cq_addr as *mut ffi::fid_cq;
                let mut cq_buf: [ffi::fi_cq_entry; CQ_SIZE] = unsafe { std::mem::zeroed() };
                let mut retry_q: VecDeque<Request> = VecDeque::new();
                let mut in_flight: usize = 0;

                while !stop2.load(Ordering::Relaxed) {
                    let mut dispatched = 0;

                    // drain the retry queue
                    while retry_q.len() > 0 && dispatched < DISPATCH_BATCH_SIZE && in_flight < CQ_SIZE {
                        match retry_q.pop_front().unwrap().try_send(&mut retry_q) {
                            WillRetry => {
                                break;
                            }
                            Failed => continue,
                            Sent => {
                                dispatched += 1;
                                in_flight += 1;
                            }
                        }
                    }

                    // drain the request queue
                    while let Ok(req) = rx.try_recv() && dispatched < DISPATCH_BATCH_SIZE {
                        match req.try_send(&mut retry_q) {
                            WillRetry => {
                                break;
                            }
                            Failed => continue,
                            Sent => {
                                dispatched += 1;
                                in_flight += 1;
                            }
                        }
                    }

                    // process completions
                    let ret =
                        unsafe { ffi::fi_cq_read(cq, &mut cq_buf as *mut _ as *mut _, cq_buf.len()) };

                    if ret > 0 {
                        for entry in &cq_buf[..ret as usize] {
                            let op = unsafe {
                                Arc::from_raw(entry.op_context as *const Op)
                            };
                            op.complete(Ok(()));
                            in_flight -= 1;
                        }
                    } else if ret == -(ffi::FI_EAGAIN as isize) {
                        // No completions available — yield the CPU briefly.
                        if dispatched == 0 {
                            std::hint::spin_loop();
                        }
                    } else if ret == -(ffi::FI_EAVAIL as isize) {
                        // Error completion: peek at the error entry.
                        let mut err_entry: ffi::fi_cq_err_entry = unsafe { std::mem::zeroed() };
                        let err_ret = unsafe { ffi::fi_cq_readerr(cq, &mut err_entry, 0) };
                        match err_ret {
                            1 => {
                                let op = unsafe {
                                    Arc::from_raw(err_entry.op_context as *const Op)
                                };
                                op.complete(Err(FabricError::from_errno(err_entry.err as u32)));
                                in_flight -= 1;
                            }
                            _ => {
                                error!("CQ readerr failed: {}", FabricError::from_errno(err_ret.abs() as u32));
                            }
                        }
                    } else {
                        let err = FabricError::from_errno(ret.abs() as u32);
                        error!("CQ read failed: {}", err);
                    }
                }
            })
            .expect("Failed to spawn CQ polling thread");

        Ok(CqDriver { stop, _thread: Some(handle), tx })
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
