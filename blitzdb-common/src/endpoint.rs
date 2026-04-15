use crate::FabricError;
use crate::driver::CQ_SIZE;
use crate::driver::{CqDriver, Request};
use crate::op::{Op, ReadFuture};
use anyhow::Context;
use log::info;
use ofi_libfabric_sys::bindgen as ffi;
use std::ffi::CString;
use std::ptr;

/// Wraps a libfabric endpoint with async read/write operations.
///
/// The `CqDriver` inside polls the completion queue on a dedicated thread and
/// wakes Tokio tasks when their operations complete.
pub struct FabricEndpoint {
    info: *mut ffi::fi_info,
    fabric: *mut ffi::fid_fabric,
    domain: *mut ffi::fid_domain,
    cq: *mut ffi::fid_cq,
    av: *mut ffi::fid_av,
    ep: *mut ffi::fid_ep,
    max_read_size: usize,
    buffer_mr: *mut ffi::fid_mr,
    buffer_ptr: *mut u8,
    buffer_len: usize,
    buffer_q: crossbeam_queue::ArrayQueue<*mut u8>,
    _driver: CqDriver,
}

// Safety: FabricEndpoint's raw pointers are libfabric handles that are thread-safe.
// The CQ driver thread already accesses the endpoint from a separate thread.
unsafe impl Send for FabricEndpoint {}
unsafe impl Sync for FabricEndpoint {}

pub struct FabricMrGuard<'a> {
    mr: *mut ffi::fid_mr,
    pub mr_key: u64,
    _mr_buf: &'a [u8],
}

impl Drop for FabricMrGuard<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::fi_close(&mut (*self.mr).fid);
        }
    }
}

pub struct FabricRecvBuffer<'a> {
    buf: *mut u8,
    ep: &'a FabricEndpoint,
}

impl Drop for FabricRecvBuffer<'_> {
    fn drop(&mut self) {
        self.ep.put_recv_buffer(self.buf);
    }
}

unsafe impl Send for FabricRecvBuffer<'_> {}

impl FabricEndpoint {
    pub fn new(max_read_size: usize) -> anyhow::Result<Self> {
        unsafe {
            let version = ffi::fi_version();
            let info = Self::getinfo(version, "efa")
                .or_else(|efa_err| {
                    info!("EFA provider unavailable ({efa_err:#}), falling back to TCP");
                    Self::getinfo(version, "tcp;ofi_rxm")
                })
                .context("fi_getinfo: no usable provider (tried efa, tcp)")?;
            let prov = std::ffi::CStr::from_ptr((*(*info).fabric_attr).prov_name);
            info!("Using provider: {}", prov.to_string_lossy());

            let mut fabric = ptr::null_mut();
            FabricError::from_ret(ffi::fi_fabric(
                (*info).fabric_attr,
                &mut fabric,
                ptr::null_mut(),
            ))
            .context("fi_fabric")?;

            let mut domain = ptr::null_mut();
            FabricError::from_ret(ffi::fi_domain(fabric, info, &mut domain, ptr::null_mut()))
                .context("fi_domain")?;

            let mut cq_attr: ffi::fi_cq_attr = std::mem::zeroed();
            cq_attr.format = ffi::fi_cq_format_FI_CQ_FORMAT_CONTEXT;
            cq_attr.size = CQ_SIZE;
            let mut cq = ptr::null_mut();
            FabricError::from_ret(ffi::fi_cq_open(
                domain,
                &mut cq_attr,
                &mut cq,
                ptr::null_mut(),
            ))
            .context("fi_cq_open")?;

            let mut av_attr: ffi::fi_av_attr = std::mem::zeroed();
            av_attr.type_ = ffi::fi_av_type_FI_AV_TABLE;
            let mut av = ptr::null_mut();
            FabricError::from_ret(ffi::fi_av_open(
                domain,
                &mut av_attr,
                &mut av,
                ptr::null_mut(),
            ))
            .context("fi_av_open")?;

            let mut ep = ptr::null_mut();
            FabricError::from_ret(ffi::fi_endpoint(domain, info, &mut ep, ptr::null_mut()))
                .context("fi_endpoint")?;
            FabricError::from_ret(ffi::fi_ep_bind(
                ep,
                &mut (*cq).fid,
                (ffi::FI_TRANSMIT | ffi::FI_RECV) as u64,
            ))
            .context("fi_ep_bind(cq)")?;
            FabricError::from_ret(ffi::fi_ep_bind(ep, &mut (*av).fid, 0))
                .context("fi_ep_bind(av)")?;
            FabricError::from_ret(ffi::fi_enable(ep)).context("fi_enable")?;

            let _driver = CqDriver::spawn(cq)?;

            let buffer_len = max_read_size * CQ_SIZE;
            let buffer_ptr =
                std::alloc::alloc(std::alloc::Layout::from_size_align(buffer_len, 4096)?);
            let mut buffer_mr: *mut ffi::fid_mr = ptr::null_mut();
            FabricError::from_ret(ffi::fi_mr_reg(
                domain,
                buffer_ptr as *const _,
                buffer_len,
                ffi::FI_READ as u64,
                0,
                buffer_ptr as usize as u64,
                0,
                &mut buffer_mr,
                ptr::null_mut(),
            ))?;

            let buffer_q = crossbeam_queue::ArrayQueue::new(CQ_SIZE);
            for i in 0..CQ_SIZE {
                buffer_q.push(buffer_ptr.add(i * max_read_size)).unwrap();
            }

            Ok(FabricEndpoint {
                info,
                fabric,
                domain,
                cq,
                av,
                ep,
                max_read_size,
                buffer_mr,
                buffer_ptr,
                buffer_len,
                buffer_q,
                _driver,
            })
        }
    }

    /// Insert a remote endpoint address into the address vector.
    /// Returns the `fi_addr_t` handle to use in subsequent `read`/`write` calls.
    pub fn av_insert(&self, addr_bytes: &[u8]) -> Result<u64, FabricError> {
        let mut fi_addr: ffi::fi_addr_t = 0;
        let ret = unsafe {
            ffi::fi_av_insert(
                self.av,
                addr_bytes.as_ptr() as *const _,
                1,
                &mut fi_addr,
                0,
                ptr::null_mut(),
            )
        };
        if ret == 1 {
            Ok(fi_addr)
        } else {
            Err(FabricError::Unknown(ret.abs() as u32))
        }
    }

    pub fn mr_reg<'a>(
        &self,
        key: u64,
        buf: &'a [u8],
        access: u64,
    ) -> Result<FabricMrGuard<'a>, FabricError> {
        let mut mr: *mut ffi::fid_mr = ptr::null_mut();
        let mr_key = FabricError::from_ret(unsafe {
            ffi::fi_mr_reg(
                self.domain,
                buf.as_ptr() as *const _,
                buf.len(),
                access,
                0,
                key,
                0,
                &mut mr,
                ptr::null_mut(),
            )
        })
        .map(|_| unsafe { ffi::fi_mr_key(mr) })?;
        Ok(FabricMrGuard {
            mr,
            mr_key,
            _mr_buf: buf,
        })
    }

    /// Get the local endpoint address (opaque bytes for address exchange).
    pub fn get_ep_addr(&self) -> Result<Vec<u8>, FabricError> {
        unsafe {
            let mut buf = vec![0u8; 256];
            let mut len = buf.len();
            FabricError::from_ret(ffi::fi_getname(
                &mut (*self.ep).fid as *mut ffi::fid as ffi::fid_t,
                buf.as_mut_ptr() as *mut _,
                &mut len,
            ))
            .map(|_| {
                buf.truncate(len);
                buf
            })
        }
    }

    pub fn get_recv_buffer(&self) -> Result<FabricRecvBuffer<'_>, FabricError> {
        let buf = self
            .buffer_q
            .pop()
            .ok_or(FabricError::BufferPoolExhausted)?;
        Ok(FabricRecvBuffer { buf, ep: self })
    }

    fn put_recv_buffer(&self, buf: *mut u8) {
        self.buffer_q.push(buf).unwrap();
    }

    pub async fn bulk_read(
        &self,
        fi_addr: u64,
        mr_key: u64,
        remote_offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, FabricError> {
        let mut buf = Vec::with_capacity(len);
        let mut recv_buf = self.get_recv_buffer()?;

        let mut bytes_remaining = len;
        while bytes_remaining > 0 {
            let read_len = std::cmp::min(bytes_remaining, self.max_read_size);
            let bytes = self
                .read(
                    fi_addr,
                    mr_key,
                    remote_offset + (len - bytes_remaining) as u64,
                    read_len,
                    &mut recv_buf,
                )
                .await?;
            buf.extend_from_slice(bytes);
            bytes_remaining -= read_len;
        }

        Ok(buf)
    }

    pub async fn read<'a>(
        &self,
        fi_addr: u64,
        mr_key: u64,
        remote_offset: u64,
        len: usize,
        buf: &mut FabricRecvBuffer<'a>,
    ) -> Result<&'a [u8], FabricError> {
        self.raw_read(fi_addr, mr_key, remote_offset, buf, len).await?;
        Ok(unsafe { std::slice::from_raw_parts(buf.buf, len) })
    }

    pub async fn read_value<'a, T: Copy + 'a>(
        &self,
        fi_addr: u64,
        mr_key: u64,
        remote_offset: u64,
        buf: &mut FabricRecvBuffer<'a>,
    ) -> Result<&'a T, FabricError> {
        self.raw_read(fi_addr, mr_key, remote_offset, buf, size_of::<T>()).await?;
        unsafe { Ok(std::mem::transmute(buf.buf)) }
    }

    async fn raw_read(
        &self,
        fi_addr: u64,
        mr_key: u64,
        remote_offset: u64,
        buf: &mut FabricRecvBuffer<'_>,
        len: usize,
    ) -> Result<(), FabricError> {
        let future = ReadFuture { op: Op::new() };
        self._driver
            .tx
            .send(Request::Read {
                op: future.op.clone(),
                ep: self.ep as usize,
                buf_ptr: buf.buf as usize,
                len,
                fi_addr,
                remote_offset,
                mr_key,
            })
            .await
            .map_err(|_| FabricError::Unknown(0))?; // FIXME: better error handling

        future.await?;
        Ok(())
    }

    unsafe fn getinfo(version: u32, provider: &str) -> anyhow::Result<*mut ffi::fi_info> {
        unsafe {
            let hints = ffi::fi_allocinfo();
            assert!(!hints.is_null(), "fi_allocinfo failed");

            (*hints).caps = (ffi::FI_MSG | ffi::FI_RMA) as u64;
            (*(*hints).ep_attr).type_ = ffi::fi_ep_type_FI_EP_RDM;
            (*(*hints).domain_attr).mr_mode = 0;
            let prov_name = CString::new(provider)?;
            (*(*hints).fabric_attr).prov_name = prov_name.into_raw() as *mut i8;

            let mut info = ptr::null_mut();
            let ret = FabricError::from_ret(ffi::fi_getinfo(
                version,
                ptr::null(),
                ptr::null(),
                0,
                hints,
                &mut info,
            ));
            ffi::fi_freeinfo(hints);
            ret.context(format!("fi_getinfo({provider})"))?;
            Ok(info)
        }
    }
}

impl Drop for FabricEndpoint {
    fn drop(&mut self) {
        self._driver.stop();

        unsafe {
            ffi::fi_close(&mut (*self.buffer_mr).fid);
            std::alloc::dealloc(
                self.buffer_ptr,
                std::alloc::Layout::from_size_align(self.buffer_len, 4096).unwrap(),
            );
            ffi::fi_close(&mut (*self.ep).fid);
            ffi::fi_close(&mut (*self.av).fid);
            ffi::fi_close(&mut (*self.cq).fid);
            ffi::fi_close(&mut (*self.domain).fid);
            ffi::fi_close(&mut (*self.fabric).fid);
            ffi::fi_freeinfo(self.info);
        }
    }
}
