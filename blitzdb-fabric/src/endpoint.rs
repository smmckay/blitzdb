use std::ffi::CString;
use crate::driver::CqDriver;
use crate::op::{Op, ReadFuture};
use blitzdb_common::{FabricError};
use ofi_libfabric_sys::bindgen as ffi;
use std::ptr;
use anyhow::Context;

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
    _driver: CqDriver,
}

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

impl FabricEndpoint {
    pub fn new() -> anyhow::Result<Self> {
        unsafe {
            let hints = ffi::fi_allocinfo();
            assert!(!hints.is_null(), "fi_allocinfo failed");

            (*hints).caps = (ffi::FI_MSG | ffi::FI_RMA) as u64;
            (*(*hints).ep_attr).type_ = ffi::fi_ep_type_FI_EP_RDM;
            (*(*hints).domain_attr).mr_mode = 0;
            let prov_name = CString::new("tcp")?;
            (*(*hints).fabric_attr).prov_name = prov_name.into_raw() as *mut i8;

            let mut info = ptr::null_mut();
            let version = ffi::fi_version();
            FabricError::from_ret(ffi::fi_getinfo(version, ptr::null(), ptr::null(), 0, hints, &mut info)).context("fi_getinfo")?;
            ffi::fi_freeinfo(hints);

            let mut fabric = ptr::null_mut();
            FabricError::from_ret(ffi::fi_fabric((*info).fabric_attr, &mut fabric, ptr::null_mut())).context("fi_fabric")?;

            let mut domain = ptr::null_mut();
            FabricError::from_ret(ffi::fi_domain(fabric, info, &mut domain, ptr::null_mut())).context("fi_domain")?;

            let mut cq_attr: ffi::fi_cq_attr = std::mem::zeroed();
            cq_attr.format = ffi::fi_cq_format_FI_CQ_FORMAT_CONTEXT;
            cq_attr.size = 64;
            let mut cq = ptr::null_mut();
            FabricError::from_ret(ffi::fi_cq_open(domain, &mut cq_attr, &mut cq, ptr::null_mut())).context("fi_cq_open")?;

            let mut av_attr: ffi::fi_av_attr = std::mem::zeroed();
            av_attr.type_ = ffi::fi_av_type_FI_AV_TABLE;
            let mut av = ptr::null_mut();
            FabricError::from_ret(ffi::fi_av_open(domain, &mut av_attr, &mut av, ptr::null_mut())).context("fi_av_open")?;

            let mut ep = ptr::null_mut();
            FabricError::from_ret(ffi::fi_endpoint(domain, info, &mut ep, ptr::null_mut())).context("fi_endpoint")?;
            FabricError::from_ret(ffi::fi_ep_bind(ep, &mut (*cq).fid, (ffi::FI_TRANSMIT | ffi::FI_RECV) as u64)).context("fi_ep_bind(cq)")?;
            FabricError::from_ret(ffi::fi_ep_bind(ep, &mut (*av).fid, 0)).context("fi_ep_bind(av)")?;
            FabricError::from_ret(ffi::fi_enable(ep)).context("fi_enable")?;

            let _driver = CqDriver::spawn(cq)?;

            Ok(FabricEndpoint {
                info,
                fabric,
                domain,
                cq,
                av,
                ep,
                _driver
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
        if ret == 1 { Ok(fi_addr) } else { Err(FabricError::Unknown(ret.abs() as u32)) }
    }

    pub fn mr_reg<'a>(&self, key: u64, buf: &'a [u8], access: u64) -> Result<FabricMrGuard<'a>, FabricError> {
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
        }).map(|_| unsafe { ffi::fi_mr_key(mr) })?;
        Ok(FabricMrGuard { mr, mr_key, _mr_buf: buf })
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
            )).map(|_| {buf.truncate(len); buf})
        }
    }

    /// Issue a one-sided RDMA read.
    ///
    /// Returns a `ReadFuture` that resolves to an owned `Vec<u8>` containing
    /// `len` bytes read from `remote_offset` in the remote MR identified by `mr_key`.
    pub fn read(
        &self,
        fi_addr: u64,
        mr_key: u64,
        remote_offset: u64,
        len: usize,
    ) -> ReadFuture {
        ReadFuture {
            op: Op::new(),
            buf: vec![0u8; len],
            ep: self.ep as usize,
            cq: self.cq as usize,
            fi_addr,
            remote_offset,
            mr_key,
            issued: false,
        }
    }
}

impl Drop for FabricEndpoint {
    fn drop(&mut self) {
        self._driver.stop();

        unsafe {
            if !self.ep.is_null() {
                ffi::fi_close(&mut (*self.ep).fid);
            }
            if !self.av.is_null() {
                ffi::fi_close(&mut (*self.av).fid);
            }
            if !self.cq.is_null() {
                ffi::fi_close(&mut (*self.cq).fid);
            }
            if !self.domain.is_null() {
                ffi::fi_close(&mut (*self.domain).fid);
            }
            if !self.fabric.is_null() {
                ffi::fi_close(&mut (*self.fabric).fid);
            }
            if !self.info.is_null() {
                ffi::fi_freeinfo(self.info);
            }
        }
    }
}