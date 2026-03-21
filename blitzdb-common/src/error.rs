use std::ffi::c_int;
use std::fmt;

/// Error type wrapping libfabric errno values.
///
/// Libfabric returns negative errno on failure (e.g. `-FI_EAGAIN`).
/// Use [`FabricError::from_ret`] to convert a raw return value into a `Result`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FabricError {
    // ── POSIX errors commonly returned by libfabric ──────────────────────────
    /// Resource temporarily unavailable; caller should retry.
    Again,
    /// Operation already in progress.
    Already,
    /// Bad address.
    Fault,
    /// Invalid argument.
    Invalid,
    /// Out of memory.
    NoMem,
    /// Permission denied.
    Access,
    /// Connection reset by peer.
    ConnReset,
    /// Connection refused.
    ConnRefused,
    /// Connection timed out.
    TimedOut,
    /// Network is unreachable.
    NetUnreach,
    /// Host is unreachable.
    HostUnreach,
    /// Operation not supported.
    OpNotSupp,
    /// No data available.
    NoData,
    /// No message of desired type.
    NoMsg,
    /// Operation was cancelled.
    Cancelled,
    /// I/O error.
    Io,

    // ── Libfabric-specific errors (code >= 256) ───────────────────────────────
    /// Unspecified error.
    Other,
    /// Provided buffer is too small.
    TooSmall,
    /// Operation not permitted in current state.
    BadState,
    /// Error entry is available on the CQ; call `fi_cq_readerr`.
    ErrorAvail,
    /// Flags not supported.
    BadFlags,
    /// Missing or unavailable event queue.
    NoEq,
    /// Invalid resource domain.
    BadDomain,
    /// Missing or unavailable completion queue.
    NoCq,
    /// CRC error.
    Crc,
    /// Message was truncated.
    Truncated,
    /// Required memory registration key not available.
    NoKey,
    /// Missing or unavailable address vector.
    NoAv,
    /// Completion queue overrun.
    Overrun,
    /// No receive buffers available.
    NoRx,
    /// No more memory registrations available.
    NoMr,
    /// Remote host is behind a firewall.
    FirewallAddr,

    /// Any other errno not covered above.
    Unknown(u32),
}

impl FabricError {
    /// Convert a raw libfabric return value to `Result<(), FabricError>`.
    ///
    /// Libfabric returns `0` on success and a **negative** errno on failure.
    pub fn from_ret(ret: c_int) -> crate::Result<c_int> {
        if ret >= 0 {
            Ok(ret)
        } else {
            Err(Self::from_errno(ret.abs() as u32))
        }
    }

    /// Convert a **positive** errno value to a `FabricError`.
    pub fn from_errno(errno: u32) -> Self {
        match errno {
            crate::ffi::FI_EAGAIN      => Self::Again,
            crate::ffi::FI_EALREADY    => Self::Already,
            crate::ffi::FI_EFAULT      => Self::Fault,
            crate::ffi::FI_EINVAL      => Self::Invalid,
            crate::ffi::FI_ENOMEM      => Self::NoMem,
            crate::ffi::FI_EACCES      => Self::Access,
            crate::ffi::FI_ECONNRESET  => Self::ConnReset,
            crate::ffi::FI_ECONNREFUSED => Self::ConnRefused,
            crate::ffi::FI_ETIMEDOUT   => Self::TimedOut,
            crate::ffi::FI_ENETUNREACH => Self::NetUnreach,
            crate::ffi::FI_EHOSTUNREACH => Self::HostUnreach,
            crate::ffi::FI_EOPNOTSUPP  => Self::OpNotSupp,
            crate::ffi::FI_ENODATA     => Self::NoData,
            crate::ffi::FI_ENOMSG      => Self::NoMsg,
            crate::ffi::FI_ECANCELED   => Self::Cancelled,
            crate::ffi::FI_EIO         => Self::Io,
            crate::ffi::FI_EOTHER      => Self::Other,
            crate::ffi::FI_ETOOSMALL   => Self::TooSmall,
            crate::ffi::FI_EOPBADSTATE => Self::BadState,
            crate::ffi::FI_EAVAIL      => Self::ErrorAvail,
            crate::ffi::FI_EBADFLAGS   => Self::BadFlags,
            crate::ffi::FI_ENOEQ       => Self::NoEq,
            crate::ffi::FI_EDOMAIN     => Self::BadDomain,
            crate::ffi::FI_ENOCQ       => Self::NoCq,
            crate::ffi::FI_ECRC        => Self::Crc,
            crate::ffi::FI_ETRUNC      => Self::Truncated,
            crate::ffi::FI_ENOKEY      => Self::NoKey,
            crate::ffi::FI_ENOAV       => Self::NoAv,
            crate::ffi::FI_EOVERRUN    => Self::Overrun,
            crate::ffi::FI_ENORX       => Self::NoRx,
            crate::ffi::FI_ENOMR       => Self::NoMr,
            crate::ffi::FI_EFIREWALLADDR => Self::FirewallAddr,
            other                                 => Self::Unknown(other),
        }
    }
}

impl fmt::Display for FabricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Again         => write!(f, "resource temporarily unavailable (EAGAIN)"),
            Self::Already       => write!(f, "operation already in progress (EALREADY)"),
            Self::Fault         => write!(f, "bad address (EFAULT)"),
            Self::Invalid       => write!(f, "invalid argument (EINVAL)"),
            Self::NoMem         => write!(f, "out of memory (ENOMEM)"),
            Self::Access        => write!(f, "permission denied (EACCES)"),
            Self::ConnReset     => write!(f, "connection reset by peer (ECONNRESET)"),
            Self::ConnRefused   => write!(f, "connection refused (ECONNREFUSED)"),
            Self::TimedOut      => write!(f, "connection timed out (ETIMEDOUT)"),
            Self::NetUnreach    => write!(f, "network unreachable (ENETUNREACH)"),
            Self::HostUnreach   => write!(f, "host unreachable (EHOSTUNREACH)"),
            Self::OpNotSupp     => write!(f, "operation not supported (EOPNOTSUPP)"),
            Self::NoData        => write!(f, "no data available (ENODATA)"),
            Self::NoMsg         => write!(f, "no message of desired type (ENOMSG)"),
            Self::Cancelled     => write!(f, "operation cancelled (ECANCELED)"),
            Self::Io            => write!(f, "I/O error (EIO)"),
            Self::Other         => write!(f, "unspecified libfabric error (FI_EOTHER)"),
            Self::TooSmall      => write!(f, "buffer too small (FI_ETOOSMALL)"),
            Self::BadState      => write!(f, "operation not permitted in current state (FI_EOPBADSTATE)"),
            Self::ErrorAvail    => write!(f, "error entry available on CQ (FI_EAVAIL)"),
            Self::BadFlags      => write!(f, "unsupported flags (FI_EBADFLAGS)"),
            Self::NoEq          => write!(f, "missing or unavailable event queue (FI_ENOEQ)"),
            Self::BadDomain     => write!(f, "invalid resource domain (FI_EDOMAIN)"),
            Self::NoCq          => write!(f, "missing or unavailable completion queue (FI_ENOCQ)"),
            Self::Crc           => write!(f, "CRC error (FI_ECRC)"),
            Self::Truncated     => write!(f, "message truncated (FI_ETRUNC)"),
            Self::NoKey         => write!(f, "required MR key not available (FI_ENOKEY)"),
            Self::NoAv          => write!(f, "missing or unavailable address vector (FI_ENOAV)"),
            Self::Overrun       => write!(f, "completion queue overrun (FI_EOVERRUN)"),
            Self::NoRx          => write!(f, "no receive buffers available (FI_ENORX)"),
            Self::NoMr          => write!(f, "no memory registrations available (FI_ENOMR)"),
            Self::FirewallAddr  => write!(f, "host unreachable behind firewall (FI_EFIREWALLADDR)"),
            Self::Unknown(e)    => write!(f, "unknown libfabric error: {e}"),
        }
    }
}

impl std::error::Error for FabricError {}