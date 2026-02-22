//! Buffer-based I/O handlers for parsing SAS files from in-memory byte slices.
//!
//! Provides [`ReadStatBufferCtx`] and a set of `extern "C"` callback functions that
//! implement the ReadStat I/O interface over a `&[u8]` buffer instead of a file.
//! This enables parsing `.sas7bdat` data without filesystem access — useful for
//! WASM targets, cloud storage, HTTP uploads, and testing.

use std::os::raw::{c_char, c_int, c_long, c_void};
use std::ptr;

use crate::err::ReadStatError;
use crate::rs_parser::ReadStatParser;

/// In-memory buffer context for ReadStat I/O callbacks.
///
/// Wraps a borrowed byte slice and tracks the current read position.
/// Passed as the `io_ctx` pointer to all I/O handler callbacks.
#[repr(C)]
pub struct ReadStatBufferCtx {
    data: *const u8,
    len: usize,
    pos: usize,
}

impl ReadStatBufferCtx {
    /// Creates a new buffer context from a byte slice.
    ///
    /// The caller must ensure the byte slice outlives the context and any
    /// parsing operations that use it.
    pub fn new(bytes: &[u8]) -> Self {
        Self {
            data: bytes.as_ptr(),
            len: bytes.len(),
            pos: 0,
        }
    }

    /// Configures a [`ReadStatParser`] to read from this buffer context
    /// instead of from a file.
    pub fn configure_parser(
        &mut self,
        parser: ReadStatParser,
    ) -> Result<ReadStatParser, ReadStatError> {
        let ctx_ptr = self as *mut ReadStatBufferCtx as *mut c_void;
        parser
            .set_open_handler(Some(buffer_open))
            .and_then(|p| p.set_close_handler(Some(buffer_close)))
            .and_then(|p| p.set_seek_handler(Some(buffer_seek)))
            .and_then(|p| p.set_read_handler(Some(buffer_read)))
            .and_then(|p| p.set_update_handler(Some(buffer_update)))
            .and_then(|p| p.set_io_ctx(ctx_ptr))
    }
}

/// No-op open handler — the buffer is already "open".
unsafe extern "C" fn buffer_open(_path: *const c_char, _io_ctx: *mut c_void) -> c_int {
    0
}

/// No-op close handler — nothing to close for an in-memory buffer.
unsafe extern "C" fn buffer_close(_io_ctx: *mut c_void) -> c_int {
    0
}

/// Seek handler that repositions the read cursor within the buffer.
unsafe extern "C" fn buffer_seek(
    offset: readstat_sys::readstat_off_t,
    whence: readstat_sys::readstat_io_flags_t,
    io_ctx: *mut c_void,
) -> readstat_sys::readstat_off_t {
    let ctx = unsafe { &mut *(io_ctx as *mut ReadStatBufferCtx) };

    let newpos: i64 = match whence {
        readstat_sys::readstat_io_flags_e_READSTAT_SEEK_SET => offset,
        readstat_sys::readstat_io_flags_e_READSTAT_SEEK_CUR => ctx.pos as i64 + offset,
        readstat_sys::readstat_io_flags_e_READSTAT_SEEK_END => ctx.len as i64 + offset,
        _ => return -1,
    };

    if newpos < 0 || newpos > ctx.len as i64 {
        return -1;
    }

    ctx.pos = newpos as usize;
    newpos
}

/// Read handler that copies bytes from the buffer into the caller's buffer.
unsafe extern "C" fn buffer_read(
    buf: *mut c_void,
    nbytes: usize,
    io_ctx: *mut c_void,
) -> isize {
    let ctx = unsafe { &mut *(io_ctx as *mut ReadStatBufferCtx) };
    let bytes_left = ctx.len.saturating_sub(ctx.pos);

    let to_copy = if nbytes <= bytes_left {
        nbytes
    } else if bytes_left > 0 {
        bytes_left
    } else {
        return 0;
    };

    unsafe {
        ptr::copy_nonoverlapping(ctx.data.add(ctx.pos), buf as *mut u8, to_copy);
    }
    ctx.pos += to_copy;
    to_copy as isize
}

/// Update/progress handler for buffer I/O.
unsafe extern "C" fn buffer_update(
    _file_size: c_long,
    progress_handler: readstat_sys::readstat_progress_handler,
    user_ctx: *mut c_void,
    io_ctx: *mut c_void,
) -> readstat_sys::readstat_error_t {
    let Some(handler) = progress_handler else {
        return readstat_sys::readstat_error_e_READSTAT_OK;
    };

    let ctx = unsafe { &*(io_ctx as *mut ReadStatBufferCtx) };
    let progress = if ctx.len > 0 {
        ctx.pos as f64 / ctx.len as f64
    } else {
        1.0
    };

    if unsafe { handler(progress, user_ctx) } != 0 {
        return readstat_sys::readstat_error_e_READSTAT_ERROR_USER_ABORT;
    }

    readstat_sys::readstat_error_e_READSTAT_OK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_ctx_new() {
        let data = vec![1u8, 2, 3, 4, 5];
        let ctx = ReadStatBufferCtx::new(&data);
        assert_eq!(ctx.len, 5);
        assert_eq!(ctx.pos, 0);
        assert_eq!(ctx.data, data.as_ptr());
    }

    #[test]
    fn buffer_ctx_empty() {
        let data: Vec<u8> = vec![];
        let ctx = ReadStatBufferCtx::new(&data);
        assert_eq!(ctx.len, 0);
        assert_eq!(ctx.pos, 0);
    }
}
