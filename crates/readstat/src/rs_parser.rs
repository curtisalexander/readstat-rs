//! Safe wrapper around the ReadStat C parser.
//!
//! [`ReadStatParser`] provides a builder-pattern API for configuring and invoking the
//! ReadStat C library's `readstat_parser_t`. It manages the parser lifecycle (init/free)
//! via RAII and exposes methods for setting callback handlers, row limits/offsets,
//! and triggering the actual `.sas7bdat` parse.

use log::debug;
use std::os::raw::{c_char, c_long, c_void};

use crate::err::{ReadStatError, check_c_error};

/// Safe RAII wrapper around the ReadStat C parser (`readstat_parser_t`).
///
/// Provides a builder-pattern API for configuring callbacks, row limits/offsets,
/// and invoking the parse. The underlying C parser is freed on drop.
pub(crate) struct ReadStatParser {
    parser: *mut readstat_sys::readstat_parser_t,
}

impl ReadStatParser {
    /// Allocates and initializes a new ReadStat C parser.
    pub(crate) fn new() -> Self {
        let parser: *mut readstat_sys::readstat_parser_t =
            unsafe { readstat_sys::readstat_parser_init() };

        Self { parser }
    }

    /// Registers the callback invoked when file-level metadata is parsed.
    pub(crate) fn set_metadata_handler(
        self,
        metadata_handler: readstat_sys::readstat_metadata_handler,
    ) -> Result<Self, ReadStatError> {
        let set_metadata_handler_error =
            unsafe { readstat_sys::readstat_set_metadata_handler(self.parser, metadata_handler) };

        debug!("After setting metadata handler, error ==> {set_metadata_handler_error}");

        check_c_error(set_metadata_handler_error as i32)?;
        Ok(self)
    }

    /// Sets the maximum number of rows to read. `None` means no limit.
    pub(crate) fn set_row_limit(self, row_limit: Option<u32>) -> Result<Self, ReadStatError> {
        match row_limit {
            Some(r) => {
                let set_row_limit_error =
                    unsafe { readstat_sys::readstat_set_row_limit(self.parser, r as c_long) };

                debug!("After setting row limit, error ==> {set_row_limit_error}");

                check_c_error(set_row_limit_error as i32)?;
                Ok(self)
            }
            None => Ok(self),
        }
    }

    /// Sets the starting row offset for reading. `None` means start from row 0.
    pub(crate) fn set_row_offset(self, row_offset: Option<u32>) -> Result<Self, ReadStatError> {
        match row_offset {
            Some(r) => {
                let set_row_offset_error =
                    unsafe { readstat_sys::readstat_set_row_offset(self.parser, r as c_long) };

                debug!("After setting row offset, error ==> {set_row_offset_error}");

                check_c_error(set_row_offset_error as i32)?;
                Ok(self)
            }
            None => Ok(self),
        }
    }

    /// Registers the callback invoked for each variable (column) definition.
    pub(crate) fn set_variable_handler(
        self,
        variable_handler: readstat_sys::readstat_variable_handler,
    ) -> Result<Self, ReadStatError> {
        let set_variable_handler_error =
            unsafe { readstat_sys::readstat_set_variable_handler(self.parser, variable_handler) };

        debug!("After setting variable handler, error ==> {set_variable_handler_error}");

        check_c_error(set_variable_handler_error as i32)?;
        Ok(self)
    }

    /// Registers the callback invoked for each cell value during row parsing.
    pub(crate) fn set_value_handler(
        self,
        value_handler: readstat_sys::readstat_value_handler,
    ) -> Result<Self, ReadStatError> {
        let set_value_handler_error =
            unsafe { readstat_sys::readstat_set_value_handler(self.parser, value_handler) };

        debug!("After setting value handler, error ==> {set_value_handler_error}");

        check_c_error(set_value_handler_error as i32)?;
        Ok(self)
    }

    /// Registers a custom handler for opening the data source.
    pub(crate) fn set_open_handler(
        self,
        open_handler: readstat_sys::readstat_open_handler,
    ) -> Result<Self, ReadStatError> {
        let error = unsafe { readstat_sys::readstat_set_open_handler(self.parser, open_handler) };
        debug!("After setting open handler, error ==> {error}");
        check_c_error(error as i32)?;
        Ok(self)
    }

    /// Registers a custom handler for closing the data source.
    pub(crate) fn set_close_handler(
        self,
        close_handler: readstat_sys::readstat_close_handler,
    ) -> Result<Self, ReadStatError> {
        let error = unsafe { readstat_sys::readstat_set_close_handler(self.parser, close_handler) };
        debug!("After setting close handler, error ==> {error}");
        check_c_error(error as i32)?;
        Ok(self)
    }

    /// Registers a custom handler for seeking within the data source.
    pub(crate) fn set_seek_handler(
        self,
        seek_handler: readstat_sys::readstat_seek_handler,
    ) -> Result<Self, ReadStatError> {
        let error = unsafe { readstat_sys::readstat_set_seek_handler(self.parser, seek_handler) };
        debug!("After setting seek handler, error ==> {error}");
        check_c_error(error as i32)?;
        Ok(self)
    }

    /// Registers a custom handler for reading from the data source.
    pub(crate) fn set_read_handler(
        self,
        read_handler: readstat_sys::readstat_read_handler,
    ) -> Result<Self, ReadStatError> {
        let error = unsafe { readstat_sys::readstat_set_read_handler(self.parser, read_handler) };
        debug!("After setting read handler, error ==> {error}");
        check_c_error(error as i32)?;
        Ok(self)
    }

    /// Registers a custom handler for progress updates.
    pub(crate) fn set_update_handler(
        self,
        update_handler: readstat_sys::readstat_update_handler,
    ) -> Result<Self, ReadStatError> {
        let error =
            unsafe { readstat_sys::readstat_set_update_handler(self.parser, update_handler) };
        debug!("After setting update handler, error ==> {error}");
        check_c_error(error as i32)?;
        Ok(self)
    }

    /// Sets a custom I/O context pointer passed to all I/O handler callbacks.
    pub(crate) fn set_io_ctx(self, io_ctx: *mut c_void) -> Result<Self, ReadStatError> {
        let error = unsafe { readstat_sys::readstat_set_io_ctx(self.parser, io_ctx) };
        debug!("After setting io ctx, error ==> {error}");
        check_c_error(error as i32)?;
        Ok(self)
    }

    /// Parses a `.sas7bdat` file, invoking registered callbacks as data is read.
    ///
    /// Returns the raw ReadStat error code. Use [`check_c_error`](crate::err::check_c_error)
    /// to convert to a `Result`.
    pub(crate) fn parse_sas7bdat(
        &mut self,
        path: *const c_char,
        user_ctx: *mut c_void,
    ) -> readstat_sys::readstat_error_t {
        let parse_sas7bdat_error: readstat_sys::readstat_error_t =
            unsafe { readstat_sys::readstat_parse_sas7bdat(self.parser, path, user_ctx) };

        debug!("After calling parse sas7bdat, error ==> {parse_sas7bdat_error}");

        parse_sas7bdat_error
    }
}

impl Drop for ReadStatParser {
    fn drop(&mut self) {
        debug!("Freeing parser");

        unsafe { readstat_sys::readstat_parser_free(self.parser) };
    }
}
