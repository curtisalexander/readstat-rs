use log::debug;
use std::os::raw::{c_char, c_long, c_void};

use crate::err::{check_c_error, ReadStatError};

pub struct ReadStatParser {
    parser: *mut readstat_sys::readstat_parser_t,
}

impl ReadStatParser {
    pub fn new() -> Self {
        let parser: *mut readstat_sys::readstat_parser_t =
            unsafe { readstat_sys::readstat_parser_init() };

        Self { parser }
    }

    pub fn set_metadata_handler(
        self,
        metadata_handler: readstat_sys::readstat_metadata_handler,
    ) -> Result<Self, ReadStatError> {
        let set_metadata_handler_error =
            unsafe { readstat_sys::readstat_set_metadata_handler(self.parser, metadata_handler) };

        debug!(
            "After setting metadata handler, error ==> {}",
            &set_metadata_handler_error
        );

        check_c_error(set_metadata_handler_error as i32)?;
        Ok(self)
    }

    pub fn set_row_limit(
        self,
        row_limit: Option<u32>,
    ) -> Result<Self, ReadStatError> {
        match row_limit {
            Some(r) => {
                let set_row_limit_error =
                    unsafe { readstat_sys::readstat_set_row_limit(self.parser, r as c_long) };

                debug!(
                    "After setting row limit, error ==> {}",
                    &set_row_limit_error
                );

                check_c_error(set_row_limit_error as i32)?;
                Ok(self)
            }
            None => Ok(self),
        }
    }

    pub fn set_row_offset(
        self,
        row_offset: Option<u32>,
    ) -> Result<Self, ReadStatError> {
        match row_offset {
            Some(r) => {
                let set_row_offset_error =
                    unsafe { readstat_sys::readstat_set_row_offset(self.parser, r as c_long) };

                debug!(
                    "After setting row offset, error ==> {}",
                    &set_row_offset_error
                );

                check_c_error(set_row_offset_error as i32)?;
                Ok(self)
            }
            None => Ok(self),
        }
    }

    pub fn set_variable_handler(
        self,
        variable_handler: readstat_sys::readstat_variable_handler,
    ) -> Result<Self, ReadStatError> {
        let set_variable_handler_error =
            unsafe { readstat_sys::readstat_set_variable_handler(self.parser, variable_handler) };

        debug!(
            "After setting variable handler, error ==> {}",
            &set_variable_handler_error
        );

        check_c_error(set_variable_handler_error as i32)?;
        Ok(self)
    }

    pub fn set_value_handler(
        self,
        value_handler: readstat_sys::readstat_value_handler,
    ) -> Result<Self, ReadStatError> {
        let set_value_handler_error =
            unsafe { readstat_sys::readstat_set_value_handler(self.parser, value_handler) };

        debug!(
            "After setting value handler, error ==> {}",
            &set_value_handler_error
        );

        check_c_error(set_value_handler_error as i32)?;
        Ok(self)
    }

    pub fn parse_sas7bdat(
        &mut self,
        path: *const c_char,
        user_ctx: *mut c_void,
    ) -> readstat_sys::readstat_error_t {
        let parse_sas7bdat_error: readstat_sys::readstat_error_t =
            unsafe { readstat_sys::readstat_parse_sas7bdat(self.parser, path, user_ctx) };

        debug!(
            "After calling parse sas7bdat, error ==> {}",
            &parse_sas7bdat_error
        );

        parse_sas7bdat_error
    }
}

impl Drop for ReadStatParser {
    fn drop(&mut self) {
        debug!("Freeing parser");

        unsafe { readstat_sys::readstat_parser_free(self.parser) };
    }
}
