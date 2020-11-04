use dunce;
use readstat;
use readstat_sys;
use std::env;
use std::path::Path;

#[test]
fn get_row_count() {
    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let data_dir = project_dir.parent().unwrap().join("data");
    let sas_path = dunce::canonicalize(data_dir.join("cars.sas7bdat")).unwrap();

    let sas_path_cstring = readstat::path_to_cstring(&sas_path).unwrap();
    let psas_path_cstring = sas_path_cstring.as_ptr();

    let mut my_count = 0;
    let pmy_count = &mut my_count as *mut i32;
    let pmy_count_void = pmy_count as *mut std::os::raw::c_void;

    let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
    assert_eq!(error, readstat_sys::readstat_error_e_READSTAT_OK);

    let parser: *mut readstat_sys::readstat_parser_t = unsafe { readstat_sys::readstat_parser_init() };

    let set_metadata_handler_error = unsafe { readstat_sys::readstat_set_metadata_handler(parser, Some(readstat::handle_metadata)) };
    assert_eq!(set_metadata_handler_error, readstat_sys::readstat_error_e_READSTAT_OK);

    let error: readstat_sys::readstat_error_t = unsafe { readstat_sys::readstat_parse_sas7bdat(parser, psas_path_cstring, pmy_count_void) };
    assert_eq!(error, readstat_sys::readstat_error_e_READSTAT_OK);

    unsafe { readstat_sys::readstat_parser_free(parser) };

    let record_count = unsafe { *pmy_count };
    assert_eq!(record_count, 1081);
}