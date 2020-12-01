use path_abs::{PathAbs, PathInfo};
use readstat;
use readstat_sys;
use std::env;

#[test]
fn get_var_count() {
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.parent().unwrap().join("data");
    let sas_path = data_dir.join("cars.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    let mut d = readstat::ReadStatData::new(rsp);
    let error = d.get_metadata().unwrap();

    assert_eq!(error, readstat_sys::readstat_error_e_READSTAT_OK as u32);

    assert_eq!(d.var_count, 13);
}