use path_abs::{PathAbs, PathInfo};
use readstat;
use readstat_sys;
use std::env;


#[test]
fn get_row_count() {
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.parent().unwrap().join("data");
    let sas_path = data_dir.join("cars.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    let mut d = readstat::ReadStatData::new(rsp);
    let error = d.get_metadata().unwrap();

    assert_eq!(error, readstat_sys::readstat_error_e_READSTAT_OK as u32);

    assert_eq!(d.row_count, 1081);
}

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

#[test]
fn get_var_names() {
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.parent().unwrap().join("data");
    let sas_path = data_dir.join("cars.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    let mut d = readstat::ReadStatData::new(rsp);
    let error = d.get_metadata().unwrap();

    assert_eq!(error, readstat_sys::readstat_error_e_READSTAT_OK as u32);

    let vars = d.vars;

    let contains_brand_key = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(0 as std::os::raw::c_int, String::from("Brand")));

    let contains_brand_key_wrong_index = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(1 as std::os::raw::c_int, String::from("Brand")));

    let brand_type = &vars.get(&readstat::ReadStatVarIndexAndName::new(0 as std::os::raw::c_int, String::from("Brand"))).unwrap().var_type;

    assert_eq!(contains_brand_key, true);
    assert_eq!(contains_brand_key_wrong_index, false);
    assert!(matches!(brand_type, readstat::ReadStatVarType::String));

}