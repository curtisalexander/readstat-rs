use dunce;
use readstat;
use readstat_sys;
use std::env;
use std::path::Path;

#[test]
fn get_var_names() {
    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let data_dir = project_dir.parent().unwrap().join("data");
    let sas_path = dunce::canonicalize(data_dir.join("cars.sas7bdat")).unwrap();

    let mut md = readstat::ReadStatMetadata::new().set_path(sas_path);
    let error = md.get_metadata().unwrap();

    assert_eq!(error, readstat_sys::readstat_error_e_READSTAT_OK);

    let vars = md.vars;

    let contains_brand_key = vars.contains_key(&readstat::ReadStatVar::new(0 as std::os::raw::c_int, String::from("Brand")));

    let contains_brand_key_wrong_index = vars.contains_key(&readstat::ReadStatVar::new(1 as std::os::raw::c_int, String::from("Brand")));

    let brand_type = vars.get(&readstat::ReadStatVar::new(0 as std::os::raw::c_int, String::from("Brand"))).unwrap();

    assert_eq!(contains_brand_key, true);
    assert_eq!(contains_brand_key_wrong_index, false);
    assert_eq!(*brand_type, 0 as readstat_sys::readstat_type_t);

}