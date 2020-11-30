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
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    let mut d = readstat::ReadStatData::new(rsp);
    let error = d.get_metadata().unwrap();

    assert_eq!(error, readstat_sys::readstat_error_e_READSTAT_OK as u32);

    let vars = d.vars;

    let contains_brand_key = vars.contains_key(&readstat::ReadStatVarMetadata::new(0 as std::os::raw::c_int, String::from("Brand")));

    let contains_brand_key_wrong_index = vars.contains_key(&readstat::ReadStatVarMetadata::new(1 as std::os::raw::c_int, String::from("Brand")));

    let brand_type = vars.get(&readstat::ReadStatVarMetadata::new(0 as std::os::raw::c_int, String::from("Brand"))).unwrap();

    assert_eq!(contains_brand_key, true);
    assert_eq!(contains_brand_key_wrong_index, false);
    assert!(matches!(*brand_type, readstat::ReadStatVarType::String));

}