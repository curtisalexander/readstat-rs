use arrow::array::{ArrayRef, StringArray};
use path_abs::{PathAbs, PathInfo};
use readstat;
use std::env;

#[test]
fn parse_file_with_missing_data() {
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.as_path().join("tests").join("data");
    let sas_path = data_dir.join("hasmissing.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    let mut d = readstat::ReadStatData::new(rsp).set_reader(readstat::Reader::mem);
    let error = d.get_data(Some(5)).unwrap();

    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    let vars = d.vars;
    let contains_id_key = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(
        0 as std::os::raw::c_int,
        String::from("ID"),
    ));
    assert!(contains_id_key);

    let id_type = &vars
        .get(&readstat::ReadStatVarIndexAndName::new(
            0 as std::os::raw::c_int,
            String::from("ID"),
        ))
        .unwrap().var_type;
    assert!(matches!(id_type, readstat::ReadStatVarType::String));

    let var_count = d.var_count;
    assert_eq!(var_count, 9);

    let row_count = d.row_count;
    assert_eq!(row_count, 5);

    // column = 5 (index 4) -> row = 2 (index 1)
    let array_refs: Vec<ArrayRef> = d
        .cols
        .iter_mut()
        .map(|builder| builder.finish())
        .collect();
    let col_with_non_missing = array_refs[4].as_any().downcast_ref::<StringArray>().unwrap();
    let non_missing_value = col_with_non_missing.value(1);
    // let row_with_missing = &d.rows[1];
    //let non_missing_value = if let readstat::ReadStatVar::ReadStat_String(s) = &row_with_missing[0] { s.to_owned() } else { String::from("") };
    assert_eq!(non_missing_value, String::from("00102"));
    /*
    let missing_value = if let readstat::ReadStatVar::ReadStat_Missing(m) = &row_with_missing[4] {  *m } else { panic!("Row 2, var 4 value is not ()")  };
    assert_eq!(missing_value, ());
    */
}
