use arrow::datatypes::DataType;
use arrow_array::{Array, Float64Array, StringArray};
use readstat::{ReadStatData, ReadStatMetadata};

mod common;

/// Helper: read cars.sas7bdat with an optional column filter applied.
fn read_cars_with_columns(
    col_names: Option<Vec<String>>,
) -> Result<(ReadStatMetadata, ReadStatData), readstat::ReadStatError> {
    let rsp = common::setup_path("cars.sas7bdat")?;
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false)?;

    let column_filter = md.resolve_selected_columns(col_names)?;
    let original_var_count = md.var_count;
    if let Some(ref mapping) = column_filter {
        md = md.filter_to_selected_columns(mapping);
    }

    let mut d = ReadStatData::new()
        .set_column_filter(column_filter, original_var_count)
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);

    d.read_data(&rsp)?;
    Ok((md, d))
}

#[test]
fn select_subset_of_columns() {
    let (md, d) = read_cars_with_columns(Some(vec![
        "Brand".to_string(),
        "Model".to_string(),
        "EngineSize".to_string(),
    ]))
    .unwrap();

    // Should have exactly 3 columns
    assert_eq!(md.var_count, 3);
    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_columns(), 3);

    // Column names should match (in original dataset order)
    assert_eq!(batch.schema().field(0).name(), "Brand");
    assert_eq!(batch.schema().field(1).name(), "Model");
    assert_eq!(batch.schema().field(2).name(), "EngineSize");

    // Data types should be correct
    assert!(matches!(batch.schema().field(0).data_type(), DataType::Utf8));
    assert!(matches!(batch.schema().field(1).data_type(), DataType::Utf8));
    assert!(matches!(
        batch.schema().field(2).data_type(),
        DataType::Float64
    ));

    // Row count should be full dataset
    assert_eq!(batch.num_rows(), 1081);
}

#[test]
fn select_single_column() {
    let (md, d) =
        read_cars_with_columns(Some(vec!["CityMPG".to_string()])).unwrap();

    assert_eq!(md.var_count, 1);
    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "CityMPG");
    assert_eq!(batch.num_rows(), 1081);

    // Verify data is present
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(col.len() > 0);
}

#[test]
fn invalid_column_name_returns_error() {
    let rsp = common::setup_path("cars.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    let result = md.resolve_selected_columns(Some(vec![
        "Brand".to_string(),
        "NonExistentColumn".to_string(),
    ]));

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_msg = format!("{}", err);
    assert!(err_msg.contains("NonExistentColumn"));
    assert!(err_msg.contains("Brand")); // available columns should include Brand
}

#[test]
fn duplicate_column_names_are_deduplicated() {
    let (md, d) = read_cars_with_columns(Some(vec![
        "Brand".to_string(),
        "Brand".to_string(),
        "Model".to_string(),
    ]))
    .unwrap();

    // Duplicates should be removed; only 2 unique columns
    assert_eq!(md.var_count, 2);
    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "Brand");
    assert_eq!(batch.schema().field(1).name(), "Model");
}

#[test]
fn column_order_preserved_from_dataset() {
    // Request columns in reverse order; output should follow dataset order
    let (md, d) = read_cars_with_columns(Some(vec![
        "Hybrid".to_string(),   // index 12
        "Brand".to_string(),    // index 0
        "CityMPG".to_string(),  // index 8
    ]))
    .unwrap();

    assert_eq!(md.var_count, 3);
    let batch = d.batch.as_ref().unwrap();
    // Should be in dataset order: Brand (0), CityMPG (8), Hybrid (12)
    assert_eq!(batch.schema().field(0).name(), "Brand");
    assert_eq!(batch.schema().field(1).name(), "CityMPG");
    assert_eq!(batch.schema().field(2).name(), "Hybrid");
}

#[test]
fn parse_columns_file() {
    let temp_dir = std::env::temp_dir();
    let columns_file = temp_dir.join("test_columns.txt");

    std::fs::write(
        &columns_file,
        "# Columns to extract\nBrand\n\n# Another comment\nModel\n  EngineSize  \n",
    )
    .unwrap();

    let names = ReadStatMetadata::parse_columns_file(&columns_file).unwrap();
    assert_eq!(names, vec!["Brand", "Model", "EngineSize"]);

    // Clean up
    let _ = std::fs::remove_file(&columns_file);
}

#[test]
fn column_select_with_streaming() {
    // Test column selection with row offsets (simulating streaming)
    let rsp = common::setup_path("cars.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    let col_names = Some(vec!["Brand".to_string(), "CityMPG".to_string()]);
    let column_filter = md.resolve_selected_columns(col_names).unwrap();
    let original_var_count = md.var_count;
    let filtered_md = md.filter_to_selected_columns(column_filter.as_ref().unwrap());

    // Read first 10 rows
    let mut d1 = ReadStatData::new()
        .set_column_filter(column_filter.clone(), original_var_count)
        .set_no_progress(true)
        .init(filtered_md.clone(), 0, 10);
    d1.read_data(&rsp).unwrap();

    let batch1 = d1.batch.as_ref().unwrap();
    assert_eq!(batch1.num_columns(), 2);
    assert_eq!(batch1.num_rows(), 10);
    assert_eq!(batch1.schema().field(0).name(), "Brand");
    assert_eq!(batch1.schema().field(1).name(), "CityMPG");

    // Read next 10 rows
    let mut d2 = ReadStatData::new()
        .set_column_filter(column_filter, original_var_count)
        .set_no_progress(true)
        .init(filtered_md, 10, 20);
    d2.read_data(&rsp).unwrap();

    let batch2 = d2.batch.as_ref().unwrap();
    assert_eq!(batch2.num_columns(), 2);
    assert_eq!(batch2.num_rows(), 10);

    // Verify data from first batch
    let brand_col = batch1
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // First row Brand value should be non-null
    assert!(!brand_col.is_null(0));
}

#[test]
fn no_columns_filter_returns_all() {
    let (md, d) = read_cars_with_columns(None).unwrap();

    // No filter should return all 13 columns
    assert_eq!(md.var_count, 13);
    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_columns(), 13);
}
