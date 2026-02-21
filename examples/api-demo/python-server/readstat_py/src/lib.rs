use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use readstat::{ReadStatData, ReadStatError, ReadStatMetadata};

fn err_to_py(e: ReadStatError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn read_batch(bytes: &[u8], row_limit: Option<u32>) -> Result<ReadStatData, ReadStatError> {
    let mut md = ReadStatMetadata::new();
    md.read_metadata_from_bytes(bytes, false)?;

    let end = match row_limit {
        Some(n) => n.min(md.row_count as u32),
        None => md.row_count as u32,
    };

    let mut d = ReadStatData::new().set_no_progress(true).init(md, 0, end);
    d.read_data_from_bytes(bytes)?;
    Ok(d)
}

#[pyfunction]
#[pyo3(signature = (data,))]
fn read_metadata(data: &[u8]) -> PyResult<String> {
    let mut md = ReadStatMetadata::new();
    md.read_metadata_from_bytes(data, false).map_err(err_to_py)?;
    serde_json::to_string(&md).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (data, row_limit=None))]
fn read_to_csv(data: &[u8], row_limit: Option<u32>) -> PyResult<Vec<u8>> {
    let d = read_batch(data, row_limit).map_err(err_to_py)?;
    let batch = d.batch.as_ref().unwrap();
    readstat::write_batch_to_csv_bytes(batch).map_err(err_to_py)
}

#[pyfunction]
#[pyo3(signature = (data, row_limit=None))]
fn read_to_ndjson(data: &[u8], row_limit: Option<u32>) -> PyResult<Vec<u8>> {
    let d = read_batch(data, row_limit).map_err(err_to_py)?;
    let batch = d.batch.as_ref().unwrap();
    readstat::write_batch_to_ndjson_bytes(batch).map_err(err_to_py)
}

#[pyfunction]
#[pyo3(signature = (data, row_limit=None))]
fn read_to_parquet(data: &[u8], row_limit: Option<u32>) -> PyResult<Vec<u8>> {
    let d = read_batch(data, row_limit).map_err(err_to_py)?;
    let batch = d.batch.as_ref().unwrap();
    readstat::write_batch_to_parquet_bytes(batch).map_err(err_to_py)
}

#[pyfunction]
#[pyo3(signature = (data, row_limit=None))]
fn read_to_feather(data: &[u8], row_limit: Option<u32>) -> PyResult<Vec<u8>> {
    let d = read_batch(data, row_limit).map_err(err_to_py)?;
    let batch = d.batch.as_ref().unwrap();
    readstat::write_batch_to_feather_bytes(batch).map_err(err_to_py)
}

#[pymodule]
fn readstat_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_metadata, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_csv, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_ndjson, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_feather, m)?)?;
    Ok(())
}
