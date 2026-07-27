use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use readstat::{ReadStatError, ReadStatReader, arrow_array::RecordBatch};

fn err_to_py(e: ReadStatError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn read_batch(bytes: &[u8], row_limit: Option<u32>) -> Result<RecordBatch, ReadStatError> {
    let reader = ReadStatReader::from_bytes(bytes);
    let row_count = u32::try_from(
        reader.metadata()?.row_count.ok_or(ReadStatError::RowCountUnavailable)?,
    )?;
    reader
        .rows(0, row_limit.map(|limit| limit.min(row_count)))
        .read()
}

#[pyfunction]
#[pyo3(signature = (data,))]
fn read_metadata(data: &[u8]) -> PyResult<String> {
    let md = ReadStatReader::from_bytes(data)
        .metadata()
        .map_err(err_to_py)?;
    serde_json::to_string(&md).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (data, row_limit=None))]
fn read_to_csv(data: &[u8], row_limit: Option<u32>) -> PyResult<Vec<u8>> {
    let batch = read_batch(data, row_limit).map_err(err_to_py)?;
    readstat::write_batch_to_csv_bytes(&batch).map_err(err_to_py)
}

#[pyfunction]
#[pyo3(signature = (data, row_limit=None))]
fn read_to_ndjson(data: &[u8], row_limit: Option<u32>) -> PyResult<Vec<u8>> {
    let batch = read_batch(data, row_limit).map_err(err_to_py)?;
    readstat::write_batch_to_ndjson_bytes(&batch).map_err(err_to_py)
}

#[pyfunction]
#[pyo3(signature = (data, row_limit=None))]
fn read_to_parquet(data: &[u8], row_limit: Option<u32>) -> PyResult<Vec<u8>> {
    let batch = read_batch(data, row_limit).map_err(err_to_py)?;
    readstat::write_batch_to_parquet_bytes(&batch).map_err(err_to_py)
}

#[pyfunction]
#[pyo3(signature = (data, row_limit=None))]
fn read_to_feather(data: &[u8], row_limit: Option<u32>) -> PyResult<Vec<u8>> {
    let batch = read_batch(data, row_limit).map_err(err_to_py)?;
    readstat::write_batch_to_feather_bytes(&batch).map_err(err_to_py)
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
