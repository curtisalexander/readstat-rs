use readstat::ReadStatMetadata;
use wasm_bindgen::prelude::*;

/// Read metadata from a `.sas7bdat` file provided as a byte slice.
///
/// Returns a JSON string containing file-level and variable-level metadata.
#[wasm_bindgen]
pub fn read_metadata(bytes: &[u8]) -> Result<String, JsValue> {
    let mut md = ReadStatMetadata::new();
    md.read_metadata_from_bytes(bytes, false)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    serde_json::to_string(&md).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Read metadata from a `.sas7bdat` file, skipping row count for speed.
///
/// Returns a JSON string containing file-level and variable-level metadata.
/// The `row_count` field will not reflect the true row count.
#[wasm_bindgen]
pub fn read_metadata_fast(bytes: &[u8]) -> Result<String, JsValue> {
    let mut md = ReadStatMetadata::new();
    md.read_metadata_from_bytes(bytes, true)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    serde_json::to_string(&md).map_err(|e| JsValue::from_str(&e.to_string()))
}
