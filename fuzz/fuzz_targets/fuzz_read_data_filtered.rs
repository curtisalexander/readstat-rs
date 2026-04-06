#![no_main]
use libfuzzer_sys::fuzz_target;
use readstat::{ReadStatData, ReadStatMetadata};

use arbitrary::Arbitrary;
use arrow::datatypes::{Field, Schema};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Arbitrary, Debug)]
struct FuzzInput<'a> {
    data: &'a [u8],
    /// Indices of columns to select (will be mapped to valid range).
    selected_columns: Vec<u16>,
}

// Cap rows and columns to avoid OOM from malformed metadata.
const MAX_ROWS: u32 = 100_000;
const MAX_VARS: i32 = 1_000;

fuzz_target!(|input: FuzzInput| {
    let mut md = ReadStatMetadata::new();
    if md.read_metadata_from_bytes(input.data, false).is_err() {
        return;
    }

    let total_var_count = md.var_count;
    if total_var_count == 0 || total_var_count > MAX_VARS {
        return;
    }

    // Map arbitrary indices into the valid variable range, deduplicate.
    let mut selected: Vec<i32> = input
        .selected_columns
        .iter()
        .map(|&idx| (idx as i32) % total_var_count)
        .collect();
    selected.sort_unstable();
    selected.dedup();

    if selected.is_empty() {
        return;
    }

    // Build the column filter: original_index -> filtered_index
    let filter: BTreeMap<i32, i32> = selected
        .iter()
        .enumerate()
        .map(|(new_idx, &orig_idx)| (orig_idx, new_idx as i32))
        .collect();

    // Rebuild vars and schema to match filtered columns
    let filtered_vars: BTreeMap<i32, _> = selected
        .iter()
        .enumerate()
        .filter_map(|(new_idx, &orig_idx)| {
            md.vars
                .get(&orig_idx)
                .cloned()
                .map(|v| (new_idx as i32, v))
        })
        .collect();

    let filtered_fields: Vec<Field> = selected
        .iter()
        .filter_map(|&idx| {
            md.schema
                .fields()
                .get(idx as usize)
                .map(|f| f.as_ref().clone())
        })
        .collect();

    md.vars = filtered_vars;
    md.var_count = selected.len() as i32;
    md.schema = Schema::new(filtered_fields);

    let row_count = (md.row_count as u32).min(MAX_ROWS);
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .set_column_filter(Some(Arc::new(filter)), total_var_count)
        .init(md, 0, row_count);
    let _ = d.read_data_from_bytes(input.data);
});
