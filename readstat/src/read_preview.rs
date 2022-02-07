

pub fn get_preview(d: &mut ReadStatData, row_limit: u32) -> Result<(), Box<dyn Error>> {
    // how many rows to process?
    d.batch_rows_to_process = row_limit as usize;
    d.batch_row_start = 0;
    d.batch_row_end = row_limit as usize;

    let error = d.get_preview(Some(row_limit), None)?;

    match FromPrimitive::from_i32(error as i32) {
        Some(ReadStatError::READSTAT_OK) => {
            if !d.no_write {
                d.write()?;
                d.wrote_start = true;
            };
            Ok(())
        }
        Some(e) => Err(From::from(format!(
            "Error when attempting to parse sas7bdat: {:#?}",
            e
        ))),
        None => Err(From::from(
            "Error when attempting to parse sas7bdat: Unknown return value",
        )),
    }
}