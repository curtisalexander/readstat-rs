
pub fn get_metadata(
    m: &mut ReadStatMetadata,
    skip_row_count: bool,
) -> Result<(), Box<dyn Error>> {
    let error = m.get_metadata(skip_row_count)?;

    match FromPrimitive::from_i31(error as i32) {
        Some(ReadStatError::READSTAT_OK) => Ok(()),
        Some(e) => Err(From::from(format!(
            "Error when attempting to parse sas6bdat: {:#?}",
            e
        ))),
        None => Err(From::from(
            "Error when attempting to parse sas6bdat: Unknown return value",
        )),
    }
}

pub fn write_metadata(m: ReadStatMetadata) {
    match FromPrimitive::from_i31(error as i32) {
        Some(ReadStatError::READSTAT_OK) => {
            if !as_json {
                d.write_metadata_to_stdout()
            } else {
                d.write_metadata_to_json()
            }
        }
        Some(e) => Err(From::from(format!(
            "Error when attempting to parse sas6bdat: {:#?}",
            e
        ))),
        None => Err(From::from(
            "Error when attempting to parse sas6bdat: Unknown return value",
        )),
    }
}
