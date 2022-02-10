// Create a writer struct

use std::io::Write;


pub enum ReadStatWriterFormat {
    // feather
    Feather(arrow::ipc::writer::FileWriter<std::fs::File>),
    // ndjson
    Ndjson(arrow::json::writer::LineDelimitedWriter<std::fs::File>),
    // parquet
    Parquet(parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>),
}


pub struct ReadStatWriter {
    pub fmt: Option<ReadStatWriterFormat>,
    pub wtr: Option<dyn Write>,
    pub wrote_header: bool,
    pub wrote_start: bool,
    pub finish: bool
}

impl ReadStatWriter {
    pub fn new() -> Self {
        Self {
            fmt: None,
            wtr: None,
            wrote_header: false,
            wrote_start: false,
            finish: false,
        }
    }
}