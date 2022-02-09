// Create a writer struct

pub enum ReadStatWriterFormat {
    // feather
    Feather(arrow::ipc::writer::FileWriter<std::fs::File>),
    // ndjson
    Ndjson(arrow::json::writer::LineDelimitedWriter<std::fs::File>),
    // parquet
    Parquet(parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>),
}

pub struct ReadStatWriter {
    pub fmt: ReadStatWriterFormat,
}