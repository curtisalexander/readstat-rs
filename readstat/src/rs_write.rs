use arrow2::io::{
    ipc as ipc_arrow2,
    parquet::{self as parquet_arrow2},
};

pub struct ReadStatParquetWriter {
    pub wtr: Box<parquet_arrow2::write::FileWriter<std::fs::File>>,
    pub options: parquet_arrow2::write::WriteOptions,
    pub encodings: Vec<Vec<parquet_arrow2::write::Encoding>>,
}

impl ReadStatParquetWriter {
    pub fn new(
        wtr: Box<parquet_arrow2::write::FileWriter<std::fs::File>>,
        options: parquet_arrow2::write::WriteOptions,
        encodings: Vec<Vec<parquet_arrow2::write::Encoding>>,
    ) -> Self {
        Self {
            wtr,
            options,
            encodings,
        }
    }
}

pub enum ReadStatWriterFormat {
    Csv(std::fs::File),
    CsvStdout(std::io::Stdout),
    Feather(Box<ipc_arrow2::write::FileWriter<std::fs::File>>),
    Ndjson(std::fs::File),
    Parquet(ReadStatParquetWriter),
}
