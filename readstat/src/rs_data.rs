use arrow2::{
    array::{Array, PrimitiveArray, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Schema, TimeUnit},
    error::Error as ArrowError,
    io::{
        csv as csv_arrow2, ipc as ipc_arrow2, ndjson as ndjson_arrow2,
        parquet::{self as parquet_arrow2, write::RowGroupIterator},
    },
};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_format::{Locale, ToFormattedString};
use num_traits::FromPrimitive;
use path_abs::PathInfo;
use std::{
    collections::BTreeMap,
    error::Error,
    fs::OpenOptions,
    io::stdout,
    os::raw::c_void,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{
    cb,
    err::ReadStatError,
    rs_metadata::{ReadStatMetadata, ReadStatVarMetadata},
    rs_parser::ReadStatParser,
    rs_path::ReadStatPath,
    rs_var::ReadStatVar,
    rs_write::{ReadStatParquetWriter, ReadStatWriterFormat},
    OutFormat,
};

#[derive(Default)]
pub struct ReadStatData {
    // metadata
    pub var_count: i32,
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    // data
    pub cols: Vec<Vec<ReadStatVar>>,
    pub schema: Schema,
    // chunk
    pub chunk: Option<Chunk<Box<dyn Array>>>,
    pub chunk_rows_to_process: usize, // min(stream_rows, row_limit, row_count)
    pub chunk_row_start: usize,
    pub chunk_row_end: usize,
    pub chunk_rows_processed: usize,
    // writer
    pub writer: Option<ReadStatWriterFormat>,
    pub wrote_header: bool,
    pub wrote_start: bool,
    // total rows
    pub total_rows_to_process: usize,
    pub total_rows_processed: Option<Arc<AtomicUsize>>,
    // progress
    pub pb: Option<ProgressBar>,
    pub no_progress: bool,
    // errors
    pub errors: Vec<String>,
}

impl ReadStatData {
    pub fn new() -> Self {
        Self {
            // metadata
            var_count: 0,
            vars: BTreeMap::new(),
            // data
            cols: Vec::new(),
            schema: Schema::default(),
            // chunk
            chunk: None,
            chunk_rows_to_process: 0,
            chunk_rows_processed: 0,
            chunk_row_start: 0,
            chunk_row_end: 0,
            // writer
            writer: None,
            wrote_header: false,
            wrote_start: false,
            // total rows
            total_rows_to_process: 0,
            total_rows_processed: None,
            // progress
            pb: None,
            no_progress: false,
            // errors
            errors: Vec::new(),
        }
    }

    fn allocate_cols(self) -> Self {
        let mut cols = Vec::with_capacity(self.var_count as usize);
        for _ in 0..self.var_count {
            cols.push(Vec::with_capacity(self.chunk_rows_to_process))
        }
        Self { cols, ..self }
    }

    fn cols_to_chunk(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // for each column in cols
        let arrays: Vec<Box<dyn Array>> = self
            .cols
            .iter()
            .map(|col| {
                // what kind of column is this?
                // grab the first element to determine the column type
                let col_type = &col[0];

                // convert from a Vec<ReadStatVar> into a Box<dyn Array>
                let array: Box<dyn Array> = match col_type {
                    ReadStatVar::ReadStat_String(_) => {
                        // get the inner value
                        let vec = col
                            .iter()
                            .map(|s| {
                                if let ReadStatVar::ReadStat_String(v) = s {
                                    v.clone()
                                } else {
                                    // should NEVER fall into this branch
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<String>>>();

                        Box::new(<Utf8Array<i32>>::from(vec))
                    }
                    ReadStatVar::ReadStat_i8(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i8(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i8>>>();

                        Box::new(<PrimitiveArray<i8>>::from(vec))
                    }
                    ReadStatVar::ReadStat_i16(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i16(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i16>>>();

                        Box::new(<PrimitiveArray<i16>>::from(vec))
                    }
                    ReadStatVar::ReadStat_i32(_) => {
                        let vec = col
                            .iter()
                            .map(|i| {
                                if let ReadStatVar::ReadStat_i32(v) = i {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Box::new(<PrimitiveArray<i32>>::from(vec))
                    }
                    ReadStatVar::ReadStat_f32(_) => {
                        let vec = col
                            .iter()
                            .map(|f| {
                                if let ReadStatVar::ReadStat_f32(v) = f {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<f32>>>();

                        Box::new(<PrimitiveArray<f32>>::from(vec))
                    }
                    ReadStatVar::ReadStat_f64(_) => {
                        let vec = col
                            .iter()
                            .map(|f| {
                                if let ReadStatVar::ReadStat_f64(v) = f {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<f64>>>();

                        Box::new(<PrimitiveArray<f64>>::from(vec))
                    }
                    ReadStatVar::ReadStat_Date(_) => {
                        let vec = col
                            .iter()
                            .map(|d| {
                                if let ReadStatVar::ReadStat_Date(v) = d {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Box::new(<PrimitiveArray<i32>>::from(vec).to(DataType::Date32))
                    }
                    ReadStatVar::ReadStat_DateTime(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTime(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Second, None)),
                        )
                    }
                    ReadStatVar::ReadStat_DateTimeWithMilliseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithMilliseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
                        )
                    }
                    ReadStatVar::ReadStat_DateTimeWithMicroseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithMicroseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
                        )
                    }
                    ReadStatVar::ReadStat_DateTimeWithNanoseconds(_) => {
                        let vec = col
                            .iter()
                            .map(|dt| {
                                if let ReadStatVar::ReadStat_DateTimeWithNanoseconds(v) = dt {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i64>>>();

                        Box::new(
                            <PrimitiveArray<i64>>::from(vec)
                                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                        )
                    }
                    ReadStatVar::ReadStat_Time(_) => {
                        let vec = col
                            .iter()
                            .map(|t| {
                                if let ReadStatVar::ReadStat_Time(v) = t {
                                    *v
                                } else {
                                    unreachable!()
                                }
                            })
                            .collect::<Vec<Option<i32>>>();

                        Box::new(
                            <PrimitiveArray<i32>>::from(vec).to(DataType::Time32(TimeUnit::Second)),
                        )
                    }
                };

                // return
                array
            })
            .collect();

        // convert into a chunk
        self.chunk = Some(Chunk::try_new(arrays)?);

        Ok(())
    }

    pub fn read_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // parse data and if successful then convert cols into a chunk
        self.parse_data(rsp)?;
        self.cols_to_chunk()?;
        Ok(())
    }

    fn parse_data(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        // path as pointer
        debug!("Path as C string is {:?}", &rsp.cstring_path);
        let ppath = rsp.cstring_path.as_ptr();

        // spinner
        // TODO - uncomment when ready to reimplement progress bar
        /*
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        */

        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}")?,
            );
            let msg = format!(
                "Parsing sas7bdat data from file {}",
                &rsp.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(std::time::Duration::new(120, 0));
        }

        // initialize context
        let ctx = self as *mut ReadStatData as *mut c_void;

        // initialize error
        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {:#?}", &error);

        // setup parser
        // once call parse_sas7bdat, iteration begins
        let error = ReadStatParser::new()
            // do not set metadata handler nor variable handler as already processed
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(Some(self.chunk_rows_to_process.try_into().unwrap()))?
            .set_row_offset(Some(self.chunk_row_start.try_into().unwrap()))?
            .parse_sas7bdat(ppath, ctx);

        match FromPrimitive::from_i32(error as i32) {
            Some(ReadStatError::READSTAT_OK) => Ok(()),
            Some(e) => Err(From::from(format!(
                "Error when attempting to parse sas7bdat: {:#?}",
                e
            ))),
            None => Err(From::from(
                "Error when attempting to parse sas7bdat: Unknown return value",
            )),
        }
    }

    /*
    pub fn get_row_count(&mut self) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata_row_count_only))?
            .parse_sas7bdat(ppath, ctx);

        Ok(error as u32)
    }
    */

    pub fn init(self, md: ReadStatMetadata, row_start: u32, row_end: u32) -> Self {
        self.set_metadata(md)
            .set_chunk_counts(row_start, row_end)
            .allocate_cols()
    }

    fn set_chunk_counts(self, row_start: u32, row_end: u32) -> Self {
        let chunk_rows_to_process = (row_end - row_start) as usize;
        let chunk_row_start = row_start as usize;
        let chunk_row_end = row_end as usize;
        let chunk_rows_processed = 0_usize;

        Self {
            chunk_rows_to_process,
            chunk_row_start,
            chunk_row_end,
            chunk_rows_processed,
            ..self
        }
    }

    fn set_metadata(self, md: ReadStatMetadata) -> Self {
        let var_count = md.var_count;
        let vars = md.vars;
        let schema = md.schema;
        Self {
            var_count,
            vars,
            schema,
            ..self
        }
    }

    pub fn set_no_progress(self, no_progress: bool) -> Self {
        Self {
            no_progress,
            ..self
        }
    }

    pub fn set_total_rows_to_process(self, total_rows_to_process: usize) -> Self {
        Self {
            total_rows_to_process,
            ..self
        }
    }

    pub fn set_total_rows_processed(self, total_rows_processed: Arc<AtomicUsize>) -> Self {
        Self {
            total_rows_processed: Some(total_rows_processed),
            ..self
        }
    }

    pub fn write_finish(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        match rsp {
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } => self.write_finish_txt(rsp),
            // Write feather data to file
            ReadStatPath {
                format: OutFormat::feather,
                ..
            } => self.write_finish_feather(rsp),
            // Write ndjson data to file
            ReadStatPath {
                format: OutFormat::ndjson,
                ..
            } => self.write_finish_txt(rsp),
            // Write parquet data to file
            ReadStatPath {
                format: OutFormat::parquet,
                ..
            } => self.write_finish_parquet(rsp),
            _ => Ok(()),
        }
    }

    fn _write_message_for_file(&mut self, rsp: &ReadStatPath) {
        if let Some(pb) = &self.pb {
            let in_f = if let Some(f) = rsp.path.file_name() {
                f.to_string_lossy().bright_red()
            } else {
                String::from("___").bright_red()
            };

            let out_f = if let Some(p) = &rsp.out_path {
                if let Some(f) = p.file_name() {
                    f.to_string_lossy().bright_green()
                } else {
                    String::from("___").bright_green()
                }
            } else {
                String::from("___").bright_green()
            };

            let msg = format!("Writing file {} as {}", in_f, out_f);

            pb.set_message(msg);
        }
    }

    fn write_message_for_rows(
        &mut self,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        //if let Some(pb) = &d.pb {
        let in_f = if let Some(f) = rsp.path.file_name() {
            f.to_string_lossy().bright_red()
        } else {
            String::from("___").bright_red()
        };

        let out_f = if let Some(p) = &rsp.out_path {
            if let Some(f) = p.file_name() {
                f.to_string_lossy().bright_green()
            } else {
                String::from("___").bright_green()
            }
        } else {
            String::from("___").bright_green()
        };

        let rows = self
            .chunk_rows_processed
            .to_formatted_string(&Locale::en)
            .truecolor(255, 132, 0);

        let msg = format!("Wrote {} rows from file {} into {}", rows, in_f, out_f);

        println!("{}", msg);
        //pb.set_message(msg);
        //}
        Ok(())
    }

    fn write_finish_txt(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        //if let Some(pb) = &d.pb {
        let in_f = if let Some(f) = rsp.path.file_name() {
            f.to_string_lossy().bright_red()
        } else {
            String::from("___").bright_red()
        };

        let out_f = if let Some(p) = &rsp.out_path {
            if let Some(f) = p.file_name() {
                f.to_string_lossy().bright_green()
            } else {
                String::from("___").bright_green()
            }
        } else {
            String::from("___").bright_green()
        };

        let rows = if let Some(trp) = &self.total_rows_processed {
            trp.load(std::sync::atomic::Ordering::SeqCst)
                .to_formatted_string(&Locale::en)
                .truecolor(255, 132, 0)
        } else {
            0.to_formatted_string(&Locale::en).truecolor(255, 132, 0)
        };

        let msg = format!(
            "In total, wrote {} rows from file {} into {}",
            rows, in_f, out_f
        );

        println!("{}", msg);

        //pb.set_message(msg);
        //}
        Ok(())
    }

    pub fn write(&mut self, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        match rsp {
            // Write data to standard out
            ReadStatPath {
                out_path: None,
                format: OutFormat::csv,
                ..
            } if self.wrote_header => self.write_data_to_stdout(),
            // Write header and data to standard out
            ReadStatPath {
                out_path: None,
                format: OutFormat::csv,
                ..
            } => {
                self.write_header_to_stdout()?;
                self.write_data_to_stdout()
            }
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } if self.wrote_header => self.write_data_to_csv(rsp),
            // Write csv header to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } => {
                self.write_header_to_csv(rsp)?;
                self.write_data_to_csv(rsp)
            }
            // Write feather data to file
            ReadStatPath {
                format: OutFormat::feather,
                ..
            } => self.write_data_to_feather(rsp),
            // Write ndjson data to file
            ReadStatPath {
                format: OutFormat::ndjson,
                ..
            } => self.write_data_to_ndjson(rsp),
            // Write parquet data to file
            ReadStatPath {
                format: OutFormat::parquet,
                ..
            } => self.write_data_to_parquet(rsp),
        }
    }

    fn write_data_to_csv(
        &mut self,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?;

            // set message for what is being read/written
            self.write_message_for_rows(rsp)?;

            // setup writer
            if !self.wrote_start {
                self.writer = Some(ReadStatWriterFormat::Csv(f))
            };

            // write
            if let Some(ReadStatWriterFormat::Csv(f)) = &mut self.writer {
                let options = csv_arrow2::write::SerializeOptions::default();

                if let Some(c) = &self.chunk {
                    let cols = &[c];
                    cols.iter()
                        .try_for_each(|batch| csv_arrow2::write::write_chunk(f, batch, &options))?;
                };

                // update
                self.wrote_start = true;
                Ok(())
            } else {
                Err(From::from(
                    "Error writing csv as associated writer is not for the csv format",
                ))
            }
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_data_to_feather(
        &mut self,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?;

            // set message for what is being read/written
            self.write_message_for_rows(rsp)?;

            // setup writer
            if !self.wrote_start {
                let options = ipc_arrow2::write::WriteOptions {
                    compression: Some(ipc_arrow2::write::Compression::ZSTD),
                };

                let wtr = ipc_arrow2::write::FileWriter::try_new(f, &self.schema, None, options)?;

                self.writer = Some(ReadStatWriterFormat::Feather(Box::new(wtr)));
            };

            // write
            if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.writer {
                if let Some(c) = &self.chunk {
                    wtr.write(c, None)?;
                };

                // update
                self.wrote_start = true;

                Ok(())
            } else {
                Err(From::from(
                    "Error writing feather as associated writer is not for the feather format",
                ))
            }
        } else {
            Err(From::from(
                "Error writing feather file as output path is set to None",
            ))
        }
    }

    fn write_finish_feather(
        &mut self,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.writer {
            wtr.finish()?;

            // set message for what is being read/written
            self.write_finish_txt(rsp)?;

            Ok(())
        } else {
            Err(From::from(
                "Error writing feather as associated writer is not for the feather format",
            ))
        }
    }

    fn write_data_to_ndjson(
        &mut self,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?;

            // set message for what is being read/written
            self.write_message_for_rows(rsp)?;

            // setup writer
            if !self.wrote_start {
                self.writer = Some(ReadStatWriterFormat::Ndjson(f));
            };

            // write
            if let Some(ReadStatWriterFormat::Ndjson(f)) = &mut self.writer {
                if let Some(c) = &self.chunk {
                    let arrays = c.columns().iter().map(Ok);

                    // serializer
                    let serializer = ndjson_arrow2::write::Serializer::new(arrays, vec![]);

                    // writer
                    let mut wtr = ndjson_arrow2::write::FileWriter::new(f, serializer);

                    // drive iterator
                    wtr.by_ref().collect::<Result<(), ArrowError>>()?;
                }

                // update
                self.wrote_start = true;

                Ok(())
            } else {
                Err(From::from(
                    "Error writing ndjson as associated writer is not for the ndjson format",
                ))
            }
        } else {
            Err(From::from(
                "Error writing ndjson file as output path is set to None",
            ))
        }
    }

    fn write_data_to_parquet(
        &mut self,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?;

            // set message for what is being read/written
            self.write_message_for_rows(rsp)?;

            // setup writer
            if !self.wrote_start {
                let options = parquet_arrow2::write::WriteOptions {
                    write_statistics: true,
                    compression: parquet_arrow2::write::CompressionOptions::Snappy,
                    version: parquet_arrow2::write::Version::V2,
                };

                let encodings: Vec<Vec<parquet_arrow2::write::Encoding>> = self
                    .schema
                    .fields
                    .iter()
                    .map(|f| {
                        parquet_arrow2::write::transverse(&f.data_type, |_| {
                            parquet_arrow2::write::Encoding::Plain
                        })
                    })
                    .collect();

                let wtr =
                    parquet_arrow2::write::FileWriter::try_new(f, self.schema.clone(), options)?;

                self.writer = Some(ReadStatWriterFormat::Parquet(ReadStatParquetWriter::new(
                    Box::new(wtr),
                    options,
                    encodings,
                )));
            }

            // write
            if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.writer {
                if let Some(c) = self.chunk.clone() {
                    let iter = vec![Ok(c)];
                    // let iter: Vec<Result<Chunk<Box<dyn Array>>, ArrowError>> = vec![Ok(c)];

                    let row_groups = RowGroupIterator::try_new(
                        iter.into_iter(),
                        &self.schema,
                        pwtr.options,
                        pwtr.encodings.clone(),
                    )?;

                    for group in row_groups {
                        pwtr.wtr.write(group?)?;
                    }
                }

                // update
                self.wrote_start = true;

                Ok(())
            } else {
                Err(From::from(
                    "Error writing parquet as associated writer is not for the parquet format",
                ))
            }
        } else {
            Err(From::from(
                "Error writing parquet file as output path is set to None",
            ))
        }
    }

    fn write_finish_parquet(
        &mut self,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.writer {
            let _size = pwtr.wtr.end(None)?;

            // set message for what is being read/written
            self.write_finish_txt(rsp)?;

            Ok(())
        } else {
            Err(From::from(
                "Error writing parquet as associated writer is not for the parquet format",
            ))
        }
    }

    fn write_data_to_stdout(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(pb) = &self.pb {
            pb.finish_and_clear()
        }

        // writer setup
        if !self.wrote_start {
            self.writer = Some(ReadStatWriterFormat::CsvStdout(stdout()));
        };

        // write
        if let Some(ReadStatWriterFormat::CsvStdout(f)) = &mut self.writer {
            let options = csv_arrow2::write::SerializeOptions::default();

            if let Some(c) = &self.chunk {
                let cols = &[c];
                cols.iter()
                    .try_for_each(|batch| csv_arrow2::write::write_chunk(f, batch, &options))?;
            };

            // update
            self.wrote_start = true;

            Ok(())
        } else {
            Err(From::from(
                "Error writing to csv as associated writer is not for the csv format",
            ))
        }
    }

    fn write_header_to_csv(
        &mut self,
        rsp: &ReadStatPath,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // spinner
            /*
            if let Some(pb) = d.pb {
                pb.finish_at_current_pos();
            }
            */

            // spinner
            /*
            if !d.no_progress {
                d.pb = Some(ProgressBar::new(!0));
            }
            if let Some(pb) = d.pb {
                pb.set_style(
                    ProgressStyle::default_spinner()
                        .template("[{spinner:.green} {elapsed_precise} | {bytes}] {msg}"),
                );

                let in_f = if let Some(f) = rsp.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = rsp.out_path {
                    if let Some(f) = p.file_name() {
                        f.to_string_lossy().bright_green()
                    } else {
                        String::from("___").bright_green()
                    }
                } else {
                    String::from("___").bright_green()
                };

                let msg = format!("Writing file {} as {}", in_f, out_f);

                pb.set_message(msg);
                pb.enable_steady_tick(120);
            }
            */
            // progress bar
            /*
            if !self.no_progress {
                self.pb = Some(ProgressBar::new(self.row_count as u64));
            }
            if let Some(pb) = &self.pb {
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[{spinner:.green} {elapsed_precise}] {bar:30.cyan/blue} {pos:>7}/{len:7} {msg}")
                        .progress_chars("##-"),
                );
                pb.set_message("Rows processed");
                pb.enable_steady_tick(120);
            }
            */

            // create file
            let mut f = std::fs::File::create(p)?;

            // Get variable names
            let vars: Vec<String> = self.vars.iter().map(|(_, m)| m.var_name.clone()).collect();

            // write
            let options = csv_arrow2::write::SerializeOptions::default();
            csv_arrow2::write::write_header(&mut f, &vars, &options)?;

            // wrote header
            self.wrote_header = true;

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_header_to_stdout(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(pb) = &self.pb {
            pb.finish_and_clear()
        }

        // Get variable names
        let vars: Vec<String> = self.vars.iter().map(|(_, m)| m.var_name.clone()).collect();

        // write
        let options = csv_arrow2::write::SerializeOptions::default();
        csv_arrow2::write::write_header(&mut stdout(), &vars, &options)?;

        // wrote header
        self.wrote_header = true;

        // return
        Ok(())
    }
}
