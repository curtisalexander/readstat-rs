use arrow::array::{
    ArrayBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int8Builder, StringBuilder, Time32SecondBuilder, TimestampSecondBuilder,
};
use arrow::csv as csv_arrow;
use arrow::ipc::writer::FileWriter;
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use arrow::{datatypes, record_batch};
use colored::Colorize;
use csv as csv_crate;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use num_format::{Locale, ToFormattedString};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use path_abs::PathInfo;
use serde_json;
use std::error::Error;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::stdout;
use std::os::raw::{c_uint, c_void};
use std::path::PathBuf;
use std::sync::Arc;

use crate::cb;
use crate::{Format, Reader};
use crate::rs_metadata::{ReadStatMetadata, ReadStatVarType, ReadStatFormatClass};
use crate::rs_path::ReadStatPath;
use crate::rs_parser::ReadStatParser;

/**********
 * Writer *
 *********/

pub enum ReadStatWriter {
    // feather
    Feather(arrow::ipc::writer::FileWriter<std::fs::File>),
    // ndjson
    Ndjson(arrow::json::writer::LineDelimitedWriter<std::fs::File>),
    // parquet
    Parquet(parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>),
}

/********
 * Data *
 *******/

pub struct ReadStatData {
    // path
    pub path: PathBuf,
    pub cstring_path: CString,
    pub out_path: Option<PathBuf>,
    // metadata
    pub metadata: ReadStatMetadata,
    // data
    pub cols: Vec<Box<dyn ArrayBuilder>>,
    pub schema: datatypes::Schema,
    pub batch: record_batch::RecordBatch,
    // writer and format
    pub format: Format,
    // should probably be declared with a trait but just utilizing enum for the time being
    pub wtr: Option<ReadStatWriter>,
    pub wrote_header: bool,
    pub wrote_start: bool,
    pub finish: bool,
    // usage
    pub reader: Reader,
    pub stream_rows: c_uint,
    pub rows_to_process: usize,  // min(stream_rows, metadata.row_count)
    pub pb: Option<ProgressBar>,
    pub no_progress: bool,
    pub is_test: bool,
    // errors
    pub errors: Vec<String>,
}

impl ReadStatData {
    pub fn new(rsp: ReadStatPath) -> Self {
        Self {
            // path
            path: rsp.path,
            cstring_path: rsp.cstring_path,
            out_path: rsp.out_path,
            // metadata
            metadata: ReadStatMetadata::new(),
            format: rsp.format,
            // data
            cols: Vec::new(),
            schema: datatypes::Schema::empty(),
            batch: RecordBatch::new_empty(Arc::new(datatypes::Schema::empty())),
            // writer and format
            wtr: None,
            wrote_header: false,
            wrote_start: false,
            finish: false,
            // usage
            reader: Reader::stream,
            stream_rows: 50000,
            rows_to_process: 0,
            pb: None,
            no_progress: false,
            is_test: false,
            // errors
            errors: Vec::new(),
        }
    }

    pub fn allocate_cols(&mut self, rows: usize) {
        for i in 0..self.metadata.var_count {
            let var_type = self.metadata.vars.get(&i).unwrap().var_type;
            // Allocate space for ArrayBuilder
            let array: Box<dyn ArrayBuilder> = match var_type {
                ReadStatVarType::String | ReadStatVarType::StringRef | ReadStatVarType::Unknown => {
                    Box::new(StringBuilder::new(rows))
                }
                ReadStatVarType::Int8 => Box::new(Int8Builder::new(rows)),
                ReadStatVarType::Int16 => Box::new(Int16Builder::new(rows)),
                ReadStatVarType::Int32 => Box::new(Int32Builder::new(rows)),
                ReadStatVarType::Float => Box::new(Float32Builder::new(rows)),
                ReadStatVarType::Double => match self.metadata.vars.get(&i).unwrap().var_format_class {
                    None => Box::new(Float64Builder::new(rows)),
                    Some(ReadStatFormatClass::Date) => Box::new(Date32Builder::new(rows)),
                    Some(ReadStatFormatClass::DateTime) |
                    Some(ReadStatFormatClass::DateTimeWithMilliseconds) |
                    Some(ReadStatFormatClass::DateTimeWithMicroseconds) |
                    Some(ReadStatFormatClass::DateTimeWithNanoseconds) => {
                        Box::new(TimestampSecondBuilder::new(rows))
                    }
                    Some(ReadStatFormatClass::Time) => Box::new(Time32SecondBuilder::new(rows)),
                },
            };

            self.cols.push(array);
        }
    }

    pub fn get_data(
        &mut self,
        row_limit: Option<u32>,
        row_offset: Option<u32>,
    ) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat data from file {}",
                &self.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }
        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {:#?}", &error);

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(row_limit)?
            .set_row_offset(row_offset)?
            .parse_sas7bdat(ppath, ctx);

        Ok(error as u32)
    }

    pub fn get_metadata(&mut self, skip_row_count: bool) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat metadata from file {}",
                &self.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }
        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let row_limit = if skip_row_count { Some(1) } else { None };

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .set_row_limit(row_limit)?
            .parse_sas7bdat(ppath, ctx);

        if let Some(pb) = &self.pb {
            pb.finish_and_clear();
        }

        Ok(error as u32)
    }

    pub fn get_preview(
        &mut self,
        row_limit: Option<u32>,
        row_offset: Option<u32>,
    ) -> Result<u32, Box<dyn Error>> {
        debug!("Path as C string is {:?}", &self.cstring_path);
        let ppath = self.cstring_path.as_ptr();

        // spinner
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat data from file {}",
                &self.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }
        let ctx = self as *mut ReadStatData as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let error = ReadStatParser::new()
            // TODO: for just a data preview, a new metadata handler may be needed that
            //   does not get the row count but just the var count
            // Believe it will save time when working with extremely large files
            .set_metadata_handler(Some(cb::handle_metadata))?
            .set_variable_handler(Some(cb::handle_variable))?
            .set_value_handler(Some(cb::handle_value))?
            .set_row_limit(row_limit)?
            .set_row_offset(row_offset)?
            .parse_sas7bdat(ppath, ctx);

        Ok(error as u32)
    }

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

    pub fn set_is_test(self, is_test: bool) -> Self {
        Self { is_test, ..self }
    }

    pub fn set_no_progress(self, no_progress: bool) -> Self {
        Self {
            no_progress,
            ..self
        }
    }

    pub fn set_reader(self, reader: Option<Reader>) -> Self {
        if let Some(r) = reader {
            Self { reader: r, ..self }
        } else {
            self
        }
    }

    pub fn set_stream_rows(self, stream_rows: Option<c_uint>) -> Self {
        match self.reader {
            Reader::stream => match stream_rows {
                Some(stream_rows) => Self {
                    stream_rows,
                    ..self
                },
                None => self,
            },
            Reader::mem => self,
        }
    }

    pub fn write(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            // Write data to standard out
            Self {
                out_path: None,
                format: Format::csv,
                ..
            } if self.wrote_header => self.write_data_to_stdout(),
            // Write header to standard out
            Self {
                out_path: None,
                format: Format::csv,
                ..
            } => {
                self.write_header_to_stdout()?;
                self.wrote_header = true;
                self.write_data_to_stdout()
            }
            // Write csv data to file
            Self {
                out_path: Some(_),
                format: Format::csv,
                ..
            } if self.wrote_header => self.write_data_to_csv(),
            // Write csv header to file
            Self {
                out_path: Some(_),
                format: Format::csv,
                ..
            } => {
                self.write_header_to_csv()?;
                self.wrote_header = true;
                self.write_data_to_csv()
            }
            // Write feather data to file
            Self {
                format: Format::feather,
                ..
            } => self.write_data_to_feather(),
            // Write ndjson data to file
            Self {
                format: Format::ndjson,
                ..
            } => self.write_data_to_ndjson(),
            // Write parquet data to file
            Self {
                format: Format::parquet,
                ..
            } => self.write_data_to_parquet(),
        }
    }

    pub fn write_header_to_csv(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            // spinner
            if let Some(pb) = &self.pb {
                pb.finish_at_current_pos();
            }

            // spinner
            if !self.no_progress {
                self.pb = Some(ProgressBar::new(!0));
            }
            if let Some(pb) = &self.pb {
                pb.set_style(
                    ProgressStyle::default_spinner()
                        .template("[{spinner:.green} {elapsed_precise} | {bytes}] {msg}"),
                );

                let in_f = if let Some(f) = &self.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = &self.out_path {
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

            let file = std::fs::File::create(p)?;
            let mut wtr = csv_crate::WriterBuilder::new().from_writer(file);

            // write header
            let vars: Vec<String> = self
                .batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect();

            // Alternate way to get variable names
            // let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();

            wtr.write_record(&vars)?;
            wtr.flush()?;

            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    pub fn write_header_to_stdout(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(pb) = &self.pb {
            pb.finish_and_clear()
        }

        let mut wtr = csv_crate::WriterBuilder::new().from_writer(stdout());

        // write header
        let vars: Vec<String> = self
            .batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();

        // Alternate way to get variable names
        // let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();

        wtr.write_record(&vars)?;
        wtr.flush()?;

        Ok(())
    }

    pub fn write_data_to_csv(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?;
            if let Some(pb) = &self.pb {
                let pb_f = pb.wrap_write(f);
                let mut wtr = csv_arrow::WriterBuilder::new()
                    .has_headers(false)
                    .build(pb_f);
                wtr.write(&self.batch)?;
            } else {
                let mut wtr = csv_arrow::WriterBuilder::new().has_headers(false).build(f);
                wtr.write(&self.batch)?;
            };

            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    pub fn write_data_to_feather(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            if let Some(pb) = &self.pb {
                let in_f = if let Some(f) = &self.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = &self.out_path {
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

            if !self.wrote_start {
                self.wtr = Some(ReadStatWriter::Feather(FileWriter::try_new(
                    f,
                    &self.schema,
                )?));
            }
            if let Some(wtr) = &mut self.wtr {
                match wtr {
                    ReadStatWriter::Feather(w) => {
                        w.write(&self.batch)?;
                        if self.finish {
                            w.finish()?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        } else {
            Err(From::from(
                "Error writing feather file as output path is set to None",
            ))
        }
    }

    pub fn write_data_to_ndjson(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            if let Some(pb) = &self.pb {
                let in_f = if let Some(f) = &self.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = &self.out_path {
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

            if !self.wrote_start {
                self.wtr = Some(ReadStatWriter::Ndjson(LineDelimitedWriter::new(f)));
            }
            if let Some(wtr) = &mut self.wtr {
                match wtr {
                    ReadStatWriter::Ndjson(w) => {
                        let mut batch = RecordBatch::new_empty(Arc::new(self.schema.clone()));
                        batch.clone_from(&self.batch);
                        w.write_batches(&[batch])?;
                        if self.finish {
                            w.finish()?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        } else {
            Err(From::from(
                "Error writing ndjson file as output path is set to None",
            ))
        }
    }

    pub fn write_data_to_parquet(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &self.out_path {
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            if let Some(pb) = &self.pb {
                let in_f = if let Some(f) = &self.path.file_name() {
                    f.to_string_lossy().bright_red()
                } else {
                    String::from("___").bright_red()
                };

                let out_f = if let Some(p) = &self.out_path {
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

            if !self.wrote_start {
                let props = WriterProperties::builder().build();
                self.wtr = Some(ReadStatWriter::Parquet(ArrowWriter::try_new(
                    f,
                    Arc::new(self.schema.clone()),
                    Some(props),
                )?));
            }
            if let Some(wtr) = &mut self.wtr {
                match wtr {
                    ReadStatWriter::Parquet(w) => {
                        w.write(&self.batch)?;
                        if self.finish {
                            w.close()?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        } else {
            Err(From::from(
                "Error writing parquet file as output path is set to None",
            ))
        }
    }

    pub fn write_data_to_stdout(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(pb) = &self.pb {
            pb.finish_and_clear()
        }

        let mut wtr = csv_arrow::WriterBuilder::new()
            .has_headers(false)
            .build(stdout());
        wtr.write(&self.batch)?;

        Ok(())
    }

    pub fn write_metadata_to_json(&mut self) -> Result<(), Box<dyn Error>> {
        match serde_json::to_string_pretty(&self.metadata) {
            Ok(s) => { println!("{}", s); Ok(()) }
            Err(e) => { Err(From::from(format!("Error converting to json: {}", e))) }
        }
    }

    pub fn write_metadata_to_stdout(&mut self) -> Result<(), Box<dyn Error>> {
        println!(
            "Metadata for the file {}\n",
            self.path.to_string_lossy().bright_yellow()
        );
        println!(
            "{}: {}",
            "Row count".green(),
            self.metadata.row_count.to_formatted_string(&Locale::en)
        );
        println!(
            "{}: {}",
            "Variable count".red(),
            self.metadata.var_count.to_formatted_string(&Locale::en)
        );
        println!("{}: {}", "Table name".blue(), self.metadata.table_name);
        println!("{}: {}", "Table label".cyan(), self.metadata.file_label);
        println!("{}: {}", "File encoding".yellow(), self.metadata.file_encoding);
        println!("{}: {}", "Format version".green(), self.metadata.version);
        println!(
            "{}: {}",
            "Bitness".red(),
            if self.metadata.is64bit == 0 {
                "32-bit"
            } else {
                "64-bit"
            }
        );
        println!("{}: {}", "Creation time".blue(), self.metadata.creation_time);
        println!("{}: {}", "Modified time".cyan(), self.metadata.modified_time);
        println!("{}: {:#?}", "Compression".yellow(), self.metadata.compression);
        println!("{}: {:#?}", "Byte order".green(), self.metadata.endianness);
        println!("{}:", "Variable names".purple());
        for (k, v) in self.metadata.vars.iter() {
            println!(
                "{}: {} {{ type class: {}, type: {}, label: {}, format class: {}, format: {}, arrow data type: {} }}",
                (*k).to_formatted_string(&Locale::en),
                v.var_name.bright_purple(),
                format!("{:#?}", v.var_type_class).bright_green(),
                format!("{:#?}", v.var_type).bright_red(),
                v.var_label.bright_blue(),
                (match &v.var_format_class {
                    Some(f) => match f {
                        ReadStatFormatClass::Date => "Date",
                        ReadStatFormatClass::DateTime |
                        ReadStatFormatClass::DateTimeWithMilliseconds | 
                        ReadStatFormatClass::DateTimeWithMicroseconds |
                        ReadStatFormatClass::DateTimeWithNanoseconds => "DateTime",
                        ReadStatFormatClass::Time => "Time",
                    },
                    None => "",
                })
                .bright_cyan(),
                v.var_format.bright_yellow(),
                self.schema.field(*k as usize).data_type().to_string().bright_green()
            );
        }

        Ok(())
    }
}
