use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use num_traits::FromPrimitive;

use crate::{ReadStatData, ReadStatError, Reader, Format};

pub fn build_offsets(
    reader: &Option<Reader>,
    row_count: u32,
    stream_rows: Option<u32>,
    row_limit: Option<u32>,
) -> Result<Vec<u32>, Box<dyn Error>> {
    // Get other row counts
    let rc = if let Some(r) = row_limit {
        std::cmp::min(r, row_count)
    } else {
        row_count
    };
    let sr = match reader {
        Some(Reader::stream) => match stream_rows {
            Some(s) => s,
            None => rc
        }
        Some(Reader::mem) | None => row_count,
    };

    // Get number of chunks based on row counts above
    let chunks: u32;
    if sr < rc {
        chunks = if rc % sr == 0 { rc / sr } else { (rc / sr) + 1 };
    } else {
        chunks = 1;
    }

    // Allocate and populate a vector for the offsets
    let mut offsets: Vec<u32> = Vec::with_capacity(chunks as usize);

    for c in 0..=chunks {
        if c == 0 {
            offsets.push(0);
        } else if c == chunks {
            offsets.push(rc);
        } else {
            offsets.push(c * sr);
        }
    }

    Ok(offsets)
}

pub fn drive_data_from_offsets(
    d: &mut ReadStatData,
    start: u32,
    end: u32,
) -> Result<(), Box<dyn Error>> {
    // how many rows to process?
    d.batch_rows_to_process = (end - start) as usize;
    d.batch_row_start = start as usize;
    d.batch_row_end = end as usize;

    // drive the iteration
    // get_data creates a parser, setting up the callbacks, and then iterates over the batch
    let error = d.get_data(Some(end - start), Some(start))?;

    match FromPrimitive::from_i32(error as i32) {
        Some(ReadStatError::READSTAT_OK) => { Ok(()) }
        Some(e) => Err(From::from(format!(
            "Error when attempting to parse sas7bdat: {:#?}",
            e
        ))),
        None => Err(From::from(
            "Error when attempting to parse sas7bdat: Unknown return value",
        )),
    }
}

pub fn get_metadata(
    m: &mut ReadStatMetadata,
    skip_row_count: bool,
) -> Result<(), Box<dyn Error>> {
    let error = m.get_metadata(skip_row_count)?;

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

pub fn write_metadata(m: ReadStatMetadata) {
    match FromPrimitive::from_i32(error as i32) {
        Some(ReadStatError::READSTAT_OK) => {
            if !as_json {
                d.write_metadata_to_stdout()
            } else {
                d.write_metadata_to_json()
            }
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

pub fn write(d: ReadStatData, rsp: ReadStatPath) -> Result<(), Box<dyn Error>> {
    match d {
        // Write data to standard out
        ReadStatData {
            out_path: None,
            format: Format::csv,
            ..
        } if self.wrote_header => self.write_data_to_stdout(),
        // Write header to standard out
        ReadStatData {
            out_path: None,
            format: Format::csv,
            ..
        } => {
            self.write_header_to_stdout()?;
            self.wrote_header = true;
            self.write_data_to_stdout()
        }
        // Write csv data to file
        ReadStatData {
            out_path: Some(_),
            format: Format::csv,
            ..
        } if self.wrote_header => self.write_data_to_csv(),
        // Write csv header to file
        ReadStatData {
            out_path: Some(_),
            format: Format::csv,
            ..
        } => {
            self.write_header_to_csv()?;
            self.wrote_header = true;
            self.write_data_to_csv()
        }
        // Write feather data to file
        ReadStatData {
            format: Format::feather,
            ..
        } => self.write_data_to_feather(),
        // Write ndjson data to file
        ReadStatData {
            format: Format::ndjson,
            ..
        } => self.write_data_to_ndjson(),
        // Write parquet data to file
        ReadStatData {
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