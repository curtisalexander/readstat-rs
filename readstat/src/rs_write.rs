// Create a writer struct
use std::fs::OpenOptions;
use std::io::stdout;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::error::Error;

use arrow::csv as csv_arrow;
use arrow::ipc::writer::FileWriter;
use arrow::json::LineDelimitedWriter;
use colored::Colorize;
use csv as csv_crate;
use indicatif::{ProgressBar, ProgressStyle};
use num_format::Locale;
use num_format::ToFormattedString;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::Format;
use crate::ReadStatFormatClass;
use crate::ReadStatMetadata;
use crate::rs_data::ReadStatData;
use crate::rs_path::ReadStatPath;

pub enum ReadStatWriterFormat {
    // csv data to file
    CsvDataToFile(csv_arrow::writer::Writer<std::fs::File>),
    // csv data to stdout
    CsvDataToStdout(csv_arrow::writer::Writer<std::io::Stdout>),
    // csv header to file
    CsvHeaderToFile(csv_crate::Writer<std::fs::File>),
    // csv header to stdout
    CsvHeaderToStdOut(csv_crate::Writer<std::io::Stdout>),
    // feather
    Feather(arrow::ipc::writer::FileWriter<std::fs::File>),
    // ndjson
    Ndjson(arrow::json::writer::LineDelimitedWriter<std::fs::File>),
    // parquet
    Parquet(parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>),
}

pub struct ReadStatWriter {
    pub wtr: Option<ReadStatWriterFormat>,
    pub wrote_header: bool,
    pub wrote_start: bool,
    pub finish: bool
}

impl ReadStatWriter {
    pub fn new() -> Self {
        Self {
            wtr: None,
            wrote_header: false,
            wrote_start: false,
            finish: false,
        }
    }

    pub fn set_finish(&mut self, finish: bool) {
        self.finish = finish;
    }

    pub fn write(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        match rsp {
            // Write data to standard out
            ReadStatPath {
                out_path: None,
                format: Format::csv,
                ..
            } if self.wrote_header => self.write_data_to_stdout(&d),
            // Write header and data to standard out
            ReadStatPath {
                out_path: None,
                format: Format::csv,
                ..
            } => {
                self.write_header_to_stdout(&d)?;
                self.write_data_to_stdout(&d)
            }
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: Format::csv,
                ..
            } if self.wrote_header => self.write_data_to_csv(&d, &rsp),
            // Write csv header to file
            ReadStatPath {
                out_path: Some(_),
                format: Format::csv,
                ..
            } => {
                self.write_header_to_csv(&d, &rsp)?;
                self.write_data_to_csv(&d, &rsp)
            }
            // Write feather data to file
            ReadStatPath {
                format: Format::feather,
                ..
            } => self.write_data_to_feather(&d, &rsp),
            // Write ndjson data to file
            ReadStatPath {
                format: Format::ndjson,
                ..
            } => self.write_data_to_ndjson(&d, &rsp),
            // Write parquet data to file
            ReadStatPath {
                format: Format::parquet,
                ..
            } => self.write_data_to_parquet(&d, &rsp),
        }
    }

    fn set_message_for_file(&mut self, d: &ReadStatData, rsp: &ReadStatPath)  {
        if let Some(pb) = &d.pb {
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

    fn write_data_to_csv(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start { OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            // set message for what is being read/written
            self.set_message_for_file(&d, &rsp);

            // setup writer if not already started writing
            /*
            self.wtr = if !self.wrote_start {
                if let Some(pb) = d.pb {
                    Some(ReadStatWriterFormat::CsvFile(csv_arrow::WriterBuilder::new()
                        .has_headers(false)
                        .build(pb.wrap_write(f))))
                } else {
                    Some(ReadStatWriterFormat::CsvFile(csv_arrow::WriterBuilder::new()
                        .has_headers(false)
                        .build(f)))
                }
            };
            */
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::CsvDataToFile(csv_arrow::WriterBuilder::new()
                    .has_headers(false)
                    .build(f)));
            };

            // write
            if let Some(rswf) = &mut self.wtr {
                match rswf {
                    ReadStatWriterFormat::CsvDataToFile(wtr) => wtr.write(&d.batch)?,
                    _ => unreachable!()
                
                }
            };
            
            // update
            self.wrote_start = true;

            // 📝 no finishing required

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_data_to_feather(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            // set message for what is being read/written
            self.set_message_for_file(&d, &rsp);

            // setup writer if not already started writing
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Feather(FileWriter::try_new(f, &d.schema)?));
            };

            // write
            if let Some(rswf) = &mut self.wtr {
                match rswf {
                    ReadStatWriterFormat::Feather(wtr) => wtr.write(&d.batch)?,
                    _ => unreachable!()
                }
            };

            // update
            self.wrote_start = true; 
            
            // finish
            if self.finish {
                if let Some(rswf) = &mut self.wtr {
                    match rswf {
                        ReadStatWriterFormat::Feather(wtr) => wtr.finish()?,
                        _ => unreachable!()
                    }
                };
            };

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing feather file as output path is set to None",
            ))
        }
    }

    fn write_data_to_ndjson(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            // set message for what is being read/written
            self.set_message_for_file(&d, &rsp);
            
            // setup writer if not already started writing
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Ndjson(LineDelimitedWriter::new(f)));
            };

            // write
            if let Some(rswf) = &mut self.wtr {
                match rswf {
                    // TODO - is a clone necessary here?
                    //   It is necessary for the compiler but there may be another way to accomplish
                    ReadStatWriterFormat::Ndjson(wtr) => wtr.write_batches(&[d.batch.clone()])?,
                    _ => unreachable!()
                }
            };

            // update
            self.wrote_start = true;

            // finish
            if self.finish {
                if let Some(rswf) = &mut self.wtr {
                    match rswf {
                        ReadStatWriterFormat::Ndjson(wtr) => wtr.finish()?,
                        _ => unreachable!()
                    }
                };
            };
            
            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing ndjson file as output path is set to None",
            ))
        }
    }

    fn write_data_to_parquet(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            // set message for what is being read/written
            self.set_message_for_file(&d, &rsp);
            
            // setup writer if not already started writing
            if !self.wrote_start {
                self.wtr = Some(ReadStatWriterFormat::Parquet(ArrowWriter::try_new(
                    f,
                    Arc::new(d.schema.clone()),
                    Some(WriterProperties::builder().build()),
                )?));
            };

            // write
            if let Some(rswf) = &mut self.wtr {
                match rswf {
                    ReadStatWriterFormat::Parquet(wtr) => wtr.write(&d.batch)?,
                    _ => unreachable!()
                }
            };

            // update
            self.wrote_start = true;

            // finish
            if self.finish {
                if let Some(rswf) = &mut self.wtr {
                    match rswf {
                        // need semi-colon in order to return unit type - ()
                        ReadStatWriterFormat::Parquet(wtr) => { wtr.close()?; }
                        _ => unreachable!()
                    }
                };
            };
            
            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing parquet file as output path is set to None",
            ))
        }
    }

    fn write_data_to_stdout(&mut self, d: &ReadStatData) -> Result<(), Box<dyn Error>> {
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        // setup writer
        self.wtr = Some(ReadStatWriterFormat::CsvDataToStdout(csv_arrow::WriterBuilder::new()
            .has_headers(false)
            .build(stdout())));
        
        // write
        if let Some(rswf) = &mut self.wtr {
            match rswf {
                ReadStatWriterFormat::CsvDataToStdout(wtr) => wtr.write(&d.batch)?,
                _ => unreachable!()
            }
        };

        // 📝 no finishing required
        
        // return
        Ok(())
    }

    fn write_header_to_csv(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
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
            let f = std::fs::File::create(p)?;
            
            // setup writer
            self.wtr = Some(ReadStatWriterFormat::CsvHeaderToFile(csv_crate::WriterBuilder::new().from_writer(f)));

            // get variable names for header
            let vars: Vec<String> = d
                .batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect();

            // Alternate way to get variable names
            // let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();

            // write
            if let Some(rswf) = &mut self.wtr {
                match rswf {
                    ReadStatWriterFormat::CsvHeaderToFile(wtr) => {
                        wtr.write_record(vars)?;
                        wtr.flush()?
                    },
                    _ => unreachable!()
                }
            };

            // wrote header
            self.wrote_header = true;

            // 📝 no finishing required

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_header_to_stdout(&mut self, d: &ReadStatData) -> Result<(), Box<dyn Error>> {
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        // setup writer
        self.wtr = Some(ReadStatWriterFormat::CsvHeaderToStdOut(csv_crate::WriterBuilder::new().from_writer(stdout())));

        // get variable names for header
        let vars: Vec<String> = d 
            .batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();

        // Alternate way to get variable names
        // let vars: Vec<String> = d.vars.iter().map(|(k, _)| k.var_name.clone()).collect();

        // write
        if let Some(rswf) = &mut self.wtr {
            match rswf {
                ReadStatWriterFormat::CsvHeaderToStdOut(wtr) => {
                    wtr.write_record(vars)?;
                    wtr.flush()?
                },
                _ => unreachable!()
            }
        };

        // wrote header
        self.wrote_header = true;

        // return
        Ok(())
    }

    pub fn write_metadata(&self, md: &ReadStatMetadata, rsp: &ReadStatPath, as_json: bool) -> Result<(), Box<dyn Error>> {
        if as_json {
            self.write_metadata_to_json(&md)
        } else {
            self.write_metadata_to_stdout(&md, &rsp)
        }
    }

    pub fn write_metadata_to_json(&self, md: &ReadStatMetadata) -> Result<(), Box<dyn Error>> {
        match serde_json::to_string_pretty(md) {
            Ok(s) => { println!("{}", s); Ok(()) }
            Err(e) => { Err(From::from(format!("Error converting to json: {}", e))) }
        }
    }

    pub fn write_metadata_to_stdout(&self, md: &ReadStatMetadata, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        println!(
            "Metadata for the file {}\n",
            rsp.path.to_string_lossy().bright_yellow()
        );
        println!(
            "{}: {}",
            "Row count".green(),
            md.row_count.to_formatted_string(&Locale::en)
        );
        println!(
            "{}: {}",
            "Variable count".red(),
            md.var_count.to_formatted_string(&Locale::en)
        );
        println!("{}: {}", "Table name".blue(), md.table_name);
        println!("{}: {}", "Table label".cyan(), md.file_label);
        println!("{}: {}", "File encoding".yellow(), md.file_encoding);
        println!("{}: {}", "Format version".green(), md.version);
        println!(
            "{}: {}",
            "Bitness".red(),
            if md.is64bit == 0 {
                "32-bit"
            } else {
                "64-bit"
            }
        );
        println!("{}: {}", "Creation time".blue(), md.creation_time);
        println!("{}: {}", "Modified time".cyan(), md.modified_time);
        println!("{}: {:#?}", "Compression".yellow(), md.compression);
        println!("{}: {:#?}", "Byte order".green(), md.endianness);
        println!("{}:", "Variable names".purple());
        for (k, v) in md.vars.iter() {
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
                md.schema.field(*k as usize).data_type().to_string().bright_green()
            );
        }

        Ok(())
    }
}