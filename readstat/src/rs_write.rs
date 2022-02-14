// Create a writer struct
use std::fs::OpenOptions;
use std::io::stdout;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::error::Error;

use arrow::csv as csv_arrow;
use arrow::ipc::writer::FileWriter;
use csv as csv_crate;
use indicatif::{ProgressBar, ProgressStyle};

use crate::Format;
use crate::rs_data::ReadStatData;
use crate::rs_path::ReadStatPath;

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
                self.wrote_header = true;
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
                self.wrote_header = true;
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

        if let Some(pb) = d.pb {
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
        }
    }


    fn write_data_to_csv(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        if let Some(p) = rsp.out_path {
            let f = if self.wrote_start { OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?;
            } else {
                std::fs::File::create(p)?;
            };

            // set message for what is being read/written
            self.set_message_for_file(&d, &rsp);

            let f = if let Some(pb) = d.pb {
                pb.wrap_write(f)
            } else {
                f
            };
            self.wtr = if !self.wrote_start {
                csv_arrow::WriterBuilder::new()
                    .has_headers(false)
                    .build(f)
            };
            self.wtr.write(&d.batch)?;
            self.wtr.wrote_start = true;

            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_data_to_feather(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        if let Some(p) = rsp.out_path {
            
            // create file
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

            self.wtr = if !self.wrote_start {
                FileWriter::try_new(f, &d.schema)?
            };

            self.wtr.write(&d.batch)?;
            self.wtr.wrote_start = true; 
            if self.finish { self.wtr.finish()? };

        } else {
            Err(From::from(
                "Error writing feather file as output path is set to None",
            ))
        }
    }

    fn write_data_to_ndjson(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
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

    fn write_data_to_parquet(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
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

    fn write_data_to_stdout(&mut self, d: &ReadStatData) -> Result<(), Box<dyn Error>> {
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        self.wtr = csv_arrow::WriterBuilder::new()
            .has_headers(false)
            .build(stdout());
        self.wtr.write(&d.batch)?;

        Ok(())
    }

    fn write_header_to_csv(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
        if let Some(p) = rsp.out_path {
            // spinner
            if let Some(pb) = d.pb {
                pb.finish_at_current_pos();
            }

            // spinner
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
            let vars: Vec<String> = d
                .batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect();

            // Alternate way to get variable names
            // let vars: Vec<String> = self.vars.iter().map(|(k, _)| k.var_name.clone()).collect();

            wtr.write_record(vars)?;
            wtr.flush()?;

            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_header_to_stdout(&mut self, d: &ReadStatData) -> Result<(), Box<dyn Error>> {
        if let Some(pb) = d.pb {
            pb.finish_and_clear()
        }

        self.wtr = csv_crate::WriterBuilder::new().from_writer(stdout());

        // write header
        let vars: Vec<String> = d 
            .batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();

        // Alternate way to get variable names
        // let vars: Vec<String> = d.vars.iter().map(|(k, _)| k.var_name.clone()).collect();

        self.wtr.write_record(vars)?;
        self.wtr.flush()?;

        Ok(())
    }

    fn write_metadata_to_json(&mut self) -> Result<(), Box<dyn Error>> {
        match serde_json::to_string_pretty(&self.metadata) {
            Ok(s) => { println!("{}", s); Ok(()) }
            Err(e) => { Err(From::from(format!("Error converting to json: {}", e))) }
        }
    }

    fn write_metadata_to_stdout(&mut self) -> Result<(), Box<dyn Error>> {
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