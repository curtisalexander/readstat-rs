use std::fs::OpenOptions;
use std::io::stdout;
use std::sync::{Arc, Mutex};
use std::error::Error;

use arrow::csv as csv_arrow;
use arrow::ipc::writer::FileWriter;
use csv as csv_crate;
use indicatif::{ProgressBar, ProgressStyle};

use crate::Format;
use crate::rs_data::ReadStatData;
use crate::rs_path::ReadStatPath;
use crate::rs_write::ReadStatWriter;

pub fn write_data(d: &ReadStatData, rsp: &ReadStatPath, rsw: &mut ReadStatWriter) -> Result<(), Box<dyn Error>> {
    match rsp {
        // Write data to standard out
        ReadStatPath {
            out_path: None,
            format: Format::csv,
            ..
        } if rsw.wrote_header => write_data_to_stdout(&d, &rsw),
        // Write header and data to standard out
        ReadStatData {
            out_path: None,
            format: Format::csv,
            ..
        } => {
            write_header_to_stdout(&d, &rsw)?;
            rsw.wrote_header = true;
            write_data_to_stdout(&d, &rsw)
        }
        // Write csv data to file
        ReadStatData {
            out_path: Some(_),
            format: Format::csv,
            ..
        } if rsw.wrote_header => write_data_to_csv(&d, &rsp, &rsw),
        // Write csv header to file
        ReadStatData {
            out_path: Some(_),
            format: Format::csv,
            ..
        } => {
            write_header_to_csv(&d, &rsp)?;
            wtr.wrote_header = true;
            write_data_to_csv(&d, &rsp)
        }
        // Write feather data to file
        ReadStatData {
            format: Format::feather,
            ..
        } => write_data_to_feather(&d, &rsp, wrote_start),
        // Write ndjson data to file
        ReadStatData {
            format: Format::ndjson,
            ..
        } => write_data_to_ndjson(),
        // Write parquet data to file
        ReadStatData {
            format: Format::parquet,
            ..
        } => write_data_to_parquet(),
    }
}

pub fn write_header_to_csv(d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error>> {
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

pub fn write_header_to_stdout(d: &ReadStatData, rsw: &mut ReadStatWriter) -> Result<(), Box<dyn Error>> {
    if let Some(pb) = d.pb {
        pb.finish_and_clear()
    }

    rsw.wtr = csv_crate::WriterBuilder::new().from_writer(stdout());

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

    rsw.wtr.write_record(vars)?;
    rsw.wtr.flush()?;

    Ok(())
}

pub fn write_data_to_csv(d: &ReadStatData, rsp: &ReadStatPath, rsw: &mut ReadStatWriter) -> Result<(), Box<dyn Error>> {
    if let Some(p) = rsp.out_path {
        let f = if rsw.wrote_start { OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(p)?;
        } else {
            std::fs::File::create(p)?;
        };

        let f = if let Some(pb) = d.pb {
            pb.wrap_write(f)
        } else {
            f
        };
        rsw.wtr = csv_arrow::WriterBuilder::new()
                .has_headers(false)
                .build(f);
        rsw.wtr.write(&d.batch)?;

        Ok(())
    } else {
        Err(From::from(
            "Error writing csv as output path is set to None",
        ))
    }
}

pub fn set_message_for_file(d: &ReadStatData, rsp: &ReadStatPath)  {

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

pub fn write_data_to_feather(d: &ReadStatData, rsp: &ReadStatPath, wrote_start: Arc<Mutex<bool>>) -> Result<(), Box<dyn Error>> {
    if let Some(p) = rsp.out_path {
        
        // create file
        let f = if wrote_start {
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(p)?
        } else {
            std::fs::File::create(p)?
        };

        // set message for what is being read/written
        set_message_for_file(&d, &rsp);

        // TODO - resume here
        // Need to pull out the writer into something outside of the ReadStatData struct
        // Create its own writer struct and pass into the write functions
        // That struct can also house the various Arc<Mutex>'s
        let wtr = if !wrote_start {
            FileWriter::try_new(f, &d.schema)?
        };
        /*
        if !wrote_start {
            let wtr = Some(ReadStatWriter::Feather(FileWriter::try_new(
                f,
                &self.schema,
            )?));
        }
        */

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

pub fn write_data_to_stdout(d: &ReadStatData, rsw: &mut ReadStatWriter) -> Result<(), Box<dyn Error>> {
    if let Some(pb) = &d.pb {
        pb.finish_and_clear()
    }

    rsw.wtr = csv_arrow::WriterBuilder::new()
        .has_headers(false)
        .build(stdout());
    rsw.wtr.write(&d.batch)?;

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