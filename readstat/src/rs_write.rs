use arrow2::{
    array::Array,
    chunk::Chunk,
    error::Error as ArrowError,
    io::{
        parquet::write::RowGroupIterator,
        csv as csv_arrow2,
        ipc as ipc_arrow2,
        ndjson as ndjson_arrow2,
        parquet as parquet_arrow2,
    },
};
use colored::Colorize;
// use indicatif::{ProgressBar, ProgressStyle};
use num_format::Locale;
use num_format::ToFormattedString;
use std::{
    fs::OpenOptions,
    error::Error,
    io::stdout,
};

use crate::OutFormat;
use crate::rs_data::ReadStatData;
use crate::rs_metadata::ReadStatMetadata;
use crate::rs_path::ReadStatPath;
use crate::rs_var::ReadStatVarFormatClass;

#[derive(Default)]
pub struct ReadStatWriter {
    pub wrote_header: bool,
    pub wrote_start: bool,
}

impl ReadStatWriter {
    pub fn new() -> Self {
        Self {
            wrote_header: false,
            wrote_start: false,
        }
    }

    pub fn finish(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>>{
        match rsp {
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } => { self.finish_txt(d, rsp) },
            // Write feather data to file
            ReadStatPath {
                format: OutFormat::feather,
                ..
            } => { self.finish_feather(d, rsp) }
            // Write ndjson data to file
            ReadStatPath {
                format: OutFormat::ndjson,
                ..
            } => { self.finish_txt(d, rsp) }
            // Write parquet data to file
            ReadStatPath {
                format: OutFormat::parquet,
                ..
            } => { self.finish_parquet(d, rsp) },
            _ => Ok(())
        }

    }

    fn _write_message_for_file(&mut self, d: &ReadStatData, rsp: &ReadStatPath)  {
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

    fn write_message_for_rows(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
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

            let rows = d.chunk_rows_processed.to_formatted_string(&Locale::en).truecolor(255, 132, 0);

            let msg = format!("Wrote {} rows from file {} into {}", rows, in_f, out_f);

            println!("{}", msg);
            //pb.set_message(msg);
        //}
            Ok(())
    }

    fn finish_txt(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
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

            let rows = if let Some(trp) = &d.total_rows_processed {
                trp
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .to_formatted_string(&Locale::en)
                    .truecolor(255, 132, 0)
            } else {
                0.to_formatted_string(&Locale::en).truecolor(255, 132, 0)
            };

            let msg = format!("In total, wrote {} rows from file {} into {}", rows, in_f, out_f);

            println!("{}", msg);
            
            //pb.set_message(msg);
        //}
            Ok(())
    }

    pub fn write(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        match rsp {
            // Write data to standard out
            ReadStatPath {
                out_path: None,
                format: OutFormat::csv,
                ..
            } if self.wrote_header => self.write_data_to_stdout(d),
            // Write header and data to standard out
            ReadStatPath {
                out_path: None,
                format: OutFormat::csv,
                ..
            } => {
                self.write_header_to_stdout(d)?;
                self.write_data_to_stdout(d)
            }
            // Write csv data to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } if self.wrote_header => self.write_data_to_csv(d, rsp),
            // Write csv header to file
            ReadStatPath {
                out_path: Some(_),
                format: OutFormat::csv,
                ..
            } => {
                self.write_header_to_csv(d, rsp)?;
                self.write_data_to_csv(d, rsp)
            }
            // Write feather data to file
            ReadStatPath {
                format: OutFormat::feather,
                ..
            } => self.write_data_to_feather(d, rsp),
            // Write ndjson data to file
            ReadStatPath {
                format: OutFormat::ndjson,
                ..
            } => self.write_data_to_ndjson(d, rsp),
            // Write parquet data to file
            ReadStatPath {
                format: OutFormat::parquet,
                ..
            } => self.write_data_to_parquet(d, rsp),
        }
    }

    fn write_data_to_csv(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // if already started writing, then need to append to file; otherwise create file
            let mut f = if self.wrote_start {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(p)?
            } else {
                std::fs::File::create(p)?
            };

            // set message for what is being read/written
            self.write_message_for_rows(d, rsp)?;

            // write
            let options = csv_arrow2::write::SerializeOptions::default();

            if let Some(c) = d.chunk.clone() {
                let cols = &[c];
                cols
                    .iter()
                    .try_for_each(|batch|
                        csv_arrow2::write::write_chunk(&mut f, batch, &options))?;
            };
            
            // update
            self.wrote_start = true;

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing csv as output path is set to None",
            ))
        }
    }

    fn write_data_to_feather(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
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
            self.write_message_for_rows(d, rsp)?;

            // write
            if let Some(c) = d.chunk.clone() {
                let options = ipc_arrow2::write::WriteOptions { compression: None };
                let mut wtr = ipc_arrow2::write::FileWriter::try_new(f, &d.schema, None, options)?;
                wtr.write(&c, None)?;
            };

            // update
            self.wrote_start = true; 

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing feather file as output path is set to None",
            ))
        }
    }

    fn finish_feather(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
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


            // setup writer if not already started writing
            /*
            if !self.wrote_start {
                let options = ipc_arrow2::write::WriteOptions { compression: None };
                let mut wtr = ipc_arrow2::write::FileWriter::try_new(f, &d.schema, None, options)?;
                self.wtr = Some(ReadStatWriterFormat::Feather(wtr));
            };
            */

            // write
            let options = ipc_arrow2::write::WriteOptions { compression: None };
            let mut wtr = ipc_arrow2::write::FileWriter::try_new(f, &d.schema, None, options)?;
            wtr.finish()?;

            // set message for what is being read/written
            self.finish_txt(d, rsp)?;

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing feather file as output path is set to None",
            ))
        }
    }

    fn write_data_to_ndjson(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
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
            self.write_message_for_rows(d, rsp)?;
            
            // write
            if let Some(c) = d.chunk.clone() {
                let arrays = c.columns().iter().map(Ok);
                // let arrays = vec![Ok(c)].into_iter();
                let serializer = ndjson_arrow2::write::Serializer::new(arrays, vec![]);

                let mut wtr = ndjson_arrow2::write::FileWriter::new(f, serializer);
                wtr.by_ref().collect::<Result<(), ArrowError>>()?;
            }

            // update
            self.wrote_start = true;

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing ndjson file as output path is set to None",
            ))
        }
    }

    fn write_data_to_parquet(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(p) = &rsp.out_path {
            // write options
            let options = parquet_arrow2::write::WriteOptions {
                    write_statistics: true,
                    compression: parquet_arrow2::write::CompressionOptions::Snappy,
                    version: parquet_arrow2::write::Version::V2
            };

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
            self.write_message_for_rows(d, rsp)?;
            
            let mut wtr = parquet_arrow2::write::FileWriter::try_new(f, d.schema.clone(), options)?;

            if let Some(c) = d.chunk.clone() {
                let iter: Vec<Result<Chunk<Box<dyn Array>>, ArrowError>> = vec![Ok(c)];

                let encodings: Vec<Vec<parquet_arrow2::write::Encoding>> = d.schema
                    .fields
                    .iter()
                    .map(|f| parquet_arrow2::write::transverse(&f.data_type,  |_| parquet_arrow2::write::Encoding::Plain))
                    .collect();

                let row_groups = RowGroupIterator::try_new(iter.into_iter(), &d.schema, options, encodings)?;

                for group in row_groups {
                    wtr.write(group?)?;
                }
            };

            // update
            self.wrote_start = true;

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing parquet file as output path is set to None",
            ))
        }
    }

    fn finish_parquet(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
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

            // write
            let options = parquet_arrow2::write::WriteOptions {
                    write_statistics: true,
                    compression: parquet_arrow2::write::CompressionOptions::Snappy,
                    version: parquet_arrow2::write::Version::V2
            };
            let mut wtr = parquet_arrow2::write::FileWriter::try_new(f, d.schema.clone(), options)?;
            let _size = wtr.end(None)?;

            // set message for what is being read/written
            self.finish_txt(d, rsp)?;

            // return
            Ok(())
        } else {
            Err(From::from(
                "Error writing parquet file as output path is set to None",
            ))
        }
    }

    fn write_data_to_stdout(&mut self, d: &ReadStatData) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        // write
        let options = csv_arrow2::write::SerializeOptions::default();

        if let Some(c) = d.chunk.clone() {
            let cols = &[c];
            cols
                .iter()
                .try_for_each(|batch|
                    csv_arrow2::write::write_chunk(&mut stdout(), batch, &options))?;
        };

        // update
        self.wrote_start = true;

        // return
        Ok(())
    }

    fn write_header_to_csv(&mut self, d: &ReadStatData, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
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
            let vars: Vec<String> = d.vars.iter().map(|(_, m)| m.var_name.clone()).collect();

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

    fn write_header_to_stdout(&mut self, d: &ReadStatData) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(pb) = &d.pb {
            pb.finish_and_clear()
        }

        // Get variable names
        let vars: Vec<String> = d.vars.iter().map(|(_, m)| m.var_name.clone()).collect();

        // write
        let options = csv_arrow2::write::SerializeOptions::default();
        csv_arrow2::write::write_header(&mut stdout(), &vars, &options)?;

        // wrote header
        self.wrote_header = true;

        // return
        Ok(())
    }

    pub fn write_metadata(&self, md: &ReadStatMetadata, rsp: &ReadStatPath, as_json: bool) -> Result<(), Box<dyn Error + Send + Sync>> {
        if as_json {
            self.write_metadata_to_json(md)
        } else {
            self.write_metadata_to_stdout(md, rsp)
        }
    }

    pub fn write_metadata_to_json(&self, md: &ReadStatMetadata) -> Result<(), Box<dyn Error + Send + Sync>> {
        match serde_json::to_string_pretty(md) {
            Ok(s) => { println!("{}", s); Ok(()) }
            Err(e) => { Err(From::from(format!("Error converting to json: {}", e))) }
        }
    }

    pub fn write_metadata_to_stdout(&self, md: &ReadStatMetadata, rsp: &ReadStatPath) -> Result<(), Box<dyn Error + Send + Sync>> {
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
                "{}: {} {{ type class: {}, type: {}, label: {}, format class: {}, format: {}, arrow logical data type: {}, arrow physical data type: {} }}",
                (*k).to_formatted_string(&Locale::en),
                v.var_name.bright_purple(),
                format!("{:#?}", v.var_type_class).bright_green(),
                format!("{:#?}", v.var_type).bright_red(),
                v.var_label.bright_blue(),
                (match &v.var_format_class {
                    Some(f) => match f {
                        ReadStatVarFormatClass::Date => "Date",
                        ReadStatVarFormatClass::DateTime |
                        ReadStatVarFormatClass::DateTimeWithMilliseconds | 
                        ReadStatVarFormatClass::DateTimeWithMicroseconds |
                        ReadStatVarFormatClass::DateTimeWithNanoseconds => "DateTime",
                        ReadStatVarFormatClass::Time => "Time",
                    },
                    None => "",
                })
                .bright_cyan(),
                v.var_format.bright_yellow(),
                format!("{:#?}", md.schema.fields[*k as usize].data_type().to_logical_type()).bright_green(),
                format!("{:#?}", md.schema.fields[*k as usize].data_type().to_physical_type()).bright_red(),
            );
        }

        Ok(())
    }
}