use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

fn main() {
    println!("Converting somedata.sas7bdat to parquet...");

    // First, convert the file using the CLI
    let output = std::process::Command::new("cargo")
        .args(["run", "--release", "-p", "readstat", "--"])
        .args(["data", "tests/data/somedata.sas7bdat"])
        .args(["-o", "/tmp/somedata_verify.parquet", "-f", "parquet"])
        .args(["--overwrite", "--no-progress"])
        .output()
        .expect("Failed to execute conversion");

    if !output.status.success() {
        eprintln!("Conversion failed:");
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        std::process::exit(1);
    }

    println!("Reading parquet file and checking metadata...\n");

    let file = File::open("/tmp/somedata_verify.parquet").expect("Failed to open parquet file");

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).expect("Failed to create parquet reader");

    let schema = builder.schema();

    // Check schema-level metadata (table label)
    println!("=== Schema Metadata ===");
    if schema.metadata().is_empty() {
        println!("  No schema metadata found");
    } else {
        for (key, value) in schema.metadata() {
            println!("  {key}: {value}");
        }
    }

    // Check field metadata (column labels)
    println!("\n=== Field Metadata (Column Labels) ===");
    let expected_labels = vec![
        ("ID", Some("ID Number")),
        ("GP", Some("Intervention Group")),
        ("AGE", Some("Age on Jan 1, 2000")),
        ("TIME1", Some("Baseline")),
        ("TIME2", Some("6 Months")),
        ("TIME3", Some("12 Months")),
        ("TIME4", Some("24 Months")),
        ("STATUS", Some("Socioeconomic Status")),
        ("SEX", None),
        ("GENDER", None),
    ];

    let mut all_correct = true;
    for (col_name, expected_label) in expected_labels {
        let field = schema
            .field_with_name(col_name)
            .unwrap_or_else(|_| panic!("{col_name} field not found"));

        let actual_label = field.metadata().get("label");

        match (expected_label, actual_label) {
            (Some(expected), Some(actual)) if expected == actual => {
                println!("  ✓ {col_name}: \"{actual}\"");
            }
            (Some(expected), Some(actual)) => {
                println!("  ✗ {col_name}: expected \"{expected}\", got \"{actual}\"");
                all_correct = false;
            }
            (Some(expected), None) => {
                println!("  ✗ {col_name}: expected \"{expected}\", but no label found");
                all_correct = false;
            }
            (None, Some(actual)) => {
                println!("  ✗ {col_name}: expected no label, but got \"{actual}\"");
                all_correct = false;
            }
            (None, None) => {
                println!("  ✓ {col_name}: (no label)");
            }
        }
    }

    println!("\n=== Result ===");
    if all_correct {
        println!("✓ All labels are correct!");
    } else {
        println!("✗ Some labels are incorrect");
        std::process::exit(1);
    }
}
