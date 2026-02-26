#![allow(clippy::float_cmp)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]

use assert_cmd::Command;
use predicates::prelude::*;
use std::path::PathBuf;
use std::sync::OnceLock;

/// Cache the built binary path to avoid rebuilding for each test.
static READSTAT_BIN: OnceLock<PathBuf> = OnceLock::new();

/// Helper function to get the readstat binary command.
/// Uses escargot to build and locate the binary in the workspace (once).
fn readstat_cmd() -> Command {
    let bin_path = READSTAT_BIN.get_or_init(|| {
        let bin = escargot::CargoBuild::new()
            .bin("readstat")
            .current_release()
            .current_target()
            .manifest_path("../readstat-cli/Cargo.toml")
            .run()
            .expect("Failed to build readstat binary");

        bin.path().to_path_buf()
    });

    Command::new(bin_path)
}

#[test]
fn cli_file_does_not_exist() {
    let mut cmd = readstat_cmd();
    cmd.arg("data").arg("tests/data/adataset.sas7bdat");
    cmd.assert().failure().stderr(
        predicate::str::is_match(r"^(Stopping with error: File)\s(.+)\s(does not exist!\n)$")
            .unwrap(),
    );
}
