# /// script
# requires-python = ">=3.11"
# dependencies = ["playwright==1.54.0", "pyarrow==21.0.0"]
# ///
"""Exercise SAS Explorer uploads and downloads in Chromium, then read them back."""

from __future__ import annotations

import contextlib
import functools
import http.server
import tempfile
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as arrow_csv
import pyarrow.feather as feather
import pyarrow.json as arrow_json
import pyarrow.parquet as parquet
from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[2]
EXPLORER = ROOT / "examples" / "sas-explorer"
INPUT = ROOT / "crates" / "readstat-tests" / "tests" / "data" / "cars.sas7bdat"
FORMATS = ("csv", "ndjson", "parquet", "feather")
REDUCED_COLUMNS = ["Brand", "Model", "CityMPG"]
REDUCED_OFFSET = 10
REDUCED_ROWS = 25
EXPECTED_COLUMNS = [
    "Brand",
    "Model",
    "Minivan",
    "Wagon",
    "Pickup",
    "Automatic",
    "EngineSize",
    "Cylinders",
    "CityMPG",
    "HwyMPG",
    "SUV",
    "AWD",
    "Hybrid",
]


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        pass


@contextlib.contextmanager
def explorer_server():
    handler = functools.partial(QuietHandler, directory=EXPLORER)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def download_exports(destination: Path) -> dict[str, Path]:
    if not (EXPLORER / "readstat_wasm.wasm").is_file():
        raise FileNotFoundError(
            "examples/sas-explorer/readstat_wasm.wasm is missing; copy the release "
            "WASM build into the Explorer directory before running this test"
        )

    paths: dict[str, Path] = {}
    errors: list[str] = []
    with explorer_server() as url, sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)
        page.on("pageerror", lambda error: errors.append(f"page error: {error}"))
        page.on(
            "console",
            lambda message: errors.append(f"console error: {message.text}")
            if message.type == "error"
            else None,
        )

        page.goto(url, wait_until="networkidle")
        picker = page.locator("#filePicker")
        picker.wait_for(state="attached")
        page.wait_for_function("!document.querySelector('#filePicker').disabled")
        picker.set_input_files(INPUT)
        page.locator("#exportPanel").wait_for(state="visible")
        page.wait_for_function("!document.querySelector('#exportButton').disabled")

        for output_format in FORMATS:
            page.locator("#exportFormat").select_option(output_format)
            with page.expect_download() as download_info:
                page.locator("#exportButton").click()
            download = download_info.value
            expected_name = f"cars.{output_format}"
            if download.suggested_filename != expected_name:
                raise AssertionError(
                    f"expected download name {expected_name!r}, got "
                    f"{download.suggested_filename!r}"
                )
            output_path = destination / expected_name
            download.save_as(output_path)
            paths[output_format] = output_path
            page.wait_for_function("!document.querySelector('#exportButton').disabled")

        page.locator("#clearVariables").click()
        for column in REDUCED_COLUMNS:
            page.get_by_label(f"Export {column}").check()
        page.locator("#exportRowStart").fill(str(REDUCED_OFFSET + 1))
        page.locator("#exportRowCount").fill(str(REDUCED_ROWS))

        for output_format in FORMATS:
            page.locator("#exportFormat").select_option(output_format)
            with page.expect_download() as download_info:
                page.locator("#exportButton").click()
            download = download_info.value
            expected_name = f"cars-subset.{output_format}"
            if download.suggested_filename != expected_name:
                raise AssertionError(
                    f"expected download name {expected_name!r}, got "
                    f"{download.suggested_filename!r}"
                )
            output_path = destination / expected_name
            download.save_as(output_path)
            paths[f"reduced_{output_format}"] = output_path
            page.wait_for_function("!document.querySelector('#exportButton').disabled")

        browser.close()

    if errors:
        raise AssertionError("SAS Explorer browser errors:\n" + "\n".join(errors))
    return paths


def read_exports(paths: dict[str, Path]) -> dict[str, pa.Table]:
    tables = {
        "csv": arrow_csv.read_csv(paths["csv"]),
        "ndjson": arrow_json.read_json(paths["ndjson"]),
        "parquet": parquet.read_table(paths["parquet"]),
        "feather": feather.read_table(paths["feather"]),
    }
    tables.update(
        {
            "reduced_csv": arrow_csv.read_csv(paths["reduced_csv"]),
            "reduced_ndjson": arrow_json.read_json(paths["reduced_ndjson"]),
            "reduced_parquet": parquet.read_table(paths["reduced_parquet"]),
            "reduced_feather": feather.read_table(paths["reduced_feather"]),
        }
    )
    return tables


def verify_round_trip(tables: dict[str, pa.Table]) -> None:
    expected_rows = tables["parquet"].to_pylist()
    for output_format in FORMATS:
        table = tables[output_format]
        assert table.num_rows == 1081, (
            f"{output_format}: expected 1,081 rows, got {table.num_rows}"
        )
        assert table.column_names == EXPECTED_COLUMNS, (
            f"{output_format}: unexpected columns {table.column_names}"
        )
        assert table.to_pylist() == expected_rows, (
            f"{output_format}: decoded values differ from the Parquet export"
        )

    expected_reduced = (
        tables["parquet"]
        .select(REDUCED_COLUMNS)
        .slice(REDUCED_OFFSET, REDUCED_ROWS)
        .to_pylist()
    )
    for output_format in FORMATS:
        table = tables[f"reduced_{output_format}"]
        assert table.num_rows == REDUCED_ROWS, (
            f"reduced {output_format}: expected {REDUCED_ROWS} rows, got {table.num_rows}"
        )
        assert table.column_names == REDUCED_COLUMNS, (
            f"reduced {output_format}: unexpected columns {table.column_names}"
        )
        assert table.to_pylist() == expected_reduced, (
            f"reduced {output_format}: decoded values differ from the selected source range"
        )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="sas-explorer-e2e-") as directory:
        paths = download_exports(Path(directory))
        verify_round_trip(read_exports(paths))
    print("SAS Explorer full and reduced E2E exports passed for all four formats")


if __name__ == "__main__":
    main()
