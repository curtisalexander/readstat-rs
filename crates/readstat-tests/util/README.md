# Utilities
Utility programs to aid development

### [create_all_types.sas](create_all_types.sas)
- SAS program to create a dataset with all types available in SAS (excluding binary types)
    - integer
    - float
    - character
    - string
    - date
    - datetime
    - time

### [create_date_time_datetime_ds.sas](create_date_time_datetime_ds.sas)
- SAS macro that creates a `sas7bdat` file containing all possible dates, times, and datetimes in the datasets:
    - `all_dates`
    - `all_times`
    - `all_datetimes`

### [create_rand_ds.sas](create_rand_ds.sas)
- SAS macro that creates a `sas7bdat` file containing random data
- Able to specify the following
    - Number of observations (rows)
    - Number of numeric columns (vars)
    - Number of character columns (vars)
    - Size of character columns
- Useful for generating test files

### [create_scientific_notation_ds.sas](create_scientific_notation.sas)
- SAS program to create a dataset with a number that initially threw errors when parsed
- Number in question contains scientific notation

### [create_malformed_utf8_ds.sas](create_malformed_utf8_ds.sas)
- SAS program to create a dataset with string values that get truncated mid-character
- SAS truncates character columns at the byte level, not at character boundaries — when a multi-byte UTF-8 character straddles the column width limit, the stored bytes are invalid UTF-8
- Exercises the lossy UTF-8 fallback added in response to [issue #78](https://github.com/curtisalexander/readstat-rs/issues/78)

### [download_ahs.sh](download_ahs.sh) / [download_ahs.ps1](download_ahs.ps1)
- Scripts to download, unzip, and rename the AHS 2019 National PUF `sas7bdat` file from the US Census Bureau
- Downloads from http://www2.census.gov/programs-surveys/ahs/2019/AHS%202019%20National%20PUF%20v1.1%20Flat%20SAS.zip
- Renames to `_ahs2019n.sas7bdat` and places in `tests/data/`
- The `_` prefix matches the `_*.sas7bdat` pattern in `.gitignore`
- Run from the `util/` directory:
    - Linux/macOS: `./download_ahs.sh`
    - Windows: `.\download_ahs.ps1`
