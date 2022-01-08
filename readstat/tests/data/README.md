# Data
Data for testing [readstat-rs](https://github.com/curtisalexander/readstat-rs) binary

## Sources
- `ahs2019n.sas7bdat` &rarr; US Census data
    - http://www2.census.gov/programs-surveys/ahs/2019/AHS%202019%20National%20PUF%20v1.1%20Flat%20SAS.zip
    - Must be downloaded manually as currently ignored by `git` (i.e. has been added to the repository `.gitignore` file)
    - Renamed to be `_ahs2019n.sas7bdat` in order to be picked up by the `_*.sas7bdat` pattern in the `.gitignore` file
- `all_types.sas7bdat` &rarr; SAS dataset containing all SAS types
    - Created using [create_all_types.sas](../util/create_all_types.sas)
- `cars.sas7bdat` &rarr; SAS cars dataset
    - https://www.alanelliott.com/sas/ED2_FILES.html
- `hasmissing.sas7bdat` &rarr; SAS dataset containing missing values
    - https://www.alanelliott.com/sas/ED2_FILES.html
- `intel.sas7bdat`
    - https://www.alanelliott.com/sas/ED2_FILES.html
- `messydata.sas7bdat`
    - https://www.alanelliott.com/sas/ED2_FILES.html
- `rand_ds.sas7bdat` &rarr; Created using [create_rand_ds.sas](../util/create_rand_ds.sas)
    - Renamed to be `_rand_ds.sas7bdat` in order to be picked up by the `_*.sas7bdat` pattern in the `.gitignore` file
- `rand_ds_largepage_err.sas7bdat` &rarr; Created using [create_rand_ds.sas](../util/create_rand_ds.sas) with [BUFSIZE](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/ledsoptsref/n0pw7cnugsttken1voc6qo0ye3cg.htm) set to `2M`
    - Does not parse with version 1.1.6 of [ReadStat](https://github.com/WizardMac/ReadStat)
- `rand_ds_largepage_ok.sas7bdat` &rarr; Created using [create_rand_ds.sas](../util/create_rand_ds.sas) with [BUFSIZE](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/ledsoptsref/n0pw7cnugsttken1voc6qo0ye3cg.htm) set to `1M`
    - Parses with version 1.1.6 of [ReadStat](https://github.com/WizardMac/ReadStat)
- `scientific_notation.sas7bdat` &rarr; Used to test float parsing
    - Created using [create_scientific_notation_ds.sas](../util/create_scientific_notation_ds.sas)
    - Previously parsed floats with [lexical](https://docs.rs/lexical/latest/lexical/) by first converting to a string `lexical::to_string(value)` and then converting back (after truncating) with `lexical::parse`
    - Had an issue where large floats were being read correctly via ReadStat and the string conversion via `lexical` resulted in a string with scientific notation; after trying to parse back from a string to a float with `lexical::parse` there would be an error thrown
    - Fixed by [d301a9f9ff8c5e3c34a604a16c095e99d205f624](https://github.com/curtisalexander/readstat-rs/commit/d301a9f9ff8c5e3c34a604a16c095e99d205f624)
- `somedata.sas7bdat`
    - https://www.alanelliott.com/sas/ED2_FILES.html
- `somemiss.sas7bdat`
    - https://www.alanelliott.com/sas/ED2_FILES.html