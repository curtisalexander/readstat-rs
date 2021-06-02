# Data
Data for testing [readstat-rs](https://github.com/curtisalexander/readstat-rs) binary


## Sources
- `ahs2019n.sas7bdat` &rarr; http://www2.census.gov/programs-surveys/ahs/2019/AHS%202019%20National%20PUF%20v1.1%20Flat%20SAS.zip
  - Must be downloaded manually as currently ignored by `git` (i.e. has been added to the repository `.gitignore` file)
  - Renamed to be `_ahs2019n.sas7bdat` in order to be picked up by the `_*.sas7bdat` pattern in the `.gitignore` file
- All other `sas7bdat` files &rarr; https://www.alanelliott.com/sas/ED2_FILES.html