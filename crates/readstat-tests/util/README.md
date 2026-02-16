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
