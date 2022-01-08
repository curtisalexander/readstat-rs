# Utilities
Utility programs to aid development

- `create_all_types.sas`
    - SAS program to create a dataset with all types available in SAS (excluding binary types)
        - integer
        - float
        - character
        - string
        - date
        - datetime
        - time
- `create_rand_ds.sas`
    - SAS macro that creates a `sas7bdat` file containing random data
    - Able to specify the following
        - Number of observations (rows)
        - Number of numeric columns (vars)
        - Number of character columns (vars)
        - Size of character columns
    - Useful for generating test files
- `create_date_and_datetime_ds.sas`
    - SAS macro that creates a `sas7bdat` file containing various date and datetimes
- `create_date_and_datetime_ds2.sas`
    - SAS macro that creates a `sas7bdat` file containing various date and datetimes
