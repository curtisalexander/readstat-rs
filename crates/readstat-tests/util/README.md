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

### [create_date_and_datetime_ds.sas](create_date_and_datetime_ds.sas)
- SAS macro that creates a `sas7bdat` file containing various date and datetimes
- Iteration performed in the macro

### [create_date_and_datetime_ds2.sas](create_date_and_datetime_ds2.sas)
- SAS macro that creates a `sas7bdat` file containing various date and datetimes
- Iteration driven by the `data` step

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
