%macro create_rand_ds(ds_name=
                     ,n_obs=
                     ,n_cols_num=
                     ,n_cols_char=
                     ,size_cols_char=
                     ,seed=20260727
                     ,compression=no
                     );

  %if &n_obs. < 1 or &n_cols_num. < 1 or &n_cols_char. < 1 or &size_cols_char. < 1 %then %do;
    %put ERROR: n_obs, n_cols_num, n_cols_char, and size_cols_char must all be positive.;
    %return;
  %end;

  data &ds_name.(compress=&compression. drop=_:);
    array _c {&size_cols_char.} $1.;
    call streaminit(&seed.);

    %do i=1 %to &n_cols_char.;
      length c&i. $&size_cols_char..;
    %end;

    do _i=1 to &n_obs.;

      %* char vars;
      %do i=1 %to &n_cols_char.;

        do _j=1 to &size_cols_char.;
          _c{_j} = byte(rand('Integer', 33, 126));
        end;

        c&i. = cats(of _c1-_c&size_cols_char.);
        call missing(of _c1-_c&size_cols_char.);
      %end;

      %* num vars;
      %do i=1 %to &n_cols_num.;
        n&i. = rand('Normal');
      %end;

      output;
    end;
  run;
%mend;

/*
  Canonical readstat-rs synthetic benchmark, version 1.

  This profile is intentionally tall rather than extremely wide. It complements
  the wide, 64,141-row Census AHS household dataset and exposes repeated-prefix
  work in partitioned readers. Random printable strings are deliberately hard
  to compress. The expected uncompressed row payload is 352 bytes:

      12 numeric columns * 8 bytes + 8 character columns * 32 bytes

  Four million rows should produce roughly 1.31 GiB plus SAS page overhead,
  safely below GitHub Releases' 2 GiB per-asset limit. Confirm the generated
  size before publishing; SAS version, platform, encoding, and page settings can
  affect the exact file.
*/
%let homedir = %sysget(HOME);

data _null_;
  length created $1024;
  if not fileexist("&homedir./data") then do;
    created = dcreate("data", "&homedir.");
    if missing(created) then do;
      putlog "ERROR: Could not create &homedir./data.";
      abort cancel;
    end;
  end;
run;

libname data "&homedir./data";

%let benchmark_rows = 4000000;
%let benchmark_numeric_columns = 12;
%let benchmark_character_columns = 8;
%let benchmark_character_width = 32;
%let benchmark_seed = 20260727;
%let benchmark_compression = no;

%create_rand_ds(ds_name=data.readstat_benchmark_v1
               ,n_obs=&benchmark_rows.
               ,n_cols_num=&benchmark_numeric_columns.
               ,n_cols_char=&benchmark_character_columns.
               ,size_cols_char=&benchmark_character_width.
               ,seed=&benchmark_seed.
               ,compression=&benchmark_compression.);

/*
  Write all information needed to reproduce and identify the corpus. PROC
  PRINTTO accepts a fileref and captures the traditional LISTING output from
  PROC CONTENTS. ODS LISTING is opened explicitly because interactive SAS
  environments such as SAS Studio may have it closed by default.
*/
filename benchmark_manifest
  "&homedir./readstat_benchmark_v1_manifest.txt"
  encoding="utf-8";

ods listing;
proc printto print=benchmark_manifest new;
run;

%macro write_optional_environment;
  %if %symexist(syshostinfolong) %then %do;
    put "Host information: &syshostinfolong.";
  %end;
  %if %symexist(sysaddrbits) %then %do;
    put "Address width: &sysaddrbits. bits";
  %end;
  %if %symexist(sysendian) %then %do;
    put "Endianness: &sysendian.";
  %end;
  %if %symexist(sysncpu) %then %do;
    put "Logical CPU count reported by SAS: &sysncpu.";
  %end;
  %if %symexist(sysprocessmode) %then %do;
    put "SAS process mode: &sysprocessmode.";
  %end;
%mend;

%macro write_artifact_info;
  %if %upcase(%sysfunc(getoption(xcmd))) = XCMD %then %do;
    filename benchmark_artifact pipe
      "LC_ALL=C ls -lh '&homedir./data/readstat_benchmark_v1.sas7bdat' 2>&1 && sha256sum '&homedir./data/readstat_benchmark_v1.sas7bdat' 2>&1"
      lrecl=32767;

    data _null_;
      length line $32767;
      infile benchmark_artifact truncover;
      file print;
      input;
      line = _infile_;
      put line;
    run;

    filename benchmark_artifact clear;
  %end;
  %else %do;
    data _null_;
      file print;
      put "Unavailable: this SAS session was started with the NOXCMD option.";
      put "Run these commands on a Linux host to complete the manifest:";
      put "  ls -lh '&homedir./data/readstat_benchmark_v1.sas7bdat'";
      put "  sha256sum '&homedir./data/readstat_benchmark_v1.sas7bdat'";
    run;
  %end;
%mend;

data _null_;
  file print;
  put "readstat-rs synthetic benchmark manifest";
  put "=========================================";
  put "Generated: %sysfunc(datetime(), e8601dt.)";
  put "Dataset: data.readstat_benchmark_v1";
  put "Dataset path: &homedir./data/readstat_benchmark_v1.sas7bdat";
  put "Manifest path: &homedir./readstat_benchmark_v1_manifest.txt";
  put;
  put "Generator parameters";
  put "--------------------";
  put "Rows: &benchmark_rows.";
  put "Numeric columns: &benchmark_numeric_columns.";
  put "Character columns: &benchmark_character_columns.";
  put "Character width: &benchmark_character_width.";
  put "Random seed: &benchmark_seed.";
  put "Compression: &benchmark_compression.";
  put;
  put "SAS environment";
  put "---------------";
  put "SAS version: &sysvlong4.";
  put "SAS version (short): &sysver.";
  put "Operating system code: &sysscp.";
  put "Operating system description: &sysscpl.";
  %write_optional_environment;
  put "Session encoding: &sysencoding.";
  put "Session locale: %sysfunc(getoption(locale))";
  put;
  put "Artifact size and SHA-256";
  put "------------------------";
run;

%write_artifact_info;

data _null_;
  file print;
  put;
  put "PROC CONTENTS";
  put "=============";
run;

proc contents data=data.readstat_benchmark_v1 varnum;
run;

proc printto;
run;

filename benchmark_manifest clear;

%put NOTE: Benchmark dataset written to &homedir./data/readstat_benchmark_v1.sas7bdat.;
%put NOTE: Benchmark manifest written to &homedir./readstat_benchmark_v1_manifest.txt.;
