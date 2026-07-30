%macro create_date_time_datetime_ds(cnt=
                                   ,last_call=
                                   ,out_ds=
                                   ,date_fmt=
                                   ,date_type=);

  %local source_width expected_source expected_value resolved_format generated_rows
         dataset_id generated_vars expected_vars close_result;
  %if &date_type. = %str(date) %then %do;
    %let source_width = 10;
    %let expected_source = 2021-01-20;
    %let expected_value = '20JAN2021'd;
  %end;
  %else %if &date_type. = %str(time) %then %do;
    /* A time-of-day is small enough for an 8-byte SAS numeric to retain
       nanoseconds. Keep the expected value independent of the TIME informat. */
    %let source_width = 18;
    %let expected_source = 18:43:54.123456789;
    %let expected_value = hms(18,43,54.123456789);
  %end;
  %else %if &date_type. = %str(datetime) %then %do;
    /* Modern SAS datetimes reliably distinguish microseconds but cannot retain
       arbitrary nanoseconds in a single 8-byte numeric. */
    %let source_width = 25;
    %let expected_source = 20JAN2021:18:43:54.123456;
    %let expected_value = dhms('20JAN2021'd,18,43,54.123456);
  %end;
  %else %do;
    %put ERROR: Unsupported fixture date_type=&date_type..;
    %return;
  %end;

  /* Named placeholder formats need a trailing period; numeric w.d formats
     already contain their separator and must not receive a second period. */
  %if %index(%superq(date_fmt), %str(.)) %then %let resolved_format = &date_fmt.;
  %else %let resolved_format = &date_fmt..;

  data __ds&cnt. ;
    format /* dates */
           d_as_str $&source_width..
           d_as_n best32.
           d_as_d_fmt&cnt._label $15.
           d_as_d_fmt&cnt._value &resolved_format.
    ; %* end format ;
    
    
    %* dates, times, and datetimes ;
    %if &date_type. = %str(date) %then %do;
      d_as_str = '2021-01-20';
      d_as_n = input(d_as_str, yymmdd10.);
    %end;
    
    %if &date_type. = %str(time) %then %do;
      d_as_str = '18:43:54.123456789';
      d_as_n = input(d_as_str, time18.);
    %end;
    
    %if &date_type. = %str(datetime) %then %do;
      d_as_str = '20JAN2021:18:43:54.123456';
      d_as_n = input(d_as_str, datetime25.);
    %end;

    if strip(d_as_str) ne "&expected_source." or
       missing(d_as_n) or d_as_n ne &expected_value. then do;
      put "ERROR: Failed to create &date_type. fixture source value"
          d_as_str= d_as_n=;
      abort cancel;
    end;
    
    d_as_d_fmt&cnt._label = "&date_fmt.";
    d_as_d_fmt&cnt._value = d_as_n;
    
  run;

  %if &last_call. = 1 %then %do;
    data &out_ds.;
    %do i=1 %to &cnt.;
      set __ds&i.;
    %end;
    run;

    proc sql noprint;
      select count(*) into :generated_rows trimmed from &out_ds.;
    quit;

    %if &generated_rows. ne 1 %then %do;
      %put ERROR: &out_ds. contains &generated_rows. rows; expected 1.;
      %abort cancel;
    %end;

    %let dataset_id = %sysfunc(open(&out_ds.));
    %if &dataset_id. = 0 %then %do;
      %put ERROR: Could not open generated fixture &out_ds..;
      %abort cancel;
    %end;
    %let generated_vars = %sysfunc(attrn(&dataset_id., nvars));
    %let close_result = %sysfunc(close(&dataset_id.));
    %let expected_vars = %eval(2 + 2 * &cnt.);
    %if &generated_vars. ne &expected_vars. %then %do;
      %put ERROR: &out_ds. contains &generated_vars. variables; expected &expected_vars..;
      %abort cancel;
    %end;

    data _null_;
      set &out_ds.;

      if strip(d_as_str) ne "&expected_source." or
         missing(d_as_n) or d_as_n ne &expected_value. then do;
        put "ERROR: &out_ds. contains an invalid &date_type. source value"
            d_as_str= d_as_n=;
        abort cancel;
      end;

      %do i=1 %to &cnt.;
        if missing(d_as_d_fmt&i._label) or
           missing(d_as_d_fmt&i._value) or
           d_as_d_fmt&i._value ne &expected_value. then do;
          put "ERROR: &out_ds. contains an invalid value"
              d_as_d_fmt&i._label= d_as_d_fmt&i._value=;
          abort cancel;
        end;
      %end;
    run;
    
    proc datasets lib=work nolist;
    %do i=1 %to &cnt.;
      delete __ds&i.;
    %end;    
    quit;
  %end;

%mend;


data ds;
  input fmt :$20. dtype $20.;
  datalines4;
b8601daw date
b8601dnw date
datew date
dayw date
ddmmyyw date
ddmmyyxw date
downamew date
dtdatew date
dtmonxyw date
dtwkdatxw date
dtyearw date
dtyyqcw date
e8601daw date
e8601dnw date
juldayw date
julianw date
mmddyyw date
mmddyyxw date
mmyyw date
mmyyxw date
monnamew date
monthw date
monyyw date
nengow date
nldatew date
nldatecpwp date
nldatelw date
nldatemw date
nldatemdw date
nldatemdlw date
nldatemdmw date
nldatemdsw date
nldatemnw date
nldatesw date
nldateww date
nldatewnw date
nldateymw date
nldateymlw date
nldateymmw date
nldateymSw date
nldateyqw date
nldateyqlw date
nldateyqmw date
nldateyqsw date
nldateyrw date
nldateyww date
qtrw date
qtrrw date
weekdatxw date
weekdayw date
yearw date
yymmw date
yymmddw date
yymmddxw date
yymmxw date
yymonw date
yyqw date
yyqxw date
yyqrw date
yyqrxw date
yyweekuw date
yyweekvw date
yyweekww date
b8601lzw time
b8601tmwd time
b8601txw time
b8601tzw time
e8601lzw time
e8601tmwd time
e8601txw time
e8601tzwd time
hhmmwd time
hourwd time
mmsswd time
nldatmtmw time
nldatmtzw time
nltimapw time
nltimew time
timewd time
timeampmwd time
todwd time
time12.3 time
time15.6 time
time18.9 time
b8601dtwd datetime
b8601dxw datetime
b8601dzw datetime
b8601lxw datetime
dateampmwd datetime
datetimewd datetime
e8601dtwd datetime
e8601dxw datetime
e8601dzw datetime
e8601lxw datetime
mdyampmwd datetime
nldatmw datetime
nldatmapw datetime
nldatmcpwp datetime
nldatmdtw datetime
nldatmlw datetime
nldatmmw datetime
nldatmmdw datetime
nldatmmdlw datetime
nldatmmdmw datetime
nldatmmdsw datetime
nldatmmnw datetime
nldatmsw datetime
nldatmww datetime
nldatmwnw datetime
nldatmwzw datetime
nldatmymw datetime
nldatmymlw datetime
nldatmymmw datetime
nldatmymsw datetime
nldatmyqw datetime
nldatmyqlw datetime
nldatmyqmw datetime
nldatmyqsw datetime
nldatmyrw datetime
nldatmyww datetime
nldatmzw datetime
datetime22.3 datetime
datetime25.6 datetime
;;;;
run;



%let homedir = %sysget(HOME);
libname data "&homedir./data";


/* Dates */
data _null_;
  set ds(where=(dtype='date')) end=lastobs;
  
  out_ds = "data.all_dates";
  call execute('%create_date_time_datetime_ds(cnt='||_N_||',last_call='||lastobs||',out_ds='||out_ds||',date_fmt='||fmt||',date_type='||dtype||')');
run;


/* Times */
data _null_;
  set ds(where=(dtype='time')) end=lastobs;
  
  out_ds = "data.all_times";
  call execute('%create_date_time_datetime_ds(cnt='||_N_||',last_call='||lastobs||',out_ds='||out_ds||',date_fmt='||fmt||',date_type='||dtype||')');
run;


/* Datetimes */
data _null_;
  set ds(where=(dtype='datetime')) end=lastobs;
  
  out_ds = "data.all_datetimes";
  call execute('%create_date_time_datetime_ds(cnt='||_N_||',last_call='||lastobs||',out_ds='||out_ds||',date_fmt='||fmt||',date_type='||dtype||')');
run;
