%macro create_date_time_datetime_ds(cnt=
                                   ,last_call=
                                   ,out_ds=
                                   ,date_fmt=
                                   ,date_type=);

  %local source_width expected_source expected_value format_name resolved_format;
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

  /* The source list contains concrete default-width SAS format names plus a
     few explicit numeric w.d formats. */
  %let format_name = &date_fmt.;
  %if %index(%superq(format_name), %str(.)) %then %let resolved_format = &format_name.;
  %else %let resolved_format = &format_name..;

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
    
    d_as_d_fmt&cnt._label = "&format_name.";
    d_as_d_fmt&cnt._value = d_as_n;
    
  run;

  %if &last_call. = 1 %then %do;
    data &out_ds.;
    %do i=1 %to &cnt.;
      set __ds&i.;
    %end;
    run;

    data _null_;
      if 0 then set &out_ds. nobs=generated_rows;

      dataset_id = open("&out_ds.");
      if dataset_id = 0 then do;
        put "ERROR: Could not open generated fixture &out_ds..";
        abort cancel;
      end;
      generated_vars = attrn(dataset_id, 'nvars');
      close_result = close(dataset_id);

      if generated_rows ne 1 then do;
        put "ERROR: &out_ds. has an unexpected row count" generated_rows=;
        abort cancel;
      end;

      if generated_vars ne %eval(2 + 2 * &cnt.) then do;
        put "ERROR: &out_ds. has an unexpected variable count" generated_vars=;
        abort cancel;
      end;

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
b8601da date
b8601dn date
date date
day date
ddmmyy date
ddmmyyd date
downame date
dtdate date
dtmonyy date
dtwkdatx date
dtyear date
dtyyqc date
e8601da date
e8601dn date
julday date
julian date
mmddyy date
mmddyyd date
mmyy date
mmyyd date
monname date
month date
monyy date
nengo date
nldate date
nldatecp date
nldatel date
nldatem date
nldatemd date
nldatemdl date
nldatemdm date
nldatemds date
nldatemn date
nldates date
nldatew date
nldatewn date
nldateym date
nldateyml date
nldateymm date
nldateyms date
nldateyq date
nldateyql date
nldateyqm date
nldateyqs date
nldateyr date
nldateyw date
qtr date
qtrr date
weekdatx date
weekday date
year date
yymm date
yymmdd date
yymmddd date
yymmd date
yymon date
yyq date
yyqd date
yyqr date
yyqrd date
yyweeku date
yyweekv date
yyweekw date
b8601lz time
b8601tm time
b8601tx time
b8601tz time
e8601lz time
e8601tm time
e8601tx time
e8601tz time
hhmm time
hour time
mmss time
nldatmtm time
nldatmtz time
nltimap time
nltime time
time time
timeampm time
tod time
time12.3 time
time15.6 time
time18.9 time
b8601dt datetime
b8601dx datetime
b8601dz datetime
b8601lx datetime
dateampm datetime
datetime datetime
e8601dt datetime
e8601dx datetime
e8601dz datetime
e8601lx datetime
mdyampm datetime
nldatm datetime
nldatmap datetime
nldatmcp datetime
nldatmdt datetime
nldatml datetime
nldatmm datetime
nldatmmd datetime
nldatmmdl datetime
nldatmmdm datetime
nldatmmds datetime
nldatmmn datetime
nldatms datetime
nldatmw datetime
nldatmwn datetime
nldatmwz datetime
nldatmym datetime
nldatmyml datetime
nldatmymm datetime
nldatmyms datetime
nldatmyq datetime
nldatmyql datetime
nldatmyqm datetime
nldatmyqs datetime
nldatmyr datetime
nldatmyw datetime
nldatmz datetime
datetime22.3 datetime
datetime25.6 datetime
;;;;
run;

/* Ask SAS for each built-in format's default width instead of duplicating
   width rules in this fixture generator. Abort before creating any fixture if
   a listed format is unavailable in the current SAS installation. */
data ds;
  set ds;
  length lookup_fmt $20 format_type $1 default_width $5;

  lookup_fmt = prxchange('s/[0-9]+(\.[0-9]+)?$//', 1, strip(fmt));
  format_type = fmtinfo(lookup_fmt, 'type');
  default_width = fmtinfo(lookup_fmt, 'defw');

  if format_type ne 'F' or missing(default_width) then do;
    put 'ERROR: Unsupported SAS format in fixture generator' fmt= lookup_fmt=;
    abort cancel;
  end;

  if index(fmt, '.') = 0 then fmt = cats(fmt, default_width);
  drop lookup_fmt format_type default_width;
run;



%let homedir = %sysget(HOME);
libname data "&homedir./data";


/* Dates */
data _null_;
  set ds(where=(dtype='date')) end=lastobs;
  
  out_ds = "data.all_dates";
  call execute(cats('%create_date_time_datetime_ds(cnt=',
                    put(_N_, best32.),
                    ',last_call=', put(lastobs, 1.),
                    ',out_ds=', out_ds,
                    ',date_fmt=', fmt,
                    ',date_type=', dtype,
                    ')'));
run;


/* Times */
data _null_;
  set ds(where=(dtype='time')) end=lastobs;
  
  out_ds = "data.all_times";
  call execute(cats('%create_date_time_datetime_ds(cnt=',
                    put(_N_, best32.),
                    ',last_call=', put(lastobs, 1.),
                    ',out_ds=', out_ds,
                    ',date_fmt=', fmt,
                    ',date_type=', dtype,
                    ')'));
run;


/* Datetimes */
data _null_;
  set ds(where=(dtype='datetime')) end=lastobs;
  
  out_ds = "data.all_datetimes";
  call execute(cats('%create_date_time_datetime_ds(cnt=',
                    put(_N_, best32.),
                    ',last_call=', put(lastobs, 1.),
                    ',out_ds=', out_ds,
                    ',date_fmt=', fmt,
                    ',date_type=', dtype,
                    ')'));
run;
