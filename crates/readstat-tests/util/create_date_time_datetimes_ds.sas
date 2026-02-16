%macro create_date_time_datetime_ds(cnt=
                                   ,last_call=
                                   ,out_ds=
                                   ,date_fmt=
                                   ,date_type=);

  data __ds&cnt. ;
    format /* dates */
           d_as_str $10.
           d_as_n best32.
           d_as_d_fmt&cnt._label $15.
           d_as_d_fmt&cnt._value &date_fmt..
    ; %* end format ;
    
    
    %* dates, times, and datetimes ;
    %if &date_type. = %str(date) %then %do;
      d_as_str = '2021-01-20';
      d_as_n = input(d_as_str, yymmdd10.);
    %end;
    
    %if &date_type. = %str(time) %then %do;
      d_as_str = '18:43:54';
      d_as_n = input(d_as_str, time8.);
    %end;
    
    %if &date_type. = %str(datetime) %then %do;
      d_as_str = '20JAN202118:43:54.221';
      d_as_n = input(d_as_str, datetime22.3);
    %end;
    
    d_as_d_fmt&cnt._label = "&date_fmt.";
    d_as_d_fmt&cnt._value = d_as_n;
    
  run;

  %if &last_call. = 1 %then %do;
    data &out_ds.;
    %do i=1 %to &cnt.;
      set __ds&i.;
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
