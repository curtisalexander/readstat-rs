%macro create_date_and_datetime_ds(cnt=
                                  ,last_call=
                                  ,out_ds=
                                  ,date_fmt=);

  data __ds&cnt. ;
    format /* dates */
           d_as_str $10.
           d_as_d best32.
           d_as_d_fmt&cnt._label $15.
           d_as_d_fmt&cnt._value &date_fmt..;
         
    ; %* end format ;
    
    
    %* dates ;
    d_as_str = '2021-01-20';
    d_as_d = input(d_as_str, yymmdd10.);
    d_as_d_fmt&cnt._label = "&date_fmt.";
    d_as_d_fmt&cnt._value = d_as_d;
    
  run;

  %if &last_call = 1 %then %do;
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
input fmt $20.;
datalines4;
b8601da
b8601dn
date
date9
date11
day
ddmmyy
ddmmyyd
downame
e8601da
e8601dn
julday
julian
mmddyy
mmddyyd
mmyy
mmyyd
monname
month
monyy
nengo
nldate
nldatel
nldatem
nldatemd
nldatemdl
nldatemdm
nldatemds
nldatemn
nldates
nldatew
nldatewn
nldateym
nldateyml
nldateymm
nldateyms
nldateyml
nldateymm
nldateyms
nldateyq
nldateyql
nldateyqm
nldateyqs
nldateyr
nldateyw
nldatmdt
nldatmmd
nldatmmdl
nldatmmdm
nldatmmds
nldatmmn
nldatmwz
nldatmyml
nldatmymm
nldatmyms
nldatmyql
nldatmyqm
nldatmyqs
pdjulg
pdjuli
qtr
qtrr
weekdate
weekdatx
weekday
weeku
weekv
worddate
worddatx
year
yymm
yymmdd
yymmdd8
yymmdd10
yymmddd
yymmd
yymon
yyq
yyqd
yyqr
yyqrd
;;;;
run;

%let homedir = %sysget(HOME);
libname data "&homedir./data";

data _null_;
  set ds end=lastobs;
  
  out_ds = "data.dates";
  call execute('%create_date_and_datetime_ds(cnt='||_N_||',last_call='||lastobs||',out_ds='||out_ds||',date_fmt='||fmt||')');
run;