%macro create_date_and_datetime_ds(out_ds=
                                  ,date_list=
                                  ,datetime_list=);

  %let date_cnt = %sysfunc(countw(%bquote(&date_list.), %str(|)));
  %let datetime_cnt = %sysfunc(countw(%bquote(&datetime_list.), %str(|)));
  

  data &out_ds. ;
    format /* dates */
           d_as_str $10.
           d_as_d best32.

    %do i = 1 %to &date_cnt.;
      %let d_fmt = %trim(%left(%scan(%bquote(&date_list.), &i., %str(|))));
           d_as_d_fmt&i._label $15.
           d_as_d_fmt&i._value &d_fmt..
    %end;
               
           /* datetimes */
           dt_as_str $22.
           dt_as_dt best32. 
           
    %do i = 1 %to &datetime_cnt.;
      %let dt_fmt = %trim(%left(%scan(%bquote(&datetime_list.), &i., %str(|))));
           dt_as_dt_fmt&i._label $15.
           dt_as_dt_fmt&i._value &dt_fmt..
    %end;
         
    ; %* end format ;
    
    
    %* dates ;
    d_as_str = '2021-01-20';
    d_as_d = input(d_as_str, yymmdd10.);
  
    %do i = 1 %to &date_cnt.;
      %let d_fmt = %trim(%left(%scan(%bquote(&date_list.), &i., %str(|))));
           d_as_d_fmt&i._label = "&d_fmt.";
           d_as_d_fmt&i._value = d_as_d;
    %end;
  
    %* datetimes ;
    dt_as_str = '2021-01-20T13:25:52';
    dt_as_dt = input(dt_as_str, e8601dt.);
  
    %do i = 1 %to &datetime_cnt.;
      %let dt_fmt = %trim(%left(%scan(%bquote(&datetime_list.), &i., %str(|))));
           dt_as_dt_fmt&i._label = "&dt_fmt.";
           dt_as_dt_fmt&i._value = dt_as_dt;
    %end; 
  run;

%mend;

%create_date_and_datetime_ds(out_ds=date_and_datetime_ds
                            ,date_list=%str(yymmdd10
                                           |yymmdd8)
                            ,datetime_list=%str(e8601dt
                                               |b8601dt));