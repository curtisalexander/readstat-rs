%macro create_rand_ds(ds_name=
                     ,n_obs=
                     ,n_cols_num=
                     ,n_cols_char=
                     ,size_cols_char=
                     );
  
  data &ds_name.(drop=_:);
    array _c {&size_cols_char.} $1.;
    do _i=1 to &n_obs.;
    
      %* char vars;
      %do i=1 %to &n_cols_char.;
      
        if _N_ = 1 then do;
          format c&i. $&size_cols_char..;
        end;

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

%let homedir = %sysget(HOME);
libname data "&homedir./data";

%create_rand_ds(ds_name=data.rand_ds
               ,n_obs=3800000
               ,n_cols_num=50
               ,n_cols_char=60
               ,size_cols_char=15);
               
proc contents data=data.rand_ds;
run;
