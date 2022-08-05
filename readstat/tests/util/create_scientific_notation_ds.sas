%let homedir = %sysget(HOME);
libname data "&homedir./data";

data data.float_parsing;
  format note $100.;
         f best32.;

  note = "Parsing error due to scientific notation";
  f = 333039375527f64;
  output;

  note = "No parsing error";
  f = 1234;
  output;

run;