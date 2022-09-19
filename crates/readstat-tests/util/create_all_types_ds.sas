/* List of data types in SAS */
/*   - Base SAS ==> https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/lepg/p1gqlhpk5ffltmn1h6iqmzv6mzv7.htm */
/*   - DS2 ==> https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/ds2pg/n0v130wmh3hmuzn1t7y5y4pgxa69.htm */

%let homedir = %sysget(HOME);
libname data "&homedir./data";

data data.all_types;

  format _int best12.
         _float best12.
         _char $1.
         _string $30.
         _date yymmdd10.
         _datetime datetime22.
         _datetime_with_ms datetime22.3
         _time time.
  ;
  
  /* obs 1 */
  _int = 1234;
  _float = 1234.5;
  _char = 's';
  _string = 'string';
  _date = '01JAN2021'd;
  _datetime = '01JAN2021:10:49:39'dt;
  _datetime_with_ms = '01JAN2021:10:49:39.333'dt;
  _time = '02:14:13't;
  output;
  
  /* obs 2 */
  _int = 4567;
  _float = 4567.8;
  _char = 'c';
  _string = 'another string';
  _date = '01JUN2021'd;
  _datetime = '01JUN2021:13:42:25'dt;
  _datetime_with_ms = '01JUN2021:13:42:25.943'dt;
  _time = '19:54:42't;
  output;
  
  /* obs 3 */
  _int = .;
  _float = 910.11;
  _char = '';
  _string = 'stringy string';
  _date = '22MAY2014'd;
  _datetime = .;
  _datetime_with_ms = .;
  _time = '11:04:44't;
  output;

run;