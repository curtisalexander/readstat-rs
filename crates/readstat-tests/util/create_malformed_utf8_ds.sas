/* Creates a dataset with string values that will be truncated mid-character */
/* by SAS due to short column widths, producing malformed UTF-8 bytes.       */
/*                                                                           */
/* Background: SAS truncates character columns at the byte level, not at     */
/* character boundaries. When a multi-byte UTF-8 character straddles the     */
/* column width limit, SAS stores a partial byte sequence — which is         */
/* invalid UTF-8. This exercises the lossy UTF-8 fallback in readstat-rs.    */
/*                                                                           */
/* See: https://github.com/curtisalexander/readstat-rs/issues/78             */

%let homedir = %sysget(HOME);
libname data "&homedir./data";

options encoding=utf8;

data data.malformed_utf8;

  /* "café" is 5 bytes in UTF-8: c(63) a(61) f(66) é(C3 A9)            */
  /* A $4 column truncates after byte 4, leaving dangling 0xC3         */
  length trunc_cafe $4;

  /* "naïve" is 6 bytes: n(6E) a(61) ï(C3 AF) v(76) e(65)              */
  /* A $3 column truncates after byte 3, leaving dangling 0xC3         */
  length trunc_naive $3;

  /* A column wide enough to hold valid UTF-8 for comparison           */
  length ok_col $20;

  /* obs 1 — truncation expected */
  trunc_cafe = "café";
  trunc_naive = "naïve";
  ok_col = "café";
  output;

  /* obs 2 — pure ASCII, no truncation issues */
  trunc_cafe = "abc";
  trunc_naive = "xy";
  ok_col = "hello";
  output;

run;
