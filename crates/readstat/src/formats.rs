use lazy_static::lazy_static;
use regex::Regex;

use crate::rs_var::ReadStatVarFormatClass;

pub fn match_var_format(v: &str) -> Option<ReadStatVarFormatClass> {
    lazy_static! {
        // DATETIME with nanosecond precision (DATETIMEw.d where d=7-9)
        static ref RE_DATETIME_WITH_NANO: Regex = Regex::new(
            r#"(?xi)
            ^DATETIME[0-9]{1,2}\.[7-9]$
            "#
        )
        .unwrap();

        // DATETIME with microsecond precision (DATETIMEw.d where d=4-6)
        static ref RE_DATETIME_WITH_MICRO: Regex = Regex::new(
            r#"(?xi)
            ^DATETIME[0-9]{1,2}\.[4-6]$
            "#
        )
        .unwrap();

        // DATETIME with millisecond precision (DATETIMEw.d where d=1-3)
        static ref RE_DATETIME_WITH_MILLI: Regex = Regex::new(
            r#"(?xi)
            ^DATETIME[0-9]{1,2}\.[1-3]$
            "#
        )
        .unwrap();

        // All time formats - checked before datetime to catch NLDATMTM and NLDATMTZ
        // Suffix allows letter width/decimal (W, WD) and/or numeric width/decimal (8, 8.2)
        static ref RE_TIME: Regex = Regex::new(
            r#"(?xi)
            ^(
                B8601LZ  |
                B8601TM  |
                B8601TX  |
                B8601TZ  |
                E8601LZ  |
                E8601TM  |
                E8601TX  |
                E8601TZ  |
                HHMM     |
                HOUR     |
                MMSS     |
                NLDATMTM |
                NLDATMTZ |
                NLTIMAP  |
                NLTIME   |
                TIMEAMPM |
                TIME     |
                TOD
            )[A-Z0-9]*(\.[A-Z0-9]*)?$
            "#
        )
        .unwrap();

        // All datetime formats - checked before date to catch DATEAMPM and DATETIME
        // NLDATM matches all NLDATM* variants; NLDATMTM/NLDATMTZ already caught by RE_TIME
        static ref RE_DATETIME: Regex = Regex::new(
            r#"(?xi)
            ^(
                B8601DT  |
                B8601DX  |
                B8601DZ  |
                B8601LX  |
                DATEAMPM |
                DATETIME |
                E8601DT  |
                E8601DX  |
                E8601DZ  |
                E8601LX  |
                MDYAMPM  |
                NLDATM
            )[A-Z0-9]*(\.[A-Z0-9]*)?$
            "#
        )
        .unwrap();

        // All date formats
        static ref RE_DATE: Regex = Regex::new(
            r#"(?xi)
            ^(
                B8601DA   |
                B8601DN   |
                DATE      |
                DAY       |
                DDMMYY    |
                DOWNAME   |
                DTDATE    |
                DTMONXY   |
                DTWKDATX  |
                DTYEAR    |
                DTYYQC    |
                E8601DA   |
                E8601DN   |
                JULDAY    |
                JULIAN    |
                MMDDYY    |
                MMYY      |
                MONNAME   |
                MONTH     |
                MONYY     |
                NENGO     |
                NLDATE    |
                QTRR?     |
                WEEKDATX  |
                WEEKDAY   |
                YEAR      |
                YYMMDD    |
                YYMM      |
                YYMON     |
                YYQR      |
                YYQ       |
                YYWEEK[UVW]
            )[A-Z0-9]*(\.[A-Z0-9]*)?$
            "#
        )
        .unwrap();
    };

    // Check order matters:
    // 1. DATETIME precision variants (most specific, numeric width only)
    // 2. Time (catches NLDATMTM, NLDATMTZ before general NLDATM datetime match)
    // 3. General datetime (catches DATEAMPM, DATETIME before DATE match)
    // 4. Date (everything else)
    if RE_DATETIME_WITH_NANO.is_match(v) {
        Some(ReadStatVarFormatClass::DateTimeWithNanoseconds)
    } else if RE_DATETIME_WITH_MICRO.is_match(v) {
        Some(ReadStatVarFormatClass::DateTimeWithMicroseconds)
    } else if RE_DATETIME_WITH_MILLI.is_match(v) {
        Some(ReadStatVarFormatClass::DateTimeWithMilliseconds)
    } else if RE_TIME.is_match(v) {
        Some(ReadStatVarFormatClass::Time)
    } else if RE_DATETIME.is_match(v) {
        Some(ReadStatVarFormatClass::DateTime)
    } else if RE_DATE.is_match(v) {
        Some(ReadStatVarFormatClass::Date)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Date formats ---

    #[test]
    fn date_formats_with_numeric_width() {
        // Existing formats that were already supported
        assert_eq!(match_var_format("DATE9"), Some(ReadStatVarFormatClass::Date));
        assert_eq!(match_var_format("DDMMYY10"), Some(ReadStatVarFormatClass::Date));
        assert_eq!(match_var_format("DDMMYYB10"), Some(ReadStatVarFormatClass::Date));
        assert_eq!(match_var_format("MMDDYY10"), Some(ReadStatVarFormatClass::Date));
        assert_eq!(match_var_format("YYMMDD10"), Some(ReadStatVarFormatClass::Date));
    }

    #[test]
    fn date_formats_with_letter_width() {
        // Format strings as stored in the test SAS datasets
        let date_formats = [
            "B8601DAW", "B8601DNW", "DATEW", "DAYW", "DDMMYYW", "DDMMYYXW",
            "DOWNAMEW", "DTDATEW", "DTMONXYW", "DTWKDATXW", "DTYEARW", "DTYYQCW",
            "E8601DAW", "E8601DNW", "JULDAYW", "JULIANW", "MMDDYYW", "MMDDYYXW",
            "MMYYW", "MMYYXW", "MONNAMEW", "MONTHW", "MONYYW", "NENGOW",
            "NLDATEW", "NLDATECPWP", "NLDATELW", "NLDATEMW", "NLDATEMDW",
            "NLDATEMDLW", "NLDATEMDMW", "NLDATEMDSW", "NLDATEMNW", "NLDATESW",
            "NLDATEWW", "NLDATEWNW", "NLDATEYMW", "NLDATEYMLW", "NLDATEYMMW",
            "NLDATEYMSW", "NLDATEYQW", "NLDATEYQLW", "NLDATEYQMW", "NLDATEYQSW",
            "NLDATEYRW", "NLDATEYWW", "QTRW", "QTRRW", "WEEKDATXW", "WEEKDAYW",
            "YEARW", "YYMMW", "YYMMDDW", "YYMMDDXW", "YYMMXW", "YYMONW",
            "YYQW", "YYQXW", "YYQRW", "YYQRXW", "YYWEEKUW", "YYWEEKVW",
            "YYWEEKWW",
        ];
        for fmt in &date_formats {
            assert_eq!(
                match_var_format(fmt),
                Some(ReadStatVarFormatClass::Date),
                "Expected Date for format: {}",
                fmt
            );
        }
    }

    // --- Time formats ---

    #[test]
    fn time_format_bare() {
        assert_eq!(match_var_format("TIME"), Some(ReadStatVarFormatClass::Time));
        assert_eq!(match_var_format("TIME8"), Some(ReadStatVarFormatClass::Time));
    }

    #[test]
    fn time_formats_with_letter_width() {
        let time_formats = [
            "B8601LZW", "B8601TMWD", "B8601TXW", "B8601TZW",
            "E8601LZW", "E8601TMWD", "E8601TXW", "E8601TZWD",
            "HHMMWD", "HOURWD", "MMSSWD", "NLDATMTMW", "NLDATMTZW",
            "NLTIMAPW", "NLTIMEW", "TIMEWD", "TIMEAMPMWD", "TODWD",
        ];
        for fmt in &time_formats {
            assert_eq!(
                match_var_format(fmt),
                Some(ReadStatVarFormatClass::Time),
                "Expected Time for format: {}",
                fmt
            );
        }
    }

    // --- Datetime formats ---

    #[test]
    fn datetime_format_with_numeric_width() {
        assert_eq!(match_var_format("DATETIME22"), Some(ReadStatVarFormatClass::DateTime));
    }

    #[test]
    fn datetime_precision_formats() {
        assert_eq!(
            match_var_format("DATETIME22.3"),
            Some(ReadStatVarFormatClass::DateTimeWithMilliseconds)
        );
        assert_eq!(
            match_var_format("DATETIME22.6"),
            Some(ReadStatVarFormatClass::DateTimeWithMicroseconds)
        );
        assert_eq!(
            match_var_format("DATETIME22.9"),
            Some(ReadStatVarFormatClass::DateTimeWithNanoseconds)
        );
    }

    #[test]
    fn datetime_formats_with_letter_width() {
        let datetime_formats = [
            "B8601DTWD", "B8601DXW", "B8601DZW", "B8601LXW",
            "DATEAMPMWD", "DATETIMEWD",
            "E8601DTWD", "E8601DXW", "E8601DZW", "E8601LXW",
            "MDYAMPMWD",
            "NLDATMW", "NLDATMAPW", "NLDATMCPWP", "NLDATMDTW", "NLDATMLW",
            "NLDATMMW", "NLDATMMDW", "NLDATMMDLW", "NLDATMMDMW", "NLDATMMDSW",
            "NLDATMMNW", "NLDATMSW", "NLDATMWW", "NLDATMWNW", "NLDATMWZW",
            "NLDATMYMW", "NLDATMYMLW", "NLDATMYMMW", "NLDATMYMSW",
            "NLDATMYQW", "NLDATMYQLW", "NLDATMYQMW", "NLDATMYQSW",
            "NLDATMYRW", "NLDATMYWW", "NLDATMZW",
        ];
        for fmt in &datetime_formats {
            assert_eq!(
                match_var_format(fmt),
                Some(ReadStatVarFormatClass::DateTime),
                "Expected DateTime for format: {}",
                fmt
            );
        }
    }

    // --- Non-matching formats ---

    #[test]
    fn non_date_time_formats() {
        assert_eq!(match_var_format("BEST12"), None);
        assert_eq!(match_var_format("$30"), None);
        assert_eq!(match_var_format("$10"), None);
        assert_eq!(match_var_format("COMMA12"), None);
        assert_eq!(match_var_format(""), None);
    }

    // --- Case insensitivity ---

    #[test]
    fn case_insensitive() {
        assert_eq!(match_var_format("date9"), Some(ReadStatVarFormatClass::Date));
        assert_eq!(match_var_format("datetime22"), Some(ReadStatVarFormatClass::DateTime));
        assert_eq!(match_var_format("time8"), Some(ReadStatVarFormatClass::Time));
    }
}
