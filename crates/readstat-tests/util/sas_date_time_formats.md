# SAS Date, Time, and Datetime Formats

## **Date Formats**

| Format | Description |
|--------|-------------|
| B8601DAw. | Writes date values by using the ISO 8601 basic notation yyyymmdd |
| B8601DNw. | Writes dates from datetime values by using the ISO 8601 basic notation yyyymmdd |
| DATEw. | Writes date values in the form ddmmmyy, ddmmmyyyy, or dd-mmm-yyyy |
| DAYw. | Writes date values as the day of the month |
| DDMMYYw. | Writes date values in the form ddmm<yy>yy or dd/mm/<yy>yy, where a forward slash is the separator and the year appears as either two or four digits |
| DDMMYYxw. | Writes date values in the form ddmm<yy>yy or dd-mm-yy<yy>, where the x in the format name is a character that represents the special character that separates the day, month, and year |
| DOWNAMEw. | Writes date values as the name of the day of the week |
| DTDATEw. | Expects a datetime value as input and writes date values in the form ddmmmyy or ddmmmyyyy |
| DTMONYYw. | Writes the date part of a datetime value as the month and year in the form mmmyy or mmmmyyyy |
| DTWKDATXw. | Writes the date part of a datetime value as the day of the week and the date in the form day-of-week, dd month-name yy (or yyyy) |
| DTYEARw. | Writes the date part of a datetime value as the year in the form yy or yyyy |
| DTYYQCw. | Writes the date part of a datetime value as the year and the quarter and separates them with a colon (:) |
| E8601DAw. | Writes date values by using the ISO 8601 extended notation yyyy-mm-dd |
| E8601DNw. | Writes dates from SAS datetime values by using the ISO 8601 extended notation yyyy-mm-dd |
| JULDAYw. | Writes date values as the Julian day of the year |
| JULIANw. | Writes date values as Julian dates in the form yyddd or yyyyddd |
| MMDDYYw. | Writes date values in the form mmdd<yy>yy or mm/dd/<yy>yy, where a forward slash is the separator and the year appears as either two or four digits |
| MMDDYYxw. | Writes date values in the form mmdd<yy>yy or mm-dd-<yy>yy, where the x in the format name is a character that represents the special character that separates the month, day, and year |
| MMYYw. | Writes date values in the form mmM<yy>yy, where M is the separator and the year appears as either two or four digits |
| MMYYxw. | Writes date values in the form mm<yy>yy or mm-<yy>yy, where the x in the format name is a character that represents the special character that separates the month and the year |
| MONNAMEw. | Writes date values as the name of the month |
| MONTHw. | Writes date values as the month of the year |
| MONYYw. | Writes date values as the month and the year in the form mmmyy or mmmmyyyy |
| NENGOw. | Writes date values as Japanese dates in the form eyymmdd |
| NLDATEw. | Writes a SAS date value as a date that is appropriate for the current SAS locale |
| NLDATECPw.p | Converts a SAS date value to a locale-sensitive-compact-interval date format |
| NLDATELw. | Writes a SAS date value as a date in the form month, date, year that is appropriate for the current SAS locale |
| NLDATEMw. | Writes a SAS date value as a date in a medium-uniform pattern that is appropriate for the current SAS locale |
| NLDATEMDw. | Writes the SAS date value as the name of the month and the day of the month that is appropriate for the current SAS locale |
| NLDATEMDLw. | Writes a SAS date value as the month and day of the month that is appropriate for the current SAS locale |
| NLDATEMDMw. | Writes a SAS date value as the month and day of the month that is appropriate for the current SAS locale |
| NLDATEMDSw. | Writes a SAS date value as the month and day of the month in a short-uniform pattern that is appropriate for the current SAS locale |
| NLDATEMNw. | Writes a SAS date value as the name of the month that is appropriate for the current SAS locale |
| NLDATESw. | Writes a SAS date value as a date string that is appropriate for the current SAS locale |
| NLDATEWw. | Writes a SAS date value as the date and the day of the week that is appropriate for the current SAS locale |
| NLDATEWNw. | Writes the SAS date value as the day of the week that is appropriate for the current SAS locale |
| NLDATEYMw. | Writes the SAS date value as the year and the name of the month that is appropriate for the current SAS locale |
| NLDATEYMLw. | Writes a SAS date value as the month and year that is appropriate for the current SAS locale |
| NLDATEYMMw. | Writes a SAS date value as the month and year with abbreviations that is appropriate for the current SAS locale |
| NLDATEYMSw. | Writes a SAS date value as a date and year that is appropriate for the current SAS locale |
| NLDATEYQw. | Writes the SAS date value as the year and the quarter that is appropriate for the current SAS locale |
| NLDATEYQLw. | Writes a SAS date value as the year and the year's quarter value (Q1-Q4) using abbreviations that is appropriate for the current SAS locale |
| NLDATEYQMw. | Writes a SAS date value as the year and the year's quarter value (Q1-Q4) using abbreviations that is appropriate for the current SAS locale |
| NLDATEYQSw. | Writes a SAS date value as the year and the year's quarter value (1-4) with numbers and delimiters that is appropriate for the current SAS locale |
| NLDATEYRw. | Writes the SAS date value as the year that is appropriate for the current SAS locale |
| NLDATEYWw. | Writes the SAS date value as the year and the week that is appropriate for the current SAS locale |
| QTRw. | Writes date values as the quarter of the year |
| QTRRw. | Writes date values as the quarter of the year in Roman numerals |
| WEEKDATXw. | Writes date values as the day of the week and date in the form day-of-week, dd month-name yy (or yyyy) |
| WEEKDAYw. | Writes date values as the day of the week |
| YEARw. | Writes date values as the year |
| YYMMw. | Writes date values in the form <yy>yyMmm, where M is a character separator to indicate that the month number follows the M and the year appears as either two or four digits |
| YYMMDDw. | Writes date values in the form yymmdd or <yy>yy-mm-dd, where a dash is the separator and the year appears as either two or four digits |
| YYMMDDxw. | Writes date values in the form yymmdd or <yy>yy-mm-dd, where the x in the format name is a character that represents the special character that separates the year, month, and day |
| YYMMxw. | Writes date values in the form <yy>yymm or <yy>yy-mm. The x in the format name represents the special character that separates the year and the month |
| YYMONw. | Writes date values in the form yymmmm or yyyymmm |
| YYQw. | Writes date values in the form <yy>yyQq, where Q is the separator, the year appears as either two or four digits, and q is the quarter of the year |
| YYQxw. | Writes date values in the form <yy>yyq or <yy>yy-q, where the x in the format name is a character that represents the special character that separates the year and the quarter or the year |
| YYQRw. | Writes date values in the form <yy>yyQqr, where Q is the separator, the year appears as either two or four digits, and qr is the quarter of the year expressed in Roman numerals |
| YYQRxw. | Writes date values in the form <yy>yyqr or <yy>yy-qr, where the x in the format name is a character that represents the special character separates the year and the quarter or the year |
| YYWEEKUw. | Writes a week number in decimal format by using the U algorithm, excluding day-of-the-week information |
| YYWEEKVw. | Writes a week number in decimal format by using the V algorithm, excluding day-of-the-week information |
| YYWEEKWw. | Writes a week number in decimal format by using the W algorithm, excluding the day-of-the-week information |

## **Time Formats**

| Format | Description |
|--------|-------------|
| B8601LZw. | Writes time values as local time by appending a time zone offset difference between the local time and UTC, using the ISO 8601 basic time notation hhmmss+|-hhmm |
| B8601TMw.d | Writes time values by using the ISO 8601 basic notation hhmmss<fffff> |
| B8601TXw. | Adjusts a Coordinated Universal Time (UTC) value to the user's local time. Then, writes the local time by using the ISO 8601 basic time notation hhmmss+|-hhmm |
| B8601TZw. | Adjusts time values to the Coordinated Universal Time (UTC) and writes the time values by using the ISO 8601 basic time notation hhmmss+|-hhmm |
| E8601LZw. | Writes time values as local time, appending the Coordinated Universal Time (UTC) offset for the local SAS session, using the ISO 8601 extended time notation hh:mm:ss+|-hh:mm |
| E8601TMw.d | Writes time values by using the ISO 8601 extended notation hh:mm:ss.ffffff |
| E8601TXw. | Adjusts a Coordinated Universal Time (UTC) value to the user's local time. Then, writes the local time by using the ISO 8601 extended time notation hh:mm:ss+|-hh:mm |
| E8601TZw.d | Adjusts time values to the Coordinated Universal Time (UTC) and writes the time values by using the ISO 8601 extended notation hh:mm:ss.<fff>+|-hh:mm |
| HHMMw.d | Writes time values as hours and minutes in the form hh:mm |
| HOURw.d | Writes time values as hours and decimal fractions of hours |
| MMSSw.d | Writes time values as the number of minutes and seconds since midnight |
| NLDATMTMw. | Writes the time portion of a SAS datetime value as the time of day that is appropriate for the current SAS locale |
| NLDATMTZw. | Writes the time portion of the SAS datetime value as the time of day and time zone that is appropriate for the current SAS locale |
| NLTIMAPw. | Writes a SAS time value as a time value with a.m. or p.m. that is appropriate for the current SAS locale |
| NLTIMEw. | Writes a SAS time value as a time value that is appropriate for the current SAS locale. NLTIME also writes SAS date-time values |
| TIMEw.d | Writes time values as hours, minutes, and seconds in the form hh:mm:ss.ss |
| TIMEAMPMw.d | Writes time and datetime values as hours, minutes, and seconds in the form hh:mm:ss.ss with AM or PM |
| TODw.d | Writes SAS time values and the time portion of SAS datetime values in the form hh:mm:ss.ss |

## **Datetime Formats**

| Format | Description |
|--------|-------------|
| B8601DTw.d | Writes datetime values by using the ISO 8601 basic notation yyyymmddThhmms<ffffff> |
| B8601DXw. | Adjusts a Coordinated Universal Time (UTC) datetime value to the user's local date and time. Then, writes the local date and time by using the ISO 8601 datetime and time zone basic notation yyyymmddThhmmss+hhmm |
| B8601DZw. | Writes datetime values for the zero meridian Coordinated Universal Time (UTC) time by using the ISO 8601 datetime and time zone basic notation yyyymmddThhmmss+0000 |
| B8601LXw. | Writes datetime values as local time by appending a time zone offset difference between the local time and UTC, using the ISO 8601 basic notation yyyymmddThhmmss+|-hhmm |
| DATEAMPMw.d | Writes datetime values in the form ddmmmyy:hh:mm:ss.ss with AM or PM |
| DATETIMEw.d | Writes datetime values in the form ddmmmyy:hh:mm:ss.ss |
| E8601DTw.d | Writes datetime values by using the ISO 8601 extended notation yyyy-mm-ddThh:mm:ss.ffffff |
| E8601DXw. | Adjusts a Coordinated Universal Time (UTC) datetime value to the user's local date and time. Then, writes the local date and time by using the ISO 8601 datetime and time zone extended notation yyyy-mm-ddThh:mm:ss+hh:mm |
| E8601DZw. | Writes datetime values for the zero meridian Coordinated Universal Time (UTC) by using the ISO 8601 datetime and time zone extended notation yyyy-mm-ddThh:mm:ss+00:00 |
| E8601LXw. | Writes datetime values as local time by appending a time zone offset difference between the local time and UTC, using the ISO 8601 extended notation yyyy-mm-ddThh:mm:ss+|-hh:mm |
| MDYAMPMw.d | Writes datetime values in the form mm/dd/<yy>yy hh:mm AM|PM. The year can be either two or four digits |
| NLDATMw. | Writes a SAS datetime value as a date time that is appropriate for the current SAS locale |
| NLDATMAPw. | Writes a SAS datetime value as a datetime with a.m. or p.m that is appropriate for the current SAS locale |
| NLDATMCPw.p | Converts a SAS datetime value to a locale-sensitive-compact datetime format |
| NLDATMDTw. | Writes the SAS datetime value as the name of the month, day of the month and year that is appropriate for the current SAS locale |
| NLDATMLw. | Writes a SAS datetime value as a long representation of the date that is appropriate for the current SAS locale |
| NLDATMMw. | Writes a SAS datetime value as a medium representation of the date that is appropriate for the current SAS locale |
| NLDATMMDw. | Writes the SAS datetime value as the name of the month and the day of the month that is appropriate for the current SAS locale |
| NLDATMMDLw. | Writes a SAS datetime value as the full-length of the month and day of the month that is appropriate for the current SAS locale |
| NLDATMMDMw. | Writes a SAS datetime value as the month and day of the month using abbreviations that is appropriate for the current SAS locale |
| NLDATMMDSw. | Writes a SAS datetime value as the month and day of the month using numbers and delimiters that is appropriate for the current SAS locale |
| NLDATMMNw. | Writes the SAS datetime value as the name of the month that is appropriate for the current SAS locale |
| NLDATMSw. | Writes a SAS datetime value as the short representation of the date that is appropriate for the current SAS locale |
| NLDATMWw. | Writes SAS datetime values as day of the week and the datetime that is appropriate for the current SAS locale |
| NLDATMWNw. | Writes a SAS datetime value as the day of the week that is appropriate for the current SAS locale |
| NLDATMWZw. | Writes SAS datetime values as a day-of-week, datetime, and time zone value that is appropriate for the current SAS locale |
| NLDATMYMw. | Writes the SAS datetime value as the month and year that is appropriate for the current SAS locale |
| NLDATMYMLw. | Writes a SAS datetime value as the month and the year that is appropriate for the current SAS locale |
| NLDATMYMMw. | Writes a SAS datetime value as the month and the year that is appropriate for the current SAS locale |
| NLDATMYMSw. | Writes a SAS datetime value as the month and year with numbers and a delimiter that is appropriate for the current SAS locale |
| NLDATMYQw. | Writes the SAS datetime value as the quarter and the year that is appropriate for the current SAS locale |
| NLDATMYQLw. | Writes a SAS datetime value as the year's quarter value (1-4) and the year that is appropriate for the current SAS locale |
| NLDATMYQMw. | Writes a SAS datetime value as the year's quarter (1-4) and year that is appropriate for the current SAS locale |
| NLDATMYQSw. | Writes a SAS datetime value as the year and the quarter (1-4) using numbers and a delimiter that is appropriate for the current SAS locale |
| NLDATMYRw. | Writes the SAS datetime value as the year that is appropriate for the current SAS locale |
| NLDATMYWw. | Writes the SAS datetime value as the week number and the year that is appropriate for the current SAS locale |
| NLDATMZw. | Writes SAS datetime values as datetime and time zone that is appropriate for the current SAS locale |
