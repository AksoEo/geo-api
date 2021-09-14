use crate::json_get;
use chrono::{Datelike, Timelike};
use serde_json::Value;
use std::fmt;
use std::fmt::Formatter;
use std::num::ParseIntError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TimeParseError {
    #[error("datetime has no date part")]
    NoDate,
    #[error("datetime has no time part")]
    NoTime,
    #[error("date part is too short")]
    DateTooShort,
    #[error("datetime has no date dash")]
    NoDateDash,
    #[error("invalid time")]
    InvalidTime,
    #[error("int parse error: {0}")]
    ParseInt(#[from] ParseIntError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WikiTime {
    pub year: i32,
    pub month: u16,
    pub day: u16,
    pub hour: u16,
    pub minute: u16,
    pub second: u16,
}

impl WikiTime {
    fn add_seconds(&self, seconds: i32) -> Self {
        let mut year = self.year;
        let mut month = self.month as i32;
        let mut day = self.day as i32;
        let mut hour = self.hour as i32;
        let mut minute = self.minute as i32;
        let mut second = self.second as i32 + seconds;

        fn carry(smol: &mut i32, upper: i32, large: &mut i32) {
            while *smol < 0 {
                *large -= 1;
                *smol += upper;
            }
            while *smol >= upper {
                *large += 1;
                *smol -= upper;
            }
        }
        carry(&mut second, 60, &mut minute);
        carry(&mut minute, 60, &mut hour);
        carry(&mut hour, 24, &mut day);
        carry(&mut day, 31, &mut month); // close enough
        carry(&mut month, 12, &mut year);

        WikiTime {
            year,
            month: month as u16,
            day: day as u16,
            hour: hour as u16,
            minute: minute as u16,
            second: second as u16,
        }
    }
    fn now() -> Self {
        let now = chrono::Utc::now();
        WikiTime {
            year: now.year(),
            month: now.month0() as u16,
            day: now.day0() as u16,
            hour: now.hour() as u16,
            minute: now.minute() as u16,
            second: now.second() as u16,
        }
    }
}

impl fmt::Display for WikiTime {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }
}

/// WikiData time apparently has a very strange format:
/// `+yyyy-mm-ddThh:mm:ssZ`
///
/// - the `+` may be a `-`
/// - there's always a Z at the end
///
/// Invariant: time string must be ascii
pub fn parse_wikidata_time(datetime: &str, zone_off: f64) -> Result<WikiTime, TimeParseError> {
    let mut datetime_parts = datetime.split('T');
    let date = datetime_parts.next().ok_or(TimeParseError::NoDate)?;
    let time = datetime_parts.next().ok_or(TimeParseError::NoTime)?;

    // skip any negative sign on the year (first character)
    let first_real_dash_idx = date[1..].find('-').ok_or(TimeParseError::NoDateDash)? + 1;

    if date.len() < first_real_dash_idx + 4 {
        return Err(TimeParseError::DateTooShort);
    }

    // ignore trailing Z and split time by :
    let mut time_parts = time[..time.len() - 1].split(":");

    let wiki_time = WikiTime {
        year: date[..first_real_dash_idx].parse()?,
        month: date[first_real_dash_idx + 1..first_real_dash_idx + 3]
            .parse::<u16>()?
            .saturating_sub(1),
        day: date[first_real_dash_idx + 4..]
            .parse::<u16>()?
            .saturating_sub(1),
        hour: time_parts
            .next()
            .ok_or(TimeParseError::InvalidTime)?
            .parse()?,
        minute: time_parts
            .next()
            .ok_or(TimeParseError::InvalidTime)?
            .parse()?,
        second: time_parts
            .next()
            .ok_or(TimeParseError::InvalidTime)?
            .parse()?,
    };

    Ok(wiki_time.add_seconds(zone_off as i32 * 60))
}

pub fn is_object_active(qualifiers: Option<&serde_json::Map<String, Value>>) -> bool {
    let qualifiers = match qualifiers {
        Some(q) => q,
        None => return true, // assume true
    };

    let now = WikiTime::now();

    // check if it already ended
    if let Some(end) = json_get!((qualifiers).P582[0]: object) {
        if json_get!((end).snaktype: string) == Some("value") {
            if let Some(time) = json_get!((end).datavalue.value: object) {
                if let (Some(datetime), Some(zone)) = (
                    json_get!((time).time: string),
                    json_get!((time).timezone: number),
                ) {
                    if let Ok(time) = parse_wikidata_time(datetime, zone) {
                        if time < now {
                            return false;
                        }
                    }
                }
            }
        }
    }

    // check if it hasn't started yet
    if let Some(end) = json_get!((qualifiers).P580[0]: object) {
        if json_get!((end).snaktype: string) == Some("value") {
            if let Some(time) = json_get!((end).datavalue.value: object) {
                if let (Some(datetime), Some(zone)) = (
                    json_get!((time).time: string),
                    json_get!((time).timezone: number),
                ) {
                    if let Ok(time) = parse_wikidata_time(datetime, zone) {
                        if time > now {
                            return false;
                        }
                    }
                }
            }
        }
    }

    true
}
