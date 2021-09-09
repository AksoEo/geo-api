use crate::json_get;
use serde_json::Value;
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
    #[error("invalid date")]
    InvalidDate,
    #[error("invalid time")]
    InvalidTime,
    #[error("int parse error: {0}")]
    ParseInt(#[from] ParseIntError),
}

/// WikiData time apparently has a very strange format:
/// `+yyyy-mm-ddThh:mm:ssZ`
///
/// - the `+` may be a `-`
/// - there's always a Z at the end
///
/// Invariant: time string must be ascii
pub fn parse_wikidata_time(
    datetime: &str,
    zone_off: f64,
) -> Result<chrono::DateTime<chrono::FixedOffset>, TimeParseError> {
    let mut datetime_parts = datetime.split('T');
    let date = datetime_parts.next().ok_or(TimeParseError::NoDate)?;
    let time = datetime_parts.next().ok_or(TimeParseError::NoTime)?;

    // skip any negative sign on the year (first character)
    let first_real_dash_idx = date[1..].find('-').ok_or(TimeParseError::NoDateDash)? + 1;

    if date.len() < first_real_dash_idx + 4 {
        return Err(TimeParseError::DateTooShort);
    }

    let date = chrono::NaiveDate::from_ymd_opt(
        date[..first_real_dash_idx].parse::<i32>()?,
        date[first_real_dash_idx + 1..first_real_dash_idx + 3].parse::<u32>()?,
        date[first_real_dash_idx + 4..].parse::<u32>()?,
    )
    .ok_or(TimeParseError::InvalidDate)?;

    // ignore trailing Z and split time by :
    let mut time_parts = time[..time.len() - 1].split(":");

    let time = chrono::NaiveTime::from_hms_opt(
        time_parts
            .next()
            .ok_or(TimeParseError::InvalidTime)?
            .parse::<u32>()?,
        time_parts
            .next()
            .ok_or(TimeParseError::InvalidTime)?
            .parse::<u32>()?,
        time_parts
            .next()
            .ok_or(TimeParseError::InvalidTime)?
            .parse::<u32>()?,
    )
    .ok_or(TimeParseError::InvalidTime)?;

    Ok(chrono::DateTime::from_utc(
        chrono::NaiveDateTime::new(date, time),
        chrono::FixedOffset::east(zone_off as i32 * 60),
    ))
}

pub fn is_object_active(qualifiers: Option<&serde_json::Map<String, Value>>) -> bool {
    let qualifiers = match qualifiers {
        Some(q) => q,
        None => return true, // assume true
    };

    let now = chrono::Utc::now();

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
