//! Minimal parsing of the three HTTP-date shapes (RFC 9110 §5.6.7) and
//! comparison rules used by `If-Range`.
//!
//! Dates are kept as Unix timestamps; this proxy never emits dates itself.

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

const DAYS: [&str; 7] = [
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
];

/// Parse an HTTP-date. Accepts IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`),
/// the obsolete RFC 850 form (`Sunday, 06-Nov-94 08:49:37 GMT`) and the
/// obsolete asctime form (`Sun Nov  6 08:49:37 1994`).
pub fn parse_http_date(s: &str) -> Option<i64> {
    parse_imf(s).or_else(|| parse_rfc850(s)).or_else(|| parse_asctime(s))
}

fn month_num(name: &str) -> Option<u32> {
    MONTHS.iter().position(|m| *m == name).map(|i| (i + 1) as u32)
}

/// Days since the civil epoch (Howard Hinnant's days_from_civil).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn to_unix(year: i64, month: u32, day: u32, h: u32, min: u32, sec: u32) -> i64 {
    (days_from_civil(year, month, day) * 86400) + h as i64 * 3600 + min as i64 * 60 + sec as i64
}

fn parse_time(t: &str) -> Option<(u32, u32, u32)> {
    let mut it = t.split(':');
    let h: u32 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let s: u32 = it.next()?.parse().ok()?;
    if it.next().is_some() {
        return None;
    }
    Some((h, m, s))
}

fn parse_imf(s: &str) -> Option<i64> {
    // Sun, 06 Nov 1994 08:49:37 GMT
    let v = s.trim();
    let v = v.strip_suffix(" GMT")?;
    let (_weekday, rest) = v.split_once(", ")?;
    let mut it = rest.split_whitespace();
    let day: u32 = it.next()?.parse().ok()?;
    let mon = month_num(it.next()?)?;
    let year: i64 = it.next()?.parse().ok()?;
    let (h, m, sec) = parse_time(it.next()?)?;
    if it.next().is_some() {
        return None;
    }
    Some(to_unix(year, mon, day, h, m, sec))
}

fn parse_rfc850(s: &str) -> Option<i64> {
    // Sunday, 06-Nov-94 08:49:37 GMT
    let v = s.trim();
    let v = v.strip_suffix(" GMT")?;
    let (_weekday, rest) = v.split_once(", ")?;
    let mut it = rest.split_whitespace();
    let date = it.next()?;
    let (d, my) = date.split_once('-')?;
    let (mon_s, yy_s) = my.split_once('-')?;
    let day: u32 = d.parse().ok()?;
    let mon = month_num(mon_s)?;
    let yy: i64 = yy_s.parse().ok()?;
    // RFC 9110: two-digit years 0..=49 are 20xx, 50..=99 are 19xx.
    let year = if yy <= 49 { 2000 + yy } else { 1900 + yy };
    let (h, m, sec) = parse_time(it.next()?)?;
    Some(to_unix(year, mon, day, h, m, sec))
}

fn parse_asctime(s: &str) -> Option<i64> {
    // Sun Nov  6 08:49:37 1994
    let v = s.trim();
    let mut it = v.split_whitespace();
    let weekday = it.next()?;
    let wd: String = weekday.chars().take(3).collect();
    if !DAYS.iter().any(|d| d.starts_with(&wd)) {
        return None;
    }
    let mon = month_num(it.next()?)?;
    let day: u32 = it.next()?.parse().ok()?;
    let (h, m, sec) = parse_time(it.next()?)?;
    let year: i64 = it.next()?.parse().ok()?;
    Some(to_unix(year, mon, day, h, m, sec))
}

/// Format a Unix timestamp as an IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`).
pub fn imf_date(ts: i64) -> String {
    let days = ts.div_euclid(86400);
    let rem = ts.rem_euclid(86400);
    let (year, month, day) = civil_from_days(days);
    let weekday = DAYS[(days + 4).rem_euclid(7) as usize]; // 1970-01-01 was Thursday
    let wd3 = &weekday[..3];
    let h = rem / 3600;
    let m = (rem % 3600) / 60;
    let s = rem % 60;
    format!(
        "{wd3}, {day:02} {} {year:04} {h:02}:{m:02}:{s:02} GMT",
        MONTHS[(month - 1) as usize]
    )
}

/// Inverse of [`days_from_civil`] (Howard Hinnant's civil_from_days).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m as u32, d as u32)
}

/// Evaluate an `If-Range` header against the current representation.
///
/// Returns `true` when the client's precondition matches and a 206 may be
/// sent, `false` when the full representation must be sent instead.
///
/// Rules (RFC 9110 §13.1.6):
/// - a weak entity tag in If-Range is *ignored* (weak validators cannot be
///   used for range retrieval);
/// - a strong entity tag matches only by strong comparison;
/// - an HTTP-date matches when the Last-Modified validator is earlier than
///   or equal to the supplied date.
///
/// Anything unparseable counts as a failed precondition (full 200 body).
pub fn if_range_matches(
    if_range: &str,
    current_strong_etag: Option<&crate::etag::ETag>,
    last_modified: Option<i64>,
) -> bool {
    let v = if_range.trim();
    if v.starts_with('"') || v.to_ascii_uppercase().starts_with("W/") {
        match crate::etag::ETag::parse(v) {
            // A weak If-Range validator never matches.
            Some(tag) if !tag.weak => match current_strong_etag {
                Some(cur) => crate::etag::strong_equal(&tag, cur),
                None => false,
            },
            _ => false,
        }
    } else {
        match (parse_http_date(v), last_modified) {
            (Some(provided), Some(lm)) => lm <= provided,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etag::ETag;

    #[test]
    fn parses_all_three_date_forms() {
        // All three describe 1994-11-06 08:49:37 GMT.
        let imf = parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT").unwrap();
        let rfc850 = parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT").unwrap();
        let asctime = parse_http_date("Sun Nov  6 08:49:37 1994").unwrap();
        assert_eq!(imf, rfc850);
        assert_eq!(imf, asctime);
        assert_eq!(imf_date(imf), "Sun, 06 Nov 1994 08:49:37 GMT");
    }

    #[test]
    fn rejects_garbage_dates() {
        assert!(parse_http_date("not a date").is_none());
        assert!(parse_http_date("Sun, 99 Xyz 1994 08:49:37 GMT").is_none());
    }

    #[test]
    fn if_range_etag_rules() {
        let cur = ETag::parse("\"v1\"").unwrap();
        // Strong match -> range allowed.
        assert!(if_range_matches("\"v1\"", Some(&cur), None));
        // Different strong tag -> full representation.
        assert!(!if_range_matches("\"v2\"", Some(&cur), None));
        // Weak If-Range validator can never prove byte identity.
        assert!(!if_range_matches("W/\"v1\"", Some(&cur), None));
        // Unparseable -> precondition fails.
        assert!(!if_range_matches("junk", Some(&cur), None));
    }

    #[test]
    fn if_range_date_rules() {
        // Last-Modified is earlier than the supplied date -> matches.
        let imf = parse_http_date("Wed, 09 Nov 1994 08:49:37 GMT").unwrap();
        let lm = parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT").unwrap();
        assert!(if_range_matches(
            "Wed, 09 Nov 1994 08:49:37 GMT",
            None,
            Some(lm)
        ));
        assert!(if_range_matches(
            "Sun, 06 Nov 1994 08:49:37 GMT",
            None,
            Some(lm)
        ));
        // Supplied date older than Last-Modified -> precondition fails.
        assert!(!if_range_matches(
            "Fri, 04 Nov 1994 00:00:00 GMT",
            None,
            Some(imf)
        ));
    }
}
