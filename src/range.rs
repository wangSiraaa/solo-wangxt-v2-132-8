//! Parsing and arithmetic for `Range`/`Content-Range` byte ranges.

/// A single, syntactically valid byte interval. The bounds are still raw:
/// open-ended ranges (`bytes=500-`) keep `end = None` and suffix ranges
/// (`bytes=-100`) keep `suffix = Some(100)` until the representation length
/// is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawInterval {
    pub first: u64,
    pub end: Option<u64>,
    pub suffix: Option<u64>,
}

/// The parsed result of a `Range` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeSpec {
    /// One `bytes=` interval.
    Single(RawInterval),
    /// More than one interval. This proxy streams, so multipart/byteranges
    /// is not generated; such a request is forwarded as a raw header.
    Multiple,
}

/// Strictly check `bytes=`. Anything we do not fully understand (unknown
/// unit, garbage, ...) yields `None`; per RFC 9110 a malformed Range is
/// ignored and the full representation is returned with 200.
pub fn parse_range(header: &str) -> Option<RangeSpec> {
    let v = header.trim();
    let body = v.strip_prefix("bytes=")?;
    let mut intervals = Vec::new();
    for part in body.split(',') {
        let p = part.trim();
        if p.is_empty() {
            return None;
        }
        let (a, b) = p.split_once('-')?;
        let a = a.trim();
        let b = b.trim();
        if a.is_empty() {
            // Suffix range: bytes=-N
            if b.is_empty() {
                return None;
            }
            let n: u64 = b.parse().ok()?;
            intervals.push(RawInterval {
                first: 0,
                end: None,
                suffix: Some(n),
            });
        } else {
            let first: u64 = a.parse().ok()?;
            if b.is_empty() {
                intervals.push(RawInterval {
                    first,
                    end: None,
                    suffix: None,
                });
            } else {
                let end: u64 = b.parse().ok()?;
                if end < first {
                    // Syntactically valid but semantically impossible.
                    // Marked so the caller can produce 416 once the length
                    // is known.
                    intervals.push(RawInterval {
                        first,
                        end: Some(end),
                        suffix: None,
                    });
                } else {
                    intervals.push(RawInterval {
                        first,
                        end: Some(end),
                        suffix: None,
                    });
                }
            }
        }
    }
    match intervals.len() {
        0 => None,
        1 => Some(RangeSpec::Single(intervals[0])),
        _ => Some(RangeSpec::Multiple),
    }
}

/// Error returned when a byte interval cannot be satisfied against a known
/// representation length (drives the 416 response path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsatisfiable;

/// Resolve a raw interval against a known complete length.
///
/// Returns:
/// - `Ok((start, end_inclusive))` for a satisfiable clamped interval,
/// - `Err(Unsatisfiable)` when the interval cannot be fulfilled (caller
///   sends 416).
///
/// Suffix ranges are clamped to the length, and a zero-length suffix
/// (`bytes=-0`) is unsatisfiable per RFC 9110 §14.1.2.
pub fn resolve(interval: RawInterval, len: u64) -> Result<(u64, u64), Unsatisfiable> {
    if let Some(n) = interval.suffix {
        if n == 0 {
            return Err(Unsatisfiable);
        }
        if len == 0 {
            return Err(Unsatisfiable);
        }
        let start = len.saturating_sub(n);
        return Ok((start, len - 1));
    }
    if interval.first >= len {
        return Err(Unsatisfiable);
    }
    let end = match interval.end {
        Some(e) => e.min(len - 1),
        None => len - 1,
    };
    if end < interval.first {
        return Err(Unsatisfiable);
    }
    Ok((interval.first, end))
}

/// Parse a `Content-Range: bytes START-END/TOTAL` header.
/// `*` forms (both bytes */TOTAL and bytes START-END/*) are not returned by
/// the scenarios this proxy supports.
pub fn parse_content_range(header: &str) -> Option<(u64, u64, Option<u64>)> {
    let body = header.trim().strip_prefix("bytes")?.trim_start();
    let (range, total) = body.split_once('/')?;
    let (s, e) = range.trim().split_once('-')?;
    let start: u64 = s.trim().parse().ok()?;
    let end: u64 = e.trim().parse().ok()?;
    let total = total.trim();
    if total == "*" {
        Some((start, end, None))
    } else {
        Some((start, end, Some(total.parse().ok()?)))
    }
}

/// `bytes START-END/TOTAL` for a 206 response.
pub fn content_range(start: u64, end: u64, total: u64) -> String {
    format!("bytes {start}-{end}/{total}")
}

/// `bytes */TOTAL` for a 416 response.
pub fn unsatisfiable_content_range(total: u64) -> String {
    format!("bytes */{total}")
}

/// Split a covered interval into the gaps not present in `covered`.
/// Both inputs are half-open `[start, end)`; `covered` must be sorted and
/// merged already.
pub fn missing_within(
    start: u64,
    end: u64,
    covered: &[(u64, u64)],
) -> Vec<(u64, u64)> {
    let mut gaps = Vec::new();
    let mut cursor = start;
    for &(cs, ce) in covered {
        if ce <= cursor {
            continue;
        }
        if cs >= end {
            break;
        }
        if cs > cursor {
            gaps.push((cursor, cs.min(end)));
        }
        cursor = cursor.max(ce);
        if cursor >= end {
            break;
        }
    }
    if cursor < end {
        gaps.push((cursor, end));
    }
    gaps
}

/// Merge an interval into a sorted, disjoint interval list (half-open).
pub fn merge_interval(
    intervals: Vec<(u64, u64)>,
    start: u64,
    end: u64,
) -> Vec<(u64, u64)> {
    if start >= end {
        return intervals;
    }
    let mut out = Vec::with_capacity(intervals.len() + 1);
    let mut cur = (start, end);
    let mut pushed = false;
    for iv in intervals {
        if iv.1 < cur.0 {
            out.push(iv);
        } else if iv.0 > cur.1 {
            if !pushed {
                out.push(cur);
                pushed = true;
            }
            out.push(iv);
        } else {
            cur = (cur.0.min(iv.0), cur.1.max(iv.1));
        }
    }
    if !pushed {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_variants() {
        assert!(matches!(
            parse_range("bytes=0-99"),
            Some(RangeSpec::Single(RawInterval {
                first: 0,
                end: Some(99),
                suffix: None
            }))
        ));
        assert!(matches!(
            parse_range("bytes=100-"),
            Some(RangeSpec::Single(RawInterval {
                first: 100,
                end: None,
                suffix: None
            }))
        ));
        assert!(matches!(
            parse_range("bytes=-50"),
            Some(RangeSpec::Single(RawInterval {
                first: 0,
                end: None,
                suffix: Some(50)
            }))
        ));
        assert!(matches!(parse_range("bytes=0-0,5-9"), Some(RangeSpec::Multiple)));
        // Unknown unit and garbage are ignored (None), not rejected.
        assert_eq!(parse_range("items=0-9"), None);
        assert_eq!(parse_range("bytes=-"), None);
        assert_eq!(parse_range("bytes=abc-def"), None);
        assert_eq!(parse_range("bytes=10-4"), Some(RangeSpec::Single(RawInterval {
            first: 10,
            end: Some(4),
            suffix: None
        })));
    }

    #[test]
    fn resolve_bounds() {
        assert_eq!(resolve(RawInterval { first: 0, end: Some(99), suffix: None }, 300), Ok((0, 99)));
        // Clamped to the end.
        assert_eq!(resolve(RawInterval { first: 290, end: Some(9999), suffix: None }, 300), Ok((290, 299)));
        // Open-ended.
        assert_eq!(resolve(RawInterval { first: 280, end: None, suffix: None }, 300), Ok((280, 299)));
        // Suffix.
        assert_eq!(resolve(RawInterval { first: 0, end: None, suffix: Some(50) }, 300), Ok((250, 299)));
        assert_eq!(resolve(RawInterval { first: 0, end: None, suffix: Some(10_000) }, 300), Ok((0, 299)));
        // Unsatisfiable.
        assert!(resolve(RawInterval { first: 300, end: None, suffix: None }, 300).is_err());
        assert!(resolve(RawInterval { first: 0, end: None, suffix: Some(0) }, 300).is_err());
        assert!(resolve(RawInterval { first: 0, end: None, suffix: Some(10) }, 0).is_err());
        assert!(resolve(RawInterval { first: 10, end: Some(4), suffix: None }, 300).is_err());
    }

    #[test]
    fn gap_arithmetic_and_merge() {
        assert_eq!(missing_within(0, 100, &[]), vec![(0, 100)]);
        assert_eq!(missing_within(10, 90, &[(0, 50)]), vec![(50, 90)]);
        assert_eq!(missing_within(0, 100, &[(0, 100)]), vec![]);
        assert_eq!(
            missing_within(0, 100, &[(0, 20), (30, 60), (80, 100)]),
            vec![(20, 30), (60, 80)]
        );
        // Hole list already sorted/merged.
        assert_eq!(
            merge_interval(vec![(0, 10), (20, 30)], 5, 25),
            vec![(0, 30)]
        );
        assert_eq!(
            merge_interval(vec![(0, 10)], 20, 30),
            vec![(0, 10), (20, 30)]
        );
        assert_eq!(
            merge_interval(vec![(10, 20)], 0, 5),
            vec![(0, 5), (10, 20)]
        );
    }

    #[test]
    fn content_range_round_trip() {
        assert_eq!(content_range(0, 99, 300), "bytes 0-99/300");
        assert_eq!(unsatisfiable_content_range(300), "bytes */300");
        assert_eq!(parse_content_range("bytes 10-19/300"), Some((10, 19, Some(300))));
        assert_eq!(parse_content_range("bytes 10-19/*"), Some((10, 19, None)));
        assert_eq!(parse_content_range("bytes */300"), None);
    }
}
