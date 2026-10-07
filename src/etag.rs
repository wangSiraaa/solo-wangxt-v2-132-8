//! ETag parsing and the strong/weak distinction from RFC 9110 §8.8.

/// A parsed entity tag. `raw_tag` is stored *without* the surrounding
/// double quotes, matching how the database keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ETag {
    pub weak: bool,
    pub raw_tag: String,
}

impl ETag {
    /// Parse an `ETag` header value. Returns `None` if it is not a single
    /// valid entity-tag (`W/"x"`, `"x"`); garbage such as multiple tags or
    /// unquoted tags is rejected instead of being treated as strong.
    pub fn parse(value: &str) -> Option<ETag> {
        let v = value.trim();
        let (weak, rest) = match v.strip_prefix("W/").or_else(|| v.strip_prefix("w/")) {
            Some(r) => (true, r.trim()),
            None => (false, v),
        };
        let inner = rest.strip_prefix('"')?.strip_suffix('"')?;
        // A valid opaque tag may not contain unescaped control chars;
        // reqwest would not normally send those, but be strict here.
        if inner
            .chars()
            .any(|c| c == '"' || c.is_control())
        {
            return None;
        }
        Some(ETag {
            weak,
            raw_tag: inner.to_string(),
        })
    }

    /// Rebuild the wire form, e.g. `W/"v1"` or `"v1"`.
    pub fn to_wire(&self) -> String {
        if self.weak {
            format!("W/\"{}\"", self.raw_tag)
        } else {
            format!("\"{}\"", self.raw_tag)
        }
    }
}

/// Strong comparison (RFC 9110 §8.8.3.2): both validators MUST be strong and
/// the tags MUST match byte for byte. This is what byte-identical partial
/// content relies on — a weak validator proves nothing about exact bytes and
/// is therefore never accepted.
pub fn strong_equal(a: &ETag, b: &ETag) -> bool {
    !a.weak && !b.weak && a.raw_tag == b.raw_tag
}

/// Why an `If-Match` version-lock value could not be accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockError {
    /// Empty value, `*`, an unquoted/garbage tag — not one entity-tag.
    Malformed,
    /// More than one entity-tag (an If-Match list). A version lock pins one
    /// exact representation, so a list is ambiguous.
    Multiple,
    /// The single tag uses the weak `W/` form and cannot prove byte identity.
    Weak,
}

/// Parse the client's `If-Match` value as a version lock.
///
/// A download pinned to one exact representation accepts **exactly one
/// STRONG entity-tag**. Rejected:
/// - `*` (matches *any* current version — does not pin bytes),
/// - comma-separated tag lists (ambiguous which representation is wanted),
/// - weak `W/"..."` tags (only semantic equivalence, never byte identity),
/// - anything that is not a single well-formed quoted entity-tag.
///
/// Commas are only treated as list separators while outside double quotes,
/// since an opaque tag may legitimately contain a comma.
pub fn parse_if_match_lock(value: &str) -> Result<ETag, LockError> {
    let v = value.trim();
    if v.is_empty() {
        return Err(LockError::Malformed);
    }
    let mut tags: Vec<&str> = Vec::new();
    let mut in_quotes = false;
    let mut start = 0usize;
    for (i, b) in v.bytes().enumerate() {
        match b {
            b'"' => in_quotes = !in_quotes,
            b',' if !in_quotes => {
                tags.push(v[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    tags.push(v[start..].trim());
    if tags.len() > 1 {
        return Err(LockError::Multiple);
    }
    let tag = ETag::parse(tags[0]).ok_or(LockError::Malformed)?;
    if tag.weak {
        Err(LockError::Weak)
    } else {
        Ok(tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strong_and_weak_forms() {
        let s = ETag::parse("\"abc\"").unwrap();
        assert!(!s.weak && s.raw_tag == "abc");
        let w = ETag::parse("W/\"abc\"").unwrap();
        assert!(w.weak && w.raw_tag == "abc");
        // Weak is never strong-equal, even with identical tags.
        assert!(!strong_equal(&s, &w));
        assert!(!strong_equal(&w, &w));
        assert!(strong_equal(&s, &ETag::parse("\"abc\"").unwrap()));
        assert!(!strong_equal(
            &ETag::parse("\"abc\"").unwrap(),
            &ETag::parse("\"abd\"").unwrap()
        ));
        // Garbage is rejected rather than treated as a strong validator.
        assert!(ETag::parse("abc").is_none());
        assert!(ETag::parse("\"a\" \"b\"").is_none());
        assert_eq!(s.to_wire(), "\"abc\"");
        assert_eq!(w.to_wire(), "W/\"abc\"");
    }

    #[test]
    fn if_match_lock_must_be_one_strong_tag() {
        // Exactly one strong tag is the only accepted lock value.
        let lock = parse_if_match_lock("\"alpha-v1\"").unwrap();
        assert!(!lock.weak && lock.raw_tag == "alpha-v1");
        assert!(parse_if_match_lock("  \"v 1\"  ").unwrap().raw_tag == "v 1");
        // A comma inside quotes is part of the tag, not a list separator.
        assert_eq!(
            parse_if_match_lock("\"a,b\"").unwrap().raw_tag,
            "a,b"
        );
        // Weak tags can never pin exact bytes.
        assert_eq!(
            parse_if_match_lock("W/\"v1\""),
            Err(LockError::Weak)
        );
        assert_eq!(
            parse_if_match_lock("w/\"v1\""),
            Err(LockError::Weak)
        );
        // Tag lists do not identify one representation.
        assert_eq!(
            parse_if_match_lock("\"a\", \"b\""),
            Err(LockError::Multiple)
        );
        // The wildcard matches any version, so it is not a lock.
        assert_eq!(parse_if_match_lock("*"), Err(LockError::Malformed));
        // Empty / unquoted / garbage are rejected.
        assert_eq!(parse_if_match_lock(""), Err(LockError::Malformed));
        assert_eq!(parse_if_match_lock("v1"), Err(LockError::Malformed));
        // A list of two values is rejected as multiple, even if one is junk.
        assert_eq!(parse_if_match_lock("\"a\", junk"), Err(LockError::Multiple));
    }
}
