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
}
