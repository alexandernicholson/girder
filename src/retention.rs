//! Per-prefix retention policy — policy-as-data (plan 0013 §6 TTL hooks).
//!
//! `GirderConfig.retention` is a list of `(key_prefix, ttl_nanos)` rows; the
//! legacy global `retention_nanos` knob folds in as the `""` (match-all)
//! row. Resolution is **longest-prefix wins** (the most specific policy
//! governs a key); duplicate prefixes fold last-entry-wins. A key matching
//! no row is kept forever.
//!
//! This is the SINGLE retention oracle: compaction's per-record retain and
//! the tick-driven groomer both resolve TTLs through [`RetentionPolicy`], so
//! they cannot disagree.

/// Compiled retention rows, longest prefix first.
#[derive(Debug, Clone, Default)]
pub(crate) struct RetentionPolicy {
    /// (prefix, ttl_nanos), sorted longest-prefix-first (ties: lexicographic
    /// — unreachable for matching, distinct equal-length prefixes never both
    /// match one key).
    rows: Vec<(String, i64)>,
}

impl RetentionPolicy {
    /// Compile from the config rows + the legacy global knob (folded in as
    /// the `""` row unless an explicit `""` row overrides it). Duplicate
    /// prefixes: LAST entry wins. Negative TTLs clamp to 0 (expire
    /// immediately).
    pub fn compile(rows: &[(String, i64)], global: Option<i64>) -> RetentionPolicy {
        let mut map: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
        if let Some(ttl) = global {
            map.insert(String::new(), ttl.max(0));
        }
        for (prefix, ttl) in rows {
            map.insert(prefix.clone(), (*ttl).max(0));
        }
        let mut rows: Vec<(String, i64)> = map.into_iter().collect();
        rows.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then(a.0.cmp(&b.0)));
        RetentionPolicy { rows }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The TTL governing `key`: longest matching prefix, `None` = keep forever.
    pub fn ttl_for_key(&self, key: &str) -> Option<i64> {
        self.rows
            .iter()
            .find(|(p, _)| key.starts_with(p.as_str()))
            .map(|(_, ttl)| *ttl)
    }

    /// Expiry cutoff for `key` at `now`: a record with `timestamp < cutoff`
    /// is expired. `None` = no policy matches (keep forever).
    pub fn cutoff_for_key(&self, key: &str, now: i64) -> Option<i64> {
        self.ttl_for_key(key).map(|ttl| now.saturating_sub(ttl))
    }

    /// Zone-level helpers for the groomer. A lexicographic key interval whose
    /// endpoints share a prefix consists entirely of keys sharing it, so a
    /// single row covering both endpoints guarantees EVERY key in the segment
    /// has a policy.
    pub fn covers_range(&self, min_key: &str, max_key: &str) -> bool {
        self.rows
            .iter()
            .any(|(p, _)| min_key.starts_with(p.as_str()) && max_key.starts_with(p.as_str()))
    }

    /// The largest TTL of any row that could govern a key in
    /// `[min_key, max_key]` — the safe bound for "everything here is expired":
    /// if `max_ts < now - max_ttl`, every record is expired under whichever
    /// (longest-prefix) row governs it. A row can govern a key in the range
    /// iff its prefix-interval intersects the range.
    pub fn max_ttl_in_range(&self, min_key: &str, max_key: &str) -> Option<i64> {
        self.rows
            .iter()
            .filter(|(p, _)| prefix_intersects_range(p, min_key, max_key))
            .map(|(_, ttl)| *ttl)
            .max()
    }
}

/// Does the set of keys starting with `prefix` intersect `[min_key, max_key]`
/// (inclusive, lexicographic)? True iff `prefix` is not wholly below or above
/// the range: some key with this prefix can be ≥ min_key and ≤ max_key.
fn prefix_intersects_range(prefix: &str, min_key: &str, max_key: &str) -> bool {
    // Everything with `prefix` is < any key that is > prefix and not an
    // extension of it. Below the range: every extension of prefix < min_key
    // ⇔ prefix < min_key and min_key does not start with prefix... simplest
    // exact check: the prefix interval is [prefix, prefix ++ '\u{10FFFF}'…);
    // intersects iff prefix <= max_key AND (min_key starts with prefix OR
    // prefix >= min_key... ) — use the two-sided test:
    // (a) not entirely above: prefix <= max_key (prefix itself is the
    //     smallest key in its interval);
    // (b) not entirely below: min_key < prefix's interval end ⇔
    //     min_key starts with prefix, or min_key < prefix.
    prefix.as_bytes() <= max_key.as_bytes()
        && (min_key.starts_with(prefix) || min_key.as_bytes() <= prefix.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_prefix_wins_and_global_folds_in() {
        let p = RetentionPolicy::compile(
            &[("s/".to_string(), 100), ("s/tenant1/".to_string(), 1_000)],
            Some(10),
        );
        assert_eq!(p.ttl_for_key("s/tenant1/span"), Some(1_000));
        assert_eq!(p.ttl_for_key("s/other/span"), Some(100));
        assert_eq!(p.ttl_for_key("t/meta"), Some(10)); // global "" row
                                                       // Explicit "" row overrides the global knob (last wins on dups too).
        let p = RetentionPolicy::compile(
            &[
                ("".to_string(), 7),
                ("a/".to_string(), 5),
                ("a/".to_string(), 6),
            ],
            Some(10),
        );
        assert_eq!(p.ttl_for_key("zzz"), Some(7));
        assert_eq!(p.ttl_for_key("a/x"), Some(6), "duplicate prefix: last wins");
        // No rows at all → keep forever.
        let p = RetentionPolicy::compile(&[], None);
        assert!(p.is_empty());
        assert_eq!(p.ttl_for_key("anything"), None);
        // No matching row → keep forever.
        let p = RetentionPolicy::compile(&[("s/".to_string(), 1)], None);
        assert_eq!(p.ttl_for_key("t/meta"), None);
    }

    #[test]
    fn range_helpers() {
        let p = RetentionPolicy::compile(
            &[("s/".to_string(), 100), ("s/t1/".to_string(), 1_000)],
            None,
        );
        assert!(p.covers_range("s/a", "s/z"));
        assert!(!p.covers_range("r/a", "s/z"), "min_key uncovered");
        // Both rows can govern keys inside [s/a, s/z] → max ttl is 1000.
        assert_eq!(p.max_ttl_in_range("s/a", "s/z"), Some(1_000));
        // A range strictly below s/t1/ never touches the specific row.
        assert_eq!(p.max_ttl_in_range("s/a", "s/b"), Some(100));
        // A range above every prefix.
        assert_eq!(p.max_ttl_in_range("t/a", "t/b"), None);
    }

    #[test]
    fn prefix_range_intersection_truth_table() {
        assert!(prefix_intersects_range("s/", "s/a", "s/z"));
        assert!(prefix_intersects_range("s/", "r/", "t/")); // range spans prefix
        assert!(prefix_intersects_range("s/", "s/m", "t/")); // min inside
        assert!(!prefix_intersects_range("s/", "t/a", "t/b")); // wholly below range? no — prefix below
        assert!(!prefix_intersects_range("u/", "s/a", "t/b")); // prefix above range
        assert!(prefix_intersects_range("", "anything", "zzz")); // match-all
    }
}
