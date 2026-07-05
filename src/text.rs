//! The FTS tokenizer — Girder's implementation of the NORMATIVE spec defined
//! in `rivet-core::filter::fts_tokens` (plan 0013 §6/§7, rivet memory 0049):
//! lowercase, split on any non-alphanumeric char (Unicode-aware), drop
//! empties. Match semantics are AND-of-tokens: a query matches a document iff
//! every query token appears among the document's tokens (exact token
//! equality, not prefix); a query with no tokens matches nothing.
//!
//! Two repos, one spec: rivet-core's evaluator is the oracle and cannot
//! depend on girder, so the spec is duplicated by construction and pinned by
//! the SAME golden vectors in both repos (`golden_fixtures` below) plus the
//! rivet-store cross-engine conformance test. Do not change one side alone.

/// Tokenize `text` per the normative spec.
pub fn fts_tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// AND-of-tokens match: does `text` contain every token in `want`?
/// `want` empty ⇒ false (no tokens = no match); `text` None ⇒ false.
/// This is the naive evaluation the token index must agree with.
pub fn text_contains_all(text: Option<&str>, want: &[String]) -> bool {
    if want.is_empty() {
        return false;
    }
    let Some(text) = text else {
        return false;
    };
    let have: std::collections::HashSet<String> = fts_tokens(text).into_iter().collect();
    want.iter().all(|t| have.contains(t))
}

/// SQL `LIKE` matcher — Girder's implementation of the NORMATIVE spec defined
/// in `rivet-core::filter::like_match` (rivet memory 0049): `%` = any run
/// (incl. empty), `_` = exactly one char, all other chars literal.
/// Case-sensitive, anchored both ends, no escape syntax (a literal `%`/`_`
/// cannot be matched — documented v1 limitation). Same two-repos-one-spec
/// exception as `fts_tokens`: the spec lives in rivet-core, this duplicate is
/// pinned by `like_match_golden_fixtures` below (to be cross-pinned in
/// rivet-core when the rivet-side pushdown wiring lands).
pub fn like_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    // Two-pointer wildcard match with backtracking to the last `%`.
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None; // (pattern idx after %, text idx it consumed to)
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some((pi + 1, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            // Let the last `%` swallow one more char and retry.
            pi = sp;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GOLDEN FIXTURES — byte-for-byte the vectors pinned in
    /// rivet-core `filter::tests::fts_tokenizer_golden_fixtures`.
    /// A drift here is a cross-repo contract break.
    #[test]
    fn golden_fixtures() {
        let cases: &[(&str, &[&str])] = &[
            ("Hello, World!", &["hello", "world"]),
            ("gpt-4o answered: 42.", &["gpt", "4o", "answered", "42"]),
            (
                "  multiple   spaces\tand\nnewlines ",
                &["multiple", "spaces", "and", "newlines"],
            ),
            (
                "llm.token_count.total=1200",
                &["llm", "token", "count", "total", "1200"],
            ),
            ("CamelCaseStaysOneToken", &["camelcasestaysonetoken"]),
            ("Ünïcode Café naïve", &["ünïcode", "café", "naïve"]),
            ("!!!", &[]),
            ("", &[]),
            ("a", &["a"]),
            ("don't stop", &["don", "t", "stop"]),
        ];
        for (input, want) in cases {
            let got = fts_tokens(input);
            let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
            assert_eq!(&got, &want, "tokenizer drifted for {input:?}");
        }
    }

    /// GOLDEN FIXTURES for `like_match` — the cross-repo pin for the SQL
    /// LIKE spec (rivet-core `filter::like_match` is the normative oracle;
    /// these vectors are to be duplicated there when the rivet-side pushdown
    /// wiring lands). A drift here is a cross-repo contract break.
    #[test]
    fn like_match_golden_fixtures() {
        let cases: &[(&str, &str, bool)] = &[
            // Anchoring: both ends, always.
            ("abc", "abc", true),
            ("abc", "abcd", false),
            ("abc", "xabc", false),
            ("abc%", "abcd", true),
            ("%abc", "xabc", true),
            ("%abc%", "xxabcyy", true),
            ("%abc%", "ab c", false),
            // `%` matches the empty run.
            ("%", "", true),
            ("a%", "a", true),
            ("%a", "a", true),
            ("a%b", "ab", true),
            ("a%b", "aXXb", true),
            ("a%b", "aXbX", false),
            // Empty pattern matches only empty text.
            ("", "", true),
            ("", "a", false),
            // `_` is exactly one char (any char, including `%`-ish literals).
            ("_", "a", true),
            ("_", "", false),
            ("_", "ab", false),
            ("a_c", "abc", true),
            ("a_c", "ac", false),
            ("__", "ab", true),
            ("_%", "a", true),
            ("_%", "", false),
            // Case-sensitive.
            ("abc", "ABC", false),
            ("ABC%", "abcdef", false),
            ("Err%", "Error: boom", true),
            ("err%", "Error: boom", false),
            // No escapes: `%`/`_` in the PATTERN are always wildcards, and
            // `%`/`_` in the TEXT are ordinary chars wildcards can consume.
            ("100%", "100%", true), // the trailing % consumed a literal '%'
            ("100_", "100%", true), // `_` consumed a literal '%'
            ("1000", "100%", false),
            // Unicode: `_` is one CHAR (scalar value), not one byte.
            ("_stanbul", "İstanbul", true),
            ("İst%", "İstanbul", true),
            ("ΟΣ%", "ΟΣΑ", true),
            ("οσ%", "ΟΣΑ", false), // case-sensitive: no folding
            ("caf_", "café", true),
            // Backtracking shapes.
            ("%a%a%", "aa", true),
            ("%a%a%", "a", false),
            ("a%%b", "ab", true),
            ("%%", "", true),
            ("x%yz%", "xAAyzBByz", true),
        ];
        for (pattern, text, want) in cases {
            assert_eq!(
                like_match(pattern, text),
                *want,
                "like_match drifted for pattern={pattern:?} text={text:?}"
            );
        }
    }
}
