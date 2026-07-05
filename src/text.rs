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
}
