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

/// A sound token-index constraint implied by a LIKE pattern (F2 prefix
/// analysis). If `like_match(pattern, text)` holds, then `fts_tokens(text)`
/// satisfies EVERY constraint of `like_constraints(pattern)` — the property
/// `like_constraints_are_sound` pins. Constraints only ever NARROW; the
/// exact matcher still verifies every candidate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LikeConstraint {
    /// Some token equals this (lowercased) word exactly.
    Token(String),
    /// Some token starts with this (ASCII-lowercased) prefix.
    Prefix(String),
}

/// Extract the sound token constraints from a LIKE pattern.
///
/// A literal run is a maximal wildcard-free span. Within a run, a maximal
/// alphanumeric word is:
///
/// - **complete on the left** iff preceded by a non-alphanumeric literal
///   char in the same run, or the run starts the pattern (anchored: the
///   text starts here). A word after `%`/`_` is NOT left-complete — the
///   wildcard may extend it leftward into a longer token.
/// - **complete on the right**, symmetrically (non-alphanumeric literal
///   follows, or the run ends the pattern).
///
/// Both-complete words become `Token(to_lowercase(word))` — sound because
/// the matched text carries the identical char sequence with the identical
/// non-alphanumeric neighbors, so the text's token there is the lowercase
/// of the very same string (final-sigma-safe: both sides lowercase the same
/// word). Left-complete words cut on the RIGHT by a wildcard become
/// `Prefix` of their maximal LEADING ASCII-alphanumeric run, ASCII-lowered
/// (ruling 3: `str::to_lowercase` is not prefix-stable — Greek final sigma —
/// but ASCII maps 1:1 context-free, so a token continuing past the cut
/// still starts with the ASCII-lowered prefix); a word starting non-ASCII
/// contributes nothing. Words NOT complete on the left (suffix fragments,
/// `%infix%` interiors) contribute nothing — a forward-sorted dictionary
/// cannot range-scan suffixes (the ledgered n-gram deferral).
///
/// An empty result means the pattern is unanalyzable (all-wildcard, or all
/// fragments wildcard-poisoned) — the caller falls back to the full walk.
pub(crate) fn like_constraints(pattern: &str) -> Vec<LikeConstraint> {
    let chars: Vec<char> = pattern.chars().collect();
    let n = chars.len();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < n {
        if chars[i] == '%' || chars[i] == '_' {
            i += 1;
            continue;
        }
        let run_start = i;
        let mut run_end = i;
        while run_end < n && chars[run_end] != '%' && chars[run_end] != '_' {
            run_end += 1;
        }
        let mut a = run_start;
        while a < run_end {
            if !chars[a].is_alphanumeric() {
                a += 1;
                continue;
            }
            let mut b = a;
            while b < run_end && chars[b].is_alphanumeric() {
                b += 1;
            }
            let left_complete = a > run_start || run_start == 0;
            let right_complete = b < run_end || run_end == n;
            if left_complete && right_complete {
                let word: String = chars[a..b].iter().collect();
                out.push(LikeConstraint::Token(word.to_lowercase()));
            } else if left_complete {
                let ascii: String = chars[a..b]
                    .iter()
                    .take_while(|c| c.is_ascii_alphanumeric())
                    .map(|c| c.to_ascii_lowercase())
                    .collect();
                if !ascii.is_empty() {
                    out.push(LikeConstraint::Prefix(ascii));
                }
            }
            a = b;
        }
        i = run_end;
    }
    out.sort();
    out.dedup();
    out
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

    /// Targeted analyzer vectors: the accelerating shapes, the honest
    /// fallthroughs, and the Unicode traps (ruling 3).
    #[test]
    fn like_constraints_vectors() {
        use LikeConstraint::{Prefix, Token};
        let cases: &[(&str, &[LikeConstraint])] = &[
            // prefix% — the headline shape.
            ("Err%", &[Prefix("err".into())]),
            ("error: db%", &[Token("error".into()), Prefix("db".into())]),
            // exact pattern (no wildcards): every word is a token.
            (
                "Error: boom",
                &[Token("boom".into()), Token("error".into())],
            ),
            // interior delimited word inside %...% accelerates.
            ("% error %", &[Token("error".into())]),
            ("%-not-%", &[Token("not".into())]),
            // bare %infix% / suffix: nothing (n-gram machinery, deferred).
            ("%error%", &[]),
            ("%error", &[]),
            ("%", &[]),
            ("", &[]),
            ("%_%", &[]),
            // `_` cuts like `%` but still leaves a left-anchored prefix.
            ("usage_percent%", &[Prefix("usage".into())]),
            ("_rror%", &[]),
            // Unicode: non-ASCII leading char → no PREFIX constraint
            // (final sigma / dotted-İ lowercase instability)…
            ("İst%", &[]),
            ("ΟΣ%", &[]),
            // …but full to_lowercase is safe for complete tokens.
            ("% ΟΣ %", &[Token("ος".into())]),
            ("% İst %", &[Token("i\u{307}st".into())]),
            // ASCII prefix stops at the first non-ASCII char.
            ("caf\u{e9}zzz%", &[Prefix("caf".into())]),
        ];
        for (pattern, want) in cases {
            assert_eq!(
                like_constraints(pattern).as_slice(),
                *want,
                "analyzer drifted for {pattern:?}"
            );
        }
    }

    /// THE SOUNDNESS PROPERTY: whenever a pattern matches a text, the
    /// text's tokens satisfy every extracted constraint — i.e. narrowing by
    /// constraints can never drop a true match. Hostile seeded generator,
    /// no rand dep; alphabet includes the Unicode traps and both wildcards.
    #[test]
    fn like_constraints_are_sound() {
        let mut state = 0xdead_beef_cafe_f00du64;
        let mut rng = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        const ALPHABET: &[char] = &[
            'a', 'b', 'A', 'B', '0', '%', '_', ' ', '-', 'Σ', 'ς', 'σ', 'İ', 'i', 'é', '!',
        ];
        let mut checked = 0usize;
        for _ in 0..4000 {
            let tlen = (rng() % 12) as usize;
            // Texts keep literal '%'/'_' chars — hostile on purpose.
            let text: String = (0..tlen)
                .map(|_| ALPHABET[(rng() % ALPHABET.len() as u64) as usize])
                .collect();
            // Pattern: either random chars, or the text with random wildcard
            // splices (biased toward MATCHING patterns, where soundness bites).
            let pattern: String = if rng() % 2 == 0 {
                let plen = (rng() % 8) as usize;
                (0..plen)
                    .map(|_| ALPHABET[(rng() % ALPHABET.len() as u64) as usize])
                    .collect()
            } else {
                let tc: Vec<char> = text.chars().collect();
                let mut p = String::new();
                let mut k = 0usize;
                while k < tc.len() {
                    match rng() % 6 {
                        0 => {
                            p.push('%');
                            k += (rng() % 3) as usize; // % swallows a run
                        }
                        1 => {
                            p.push('_');
                            k += 1;
                        }
                        _ => {
                            p.push(tc[k]);
                            k += 1;
                        }
                    }
                }
                if rng() % 3 == 0 {
                    p.push('%');
                }
                p
            };
            if !like_match(&pattern, &text) {
                continue;
            }
            checked += 1;
            let tokens = fts_tokens(&text);
            for c in like_constraints(&pattern) {
                let ok = match &c {
                    LikeConstraint::Token(t) => tokens.iter().any(|x| x == t),
                    LikeConstraint::Prefix(p) => tokens.iter().any(|x| x.starts_with(p.as_str())),
                };
                assert!(
                    ok,
                    "UNSOUND: pattern {pattern:?} matches text {text:?} but constraint {c:?} \
                     is unsatisfied by tokens {tokens:?}"
                );
            }
        }
        assert!(
            checked > 300,
            "generator produced too few matching pairs ({checked}) — property vacuous"
        );
    }
}
