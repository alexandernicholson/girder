//! The in-memory write buffer: key-sorted records PLUS the text token map,
//! kept consistent under ONE lock (a reader that sees a record always sees
//! its tokens — no two-lock skew, the A1b lesson).
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::record::{merge_delta, Record};
use crate::text::fts_tokens;

/// A memtable: newest-wins records + the token→keys map for `text_match`.
/// The map is maintained at insert time (tokenize once per write, not once
/// per scan) and is exactly consistent with `records` by construction —
/// overwrites remove the old version's tokens first.
#[derive(Default)]
pub struct MemTable {
    records: BTreeMap<String, Record>,
    /// token → keys of records whose text contains that token.
    token_keys: HashMap<String, HashSet<String>>,
    /// key → its text's distinct tokens (for overwrite cleanup).
    key_tokens: HashMap<String, Vec<String>>,
    /// Resident delta-flagged records (their base may be in a segment).
    delta_keys: usize,
    /// Conservative key range of every delta inserted since this memtable
    /// was created (never shrunk on overwrite — a superset is safe; the
    /// groomer only uses it to SKIP ranges).
    delta_key_range: Option<(String, String)>,
}

impl MemTable {
    /// Fold a delta record onto the resident version (if any) and store the
    /// result — a single serialized-writer step, so concurrent increments can
    /// never lose an update. When no version is resident the delta is stored
    /// AS a delta (its base may live in a segment; reads fold across sources).
    pub fn insert_delta(&mut self, delta: Record) {
        let merged = merge_delta(self.records.get(&delta.key), &delta);
        self.insert(merged);
    }

    /// Newest-wins upsert, keeping the token map consistent.
    pub fn insert(&mut self, record: Record) {
        if let Some(old) = self.records.get(&record.key) {
            if old.is_delta() {
                self.delta_keys -= 1;
            }
        }
        if record.is_delta() {
            self.delta_keys += 1;
            self.delta_key_range = Some(match self.delta_key_range.take() {
                None => (record.key.clone(), record.key.clone()),
                Some((lo, hi)) => (lo.min(record.key.clone()), hi.max(record.key.clone())),
            });
        }
        let key = record.key.clone();
        if let Some(old_tokens) = self.key_tokens.remove(&key) {
            for t in old_tokens {
                if let Some(keys) = self.token_keys.get_mut(&t) {
                    keys.remove(&key);
                    if keys.is_empty() {
                        self.token_keys.remove(&t);
                    }
                }
            }
        }
        if let Some(text) = &record.text {
            let mut distinct: Vec<String> = fts_tokens(text);
            distinct.sort_unstable();
            distinct.dedup();
            for t in &distinct {
                self.token_keys
                    .entry(t.clone())
                    .or_default()
                    .insert(key.clone());
            }
            if !distinct.is_empty() {
                self.key_tokens.insert(key.clone(), distinct);
            }
        }
        self.records.insert(key, record);
    }

    pub fn get(&self, key: &str) -> Option<&Record> {
        self.records.get(key)
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Key-ascending record iteration (flush encodes straight from this).
    pub fn values(&self) -> impl Iterator<Item = &Record> {
        self.records.values()
    }

    /// Are any delta-flagged records resident? (Fold-mode scan detection.)
    pub fn has_deltas(&self) -> bool {
        self.delta_keys > 0
    }

    /// Conservative key range of every delta ever inserted here (a superset
    /// — never shrunk on overwrite). The groomer uses it to keep hands off
    /// counter ranges.
    pub fn delta_range(&self) -> Option<(String, String)> {
        self.delta_key_range.clone()
    }

    /// Keys whose text contains ALL of `want` (AND-of-tokens) — the token-map
    /// face of the `QuerySpec::matches` text oracle; equal by construction
    /// (same tokenizer at insert time) and pinned by the agreement tests.
    /// `want` empty ⇒ empty set (no tokens = no match).
    pub fn text_candidates(&self, want: &[String]) -> HashSet<&str> {
        if want.is_empty() {
            return HashSet::new();
        }
        let mut sets: Vec<&HashSet<String>> = Vec::with_capacity(want.len());
        for t in want {
            match self.token_keys.get(t) {
                Some(s) => sets.push(s),
                None => return HashSet::new(),
            }
        }
        sets.sort_by_key(|s| s.len());
        let (first, rest) = sets.split_first().expect("want is non-empty");
        first
            .iter()
            .filter(|k| rest.iter().all(|s| s.contains(*k)))
            .map(|k| k.as_str())
            .collect()
    }
}
