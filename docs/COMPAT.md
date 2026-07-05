# On-disk format compatibility

Every on-disk artifact carries (or implies) a format version; **absent
version words mean v0 and stay readable forever**. Unknown FUTURE versions
fail closed — an error, never a silent empty read (dropping acked data on a
downgrade would be data loss dressed as recovery). Downgrade (old binary,
new files) is not supported; see the last column for what an old binary
does.

| Artifact | Detection | Versions | Written | Read | Old binary on new file |
|---|---|---|---|---|---|
| Segment | `[u32 "gird"][u32 ver]` header | v1 (whole-file rmp), v2 (columnar) | v2 | v1 + v2 | v2 refused with a clean version error |
| Manifest | `[u32 "GMAN"][u32 ver]` prefix; no magic = v0 | v0 (bare rmp), v1 (headered), v2 (+ blob existence set) | v2 | v0 + v1 + v2 | v0-reader: misparse → corrupt error; v1-reader (B3): version check fails CLOSED (deliberate — silently rewriting a v2 manifest without its blob set would orphan every blob) |
| WAL | `[u32 "GWAL"][u32 ver]` header; no header = v0 | v0 (bare frames), v1 (headered frames) | v1 (new files; appends to a v0 file keep it v0) | v0 + v1 | the magic decodes as a >1 GB frame length (cap: 64 MB) → replay stops cleanly at byte 0 |

Segment section kinds are additionally forward-extensible WITHIN v2: the
directory is self-describing `(kind, name, offset, len, crc)` entries and
readers ignore unknown kinds — adding a section (K_TEXT/K_TOKENS were added
this way) requires no version bump.

**Section-BODY layouts version separately when ignoring would lie.** The
K_TOKENS body carries its own version word (D7-b: `[u32 u32::MAX
sentinel][u32 version]`; a headerless body is v1, readable forever — a v1
body starts with `ntokens`, which can never be `u32::MAX`). Deliberately
NOT a new section kind: an old reader ignoring an unknown kind would treat
the segment as having no text index and serve **silently-empty text
matches** — ignore-is-not-fail-closed. An unknown FUTURE body version is a
loud corrupt error instead. Written: v2 (token directory + lazy postings
blob). Read: v1 + v2.

**K_TEXT versions by POISON, not sentinel (D8).** Unlike K_TOKENS, a v1
K_TEXT body begins with the presence BITMAP — arbitrary bytes — so an
in-body sentinel cannot be unambiguous. Instead, a v2 segment writes a
2-byte poison body at the old directory key `(K_TEXT, "")` — the v1
decoder requires at least `ceil(count/8) + 8·(count+1)` bytes, so for any
count ≥ 1 the poison fails GUARANTEED-loud with "text section shorter
than header" — and the real v2 body under the new name `(K_TEXT, "z2")`:
an in-body version word, presence bitmap, COMPRESSED-rows bitmap,
stored-space offsets, then the blob with each row's text raw below 512
raw bytes and individually deflated above (stored compressed only when
actually smaller). New readers resolve "z2" first and never parse the
poison; an old binary reading a v2 store dies loudly instead of silently
reading every text as absent — the same ignore-is-not-fail-closed
doctrine, achieved through the self-describing directory because the body
cannot carry a sentinel. Per-ROW compression is deliberate: the first D8
cut compressed 64 KiB chunks and regressed scattered reads 12× (every
materialized row inflated a whole chunk). v1 stores stay readable
forever; compaction rewrites them to v2. Written: v2. Read: v1 + v2.

## Deletes / tombstone vocabulary (no format change)

The delete API and the G5 shadowing guarantee (`docs/GUARANTEES.md`
§Deletes) introduced **no on-disk format change**: a tombstone is an
ordinary record labelled `del=1`, and the walk fix is read-path-only
(segment key ranges were already in the manifest zone maps). Pre-existing
embedder tombstones — including rivet's historical `timestamp: 0` shape —
are engine tombstones retroactively: they shadow correctly and read as
absent under every spec. Their one residual hazard is retention (a ts=0
tombstone TTL-expires immediately; the embedder should write delete-time
timestamps going forward — GUARANTEES §Deletes, timestamp rule).

## Background migration

Legacy segments are rewritten to the current format by a maintenance-tick
stage (`migrate()`), at most one segment per tick — bounded background work,
never hostage to write volume (same principle as retention grooming). Reads
never depend on migration: v1 stays readable forever; migration is hygiene,
not repair.

**Restart safety by construction:** each rewrite is
tmp → fsync → rename, then an atomic manifest swap, then the old file is
deleted. A kill at any point leaves either the old segment manifest-listed
(the rewrite retries next tick; the new file is an unlisted orphan =
garbage, per the manifest-is-truth rule) or the new one listed (done).
Pinned by `actors::tests::migration_converges_and_survives_kill`, which
fabricates a full pre-versioning store — v1 segments, v0 manifest, a
kill-mid-rewrite orphan — and drives it to convergence across restarts.

Rivet-side control-plane *namespace* versioning (per-namespace schema
versions inside payloads) is a separate concern — this document covers the
engine's own container formats only. The first instance lives in rivet
(plan 0013 §6 D5): score records in the `sc/` namespace carry a PINNED set
of index dimensions — labels `project`/`trace`/`source`/`name` + numeric
`value`, timestamp = the score's `created_unix_nanos` — versioned by
rivet's own `scv/format` marker with a one-shot restart-safe upgrade pass
(marker written last; reads fall back to full scan until it says
upgraded). Changing that set means a new marker version + a new pass on
the rivet side; nothing about it is engine format.
