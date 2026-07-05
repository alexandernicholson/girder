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
| Manifest | `[u32 "GMAN"][u32 ver]` prefix; no magic = v0 | v0 (bare rmp), v1 (headered rmp) | v1 | v0 + v1 | misparse → corrupt error (no silent damage: the manifest is never partially applied) |
| WAL | `[u32 "GWAL"][u32 ver]` header; no header = v0 | v0 (bare frames), v1 (headered frames) | v1 (new files; appends to a v0 file keep it v0) | v0 + v1 | the magic decodes as a >1 GB frame length (cap: 64 MB) → replay stops cleanly at byte 0 |

Segment section kinds are additionally forward-extensible WITHIN v2: the
directory is self-describing `(kind, name, offset, len, crc)` entries and
readers ignore unknown kinds — adding a section (K_TEXT/K_TOKENS were added
this way) requires no version bump.

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
versions inside payloads) is a separate, deferred concern — this document
covers the engine's own container formats only.
