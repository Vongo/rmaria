# Encoding Hardening for `rmaria` — Design

**Date:** 2026-06-08
**Branch:** `debug_nul_string`
**Status:** Approved design, pending spec review

## Problem

`selectq('select * from session_request_history_details where field = "keyword" and value<>""')`
fails with:

```
embedded nul in string: '\xff\xfep\0r\0e\0m\0i\0e\0r\0e\0 \0p\0r\0o\0\r\0'
```

The byte sequence decodes as **UTF-16LE** (`FF FE` BOM + `p\0 r\0 e\0 …` + trailing `\r`)
= the text `"premiere pro\r"`. A `value` column declared as text holds raw UTF-16
bytes (Windows origin: BOM + CR), inserted without decoding to UTF-8.

Two root causes:

1. **Data layer:** one or more `value` rows contain raw UTF-16 bytes instead of UTF-8.
2. **Mechanism layer:** the fetch dies at `pull_data` → `RMariaDB::dbGetQuery`
   (`R/maria.R:108`). R `character` vectors cannot hold a `\0` byte (C-level
   NUL termination), so RMariaDB aborts the **entire** `dbGetQuery` — one poisoned
   cell kills the whole result set. The `set character set "utf8"` step does not
   help, because `U+0000` is valid UTF-8.

## Goals

- **E (read-side, primary):** `pull_data`/`selectq` transparently recover from
  embedded-NUL columns and return usable UTF-8 strings, with a warning. This is
  what unblocks reading already-poisoned rows.
- **D (write-side, defense-in-depth):** normalize character columns to UTF-8 on
  insert/upsert/update paths, and warn when a binary (`raw`/`list`) column — the
  only realistic NUL carrier — is inserted.

### Non-goals

- Retroactively repairing existing DB rows (a separate data-migration task).
- Fixing the upstream ingestion that produced the bytes (lives in another repo —
  `rmaria` is only the connection layer).
- Transcoding genuine binary blobs (we cannot know their intended encoding).

## Why write-side cannot fix the observed symptom

By the time data reaches an insert function as an R `character` column, it is
**already NUL-free** — R cannot construct a string containing `\0`. The UTF-16
bytes only reached the DB through a binary-preserving path (`raw`/`blob` column,
or file-based `LOAD DATA`). So write-side work is about **encoding correctness
going forward** (latin1/`unknown` → UTF-8) plus a **warn policy** for binary
columns — not a fix for the exact bytes already stored.

## Architecture (Approach A: centralized helpers)

New file **`R/encoding.R`** — dependency-free, mostly pure helpers:

| Helper | Signature | Purpose | DB? |
|---|---|---|---|
| `decode_dbi_bytes` | `(raw, mode = "decode") -> character(1)` | One raw vector → UTF-8 string, BOM-aware | no |
| `normalize_utf8` | `(x) -> x` | Character vector → `enc2utf8`; else passthrough | no |
| `is_embedded_nul_error` | `(e) -> logical(1)` | TRUE iff message matches `embedded nul` | no |
| `build_recovery_query` | `(query, colinfo) -> character(1)` | Build `CAST(... AS BINARY)` re-fetch SQL | no |
| `recover_nul_fetch` | `(con, query, mode) -> data.frame` | Orchestrate re-fetch + decode | yes |

`maria.R` only gains thin calls into these helpers. All helpers except
`recover_nul_fetch` are unit-testable with no server.

## Read-side design (E)

### Public surface

`pull_data` gains a parameter:

```r
on_nul = c("decode", "error", "strip")   # default "decode"
```

`selectq` already forwards `...` to `pull_data`, so `selectq(query, on_nul = "error")`
works with no change to `selectq`.

### Control flow in `pull_data`

Inside the existing fetch `tryCatch`, when `dbGetQuery` throws and
`is_embedded_nul_error(e)` is TRUE:

- **`on_nul = "error"`** — raise an actionable error and stop. Do **not** retry
  (the error is deterministic). Message lists the **candidate text column(s)**
  (from a cheap `dbSendQuery` + `dbColumnInfo`, no row fetch — we don't pinpoint
  the exact offending cell in this mode) and the `HEX()` + `iconv` recovery recipe.
  If even the metadata probe fails, fall back to a generic message carrying the
  recipe.
- **`on_nul = "decode"` or `"strip"`** — call `recover_nul_fetch(con, query, mode)`
  on the still-open connection. On success, set `state$data`, `logwarn`, and break
  the retry loop. On any failure inside recovery, fall back to the `"error"`
  message (recovery never makes things worse).

Embedded-NUL is excluded from the retry/backoff loop regardless of mode.

### `recover_nul_fetch` algorithm

1. `res <- dbSendQuery(con, query)`; `ci <- dbColumnInfo(res)`; `dbClearResult(res)`.
   `dbColumnInfo` reads the result header **without fetching rows**, so it does
   not trigger the NUL error.
2. `recovery_query <- build_recovery_query(query, ci)`.
3. `raw_df <- dbGetQuery(con, recovery_query)` — cast columns come back as `raw`
   (NUL-safe); other columns unchanged.
4. For each cast column, `decode_dbi_bytes` every cell back into the column, in
   the original column order. Count rows that actually contained `\0` for the
   warning.
5. Return the assembled `data.frame` (downstream `pull_data` post-processing —
   `modify_if`, `as.data.table` — runs as usual).

### `build_recovery_query`

```sql
SELECT `id`, CAST(`value` AS BINARY) AS `value`, ...
FROM ( <original query, trailing ';' stripped> ) AS rmaria_sub
```

- Only columns whose `colinfo$type == "character"` are cast; all others pass
  through unchanged (preserves types).
- Column names backtick-quoted; original order preserved.
- Assumes a single `SELECT` (true for `pull_data`'s purpose). Multi-statement,
  non-SELECT, or duplicate-column-name queries that cannot be wrapped → caller
  falls back to the actionable error.

### `decode_dbi_bytes` rules

- Empty raw → `""`.
- BOM `FF FE` → UTF-16LE; `FE FF` → UTF-16BE. Strip BOM, `iconv` to UTF-8.
- No BOM but contains `0x00` → assume UTF-16LE (genuine UTF-8 never contains
  `0x00`). If `iconv` fails, fall back to `strip`.
- No `0x00` → UTF-8 passthrough (`rawToChar`, mark UTF-8) — the already-fine
  column case.
- `mode = "strip"` → remove `0x00` bytes and any BOM, then treat remainder as
  UTF-8.
- `iconv` is always called with a safe fallback (`sub` / `tryCatch`) so a
  malformed byte never throws.
- **Faithful decode:** a trailing `\r` is kept (real stored content, not ours to
  trim).

## Write-side design (D)

At the **start** of `insert_table`, `upsert_table`, `update_table`, and
`insert_table_local` (once, not per-cell):

- Every `is.character` column → `normalize_utf8(col)`. Safe and idempotent: no-op
  on already-UTF-8; converts `latin1`/`unknown`-marked strings to consistent
  UTF-8 bytes matching the connection charset.
- Every `is.raw` / `is.list` column → `logwarn` that a binary column is being
  inserted and may carry embedded NULs requiring `on_nul` recovery to read back.
  **Warn-only**; no transcoding.

### Noted, out of scope (flagged for later decision)

The homemade builders set `character set "utf8"` (3-byte) via `exec_query`
(`R/maria.R:218`), while `insert_table_local` uses `utf8mb4`. 4-byte characters
(e.g. emoji) could truncate on the homemade paths. Not changed here.

## Error handling summary

- Only the `embedded nul` error class triggers recovery; all other errors keep
  today's retry/stop behavior.
- Embedded-NUL is deterministic → never retried.
- Recovery failure → graceful fall back to the actionable error.
- Pure helpers never throw on malformed bytes.

## Testing (TDD)

Add `tests/testthat.R` + `tests/testthat/`; add `testthat` to `Suggests` in
`DESCRIPTION`. Tests written first (RED), then implementation (GREEN).

Pure unit tests (no DB):

- `decode_dbi_bytes`: UTF-16LE+BOM → `"premiere pro\r"` (exact-bytes fixture from
  the original error), UTF-16BE, plain UTF-8 passthrough, empty input, non-ASCII
  (`é`) round-trip, `strip` mode.
- `normalize_utf8`: latin1 → UTF-8, UTF-8 no-op, `NA`, non-character passthrough.
- `is_embedded_nul_error`: matches RMariaDB's message, rejects unrelated errors.
- `build_recovery_query`: only character columns cast, trailing `;` stripped,
  column order preserved, names backtick-quoted.

Live-DB integration tests (insert UTF-16 bytes → read back via each `on_nul`
mode; write-side normalization round-trip) are `skip()`-guarded behind an env var
(e.g. `RMARIA_TEST_HOST`), since CI has no MariaDB. Pure-helper tests carry the
80% coverage target.

## Files touched

- **New:** `R/encoding.R`, `tests/testthat.R`, `tests/testthat/test-encoding.R`
  (+ DB-guarded test file).
- **Modified:** `R/maria.R` (`pull_data` recovery hook + `on_nul` param; write-side
  normalization in 4 functions), `DESCRIPTION` (`Suggests: testthat`), `NAMESPACE`
  (roxygen regen for the new `pull_data` param docs).
