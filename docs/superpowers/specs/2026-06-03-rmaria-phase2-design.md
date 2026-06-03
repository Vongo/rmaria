# rmaria Phase 2 — Structural + Performance Refactor (Design)

**Date:** 2026-06-03
**Branch:** `perf_phase2` (single PR, based on `main` with Phase 1 merged)
**Status:** design approved pending spec review

## Goal

Build on Phase 1 (which fixed injection, per-row connections, data-loss, data.table support) with a single PR that:
1. **Eliminates duplication & cleans structure** — extract `resolve_credentials()`, split `R/maria.R` into focused files, remove dead escaping helpers, clear `R CMD check` NOTEs.
2. **Makes the write path fast** — replace per-row / per-chunk string SQL with **parameterized, batched** statements built by **pure (DB-free) builder functions**, wrapped in **transactions** that return affected-row counts.

## Decisions (locked with the user)

- **One PR** for all of Phase 2 (foundation + performance).
- `resolve_credentials()`: **de-dup + `stop()` on missing creds** (no longer returns `FALSE`).
- Dead code: **remove `esq`/`edq`; keep `insert_source_full_file`** (minimally qualify its calls).
- Write path: **parameterized + batched** via pure builders.
- Failure mode: **atomic per operation** (transaction) + **`stop()` on error** + **return affected-row count**.
- **Defer connection pooling** (`pool`) — not in this PR.
- Skip-NULL and Inf/NaN→NULL semantics from Phase 1 are **preserved** (via `COALESCE`).

## Empirical validation (live MariaDB 11.8, 10,000 rows)

Correctness (both `TRUE`):
- Multi-row `INSERT … ON DUPLICATE KEY UPDATE col = COALESCE(VALUES(col), col)` preserves skip-NULL: existing value kept when the incoming value is NULL, updated otherwise; brand-new keys inserted (NULL stored as NULL).
- `UPDATE t JOIN tmp ON keys SET col = COALESCE(tmp.col, t.col)` preserves composite-key matching, skip-NULL, and skips NULL-key rows (NULL never matches in the JOIN).

Performance:
| Operation | Per-row (Phase 1) | Batched (Phase 2) | Speedup |
|---|---|---|---|
| Upsert 10k | 72.1 s | 0.50 s (multi-row param, 1000/chunk) | 145× |
| Update 10k | 72.0 s | 0.32 s (temp-table JOIN) | 226× |

The multi-row parameterized string (chunked) beat per-vector binding (0.50 s vs 0.70 s) and is the chosen approach for insert/upsert; temp-table-join is chosen for update.

## Architecture

### File split (pure reorganization; NAMESPACE/man regenerate unchanged)
| File | Responsibility |
|---|---|
| `R/utils.R` *(exists)* | `%ni%`, `create_pb`, `update_pb`, `.term_width` |
| `R/connection.R` | `LOGGER.MAIN` (package constant), `.maria_connect` |
| `R/credentials.R` | `resolve_credentials` |
| `R/sql-builders.R` | pure: `quote_ident`, `build_insert_sql`, `build_upsert_sql`, `build_update_join_sql` |
| `R/select.R` | `pull_data`, `selectq`, `exec_query`, `execq` |
| `R/insert.R` | `insert_table`, `insertq`, `insert_table_local`, `insert_source_full_file` |
| `R/modify.R` | `upsert_table`, `upsertq`, `update_table`, `updateq` |
| `R/delete.R` | `delete_from_table`, `deleteq`, `truncate_table` |

`R/maria.R` removed once emptied. `LOGGER.MAIN <- "com.vongo.rmaria"` becomes a top-level constant; `init()` and all its call sites are removed.

### `resolve_credentials(call_env = parent.frame())`
- Reproduces the **exact** existing env-scan (the 9-environment `parent.frame`/`parent.env` walk, including the purrr::map / parallel::mclapply frames) — but computed relative to `call_env` so behavior is identical when invoked one frame deeper from the wrapper.
- Returns `list(host=, db=, user=, pwd=)` on success.
- **`stop("rmaria: DB/HOST/PWD/USER not found in the calling context. See ?load_env")`** on failure (replaces the silent `return(FALSE)` in all 7 wrappers).
- Callers become: `creds <- resolve_credentials(); pull_data(host=creds$host, db=creds$db, user=creds$user, password=creds$pwd, query=query, ...)`.
- Removing the bare `DB/HOST/PWD/USER` names clears their "no visible binding" NOTE.

### Pure SQL builders (`R/sql-builders.R`) — no DB connection required
- `quote_ident(x)` — MySQL/MariaDB identifier quoting: wrap in backticks, double any internal backtick. Pure, testable.
- `build_insert_sql(table, cols, n_rows, ignore)` → `INSERT [IGNORE] INTO \`t\` (\`c1\`,\`c2\`) VALUES (?,?),(?,?)…` with `n_rows` placeholder tuples.
- `build_upsert_sql(table, cols, keycols, n_rows)` → as above plus `ON DUPLICATE KEY UPDATE \`c\`=COALESCE(VALUES(\`c\`),\`c\`)` for each non-key column.
- `build_update_join_sql(table, tmp, cols, keycols)` → `UPDATE \`t\` x JOIN \`tmp\` n ON x.\`k\`=n.\`k\` [AND …] SET x.\`c\`=COALESCE(n.\`c\`,x.\`c\`) …`.
- Values are always `?` placeholders — no value interpolation anywhere.

### Execution
- Params are bound row-major (flat list of `n_rows × ncol` scalars per chunk for insert/upsert).
- Non-finite numerics → NA (Phase-1 helper reused) → bind as NULL.
- `as.data.frame()` normalization retained (data.table-safe).
- **Chunking:** insert/upsert in chunks (default 1000 rows/statement) to stay within `max_allowed_packet`/placeholder limits.
- **Transactions:** each public write runs inside `DBI::dbWithTransaction(con, { … })` on one `.maria_connect` connection (`on.exit` disconnect). On any error the transaction rolls back and the error is re-raised (`stop`). On success the function returns `invisible(<affected row count>)`.

### Per-operation
- **insert_table** → chunked multi-row parameterized `INSERT [IGNORE]`. (`dbAppendTable` rejected: it can't express `INSERT IGNORE` and we want one consistent builder path.)
- **upsert_table** → chunked multi-row parameterized `INSERT … ON DUPLICATE KEY UPDATE col=COALESCE(VALUES(col),col)`; `keycols` validation retained; the Phase-1 per-row null-skip and empty-update special-case are subsumed by COALESCE.
- **update_table** → create a `TEMPORARY` table, bulk-load the batch (chunked parameterized insert), one `UPDATE … JOIN … SET col=COALESCE(tmp.col, target.col)`, drop temp. `keycols` validation retained; composite-key and skip-NULL behavior preserved by the JOIN + COALESCE.
- **insert_table_local**, **delete_from_table**, **truncate_table**, **exec_query**, **pull_data** — unchanged from Phase 1 except routed through `resolve_credentials`/`LOGGER.MAIN` as applicable.

## Behavior changes (the only externally visible ones)
1. Missing credentials → **error** (was silent `FALSE`) in selectq/execq/insertq/deleteq/upsertq/updateq/insert_table_local.
2. Writes are **atomic**, **`stop()` on failure** (was best-effort log-and-continue), and **return the affected-row count** (was `invisible(NULL)`).
3. Signatures unchanged. Data semantics (skip-NULL, Inf/NaN→NULL, identifier/value safety, data.table support) preserved.

Edge case noted: if an input update/upsert batch contains duplicate keys, the batched form resolves them as the DB does (last/arbitrary wins within a statement) rather than Phase-1's strict last-row-wins per-row loop. This only affects malformed input and is documented.

## Testing
- **New pure unit tests (no DB)** — `tests/testthat/test-sql-builders.R`: `quote_ident` escaping (incl. embedded backtick); exact SQL strings from `build_insert_sql`/`build_upsert_sql`/`build_update_join_sql` for 1-col, multi-col, multi-row, key/non-key splits.
- **`resolve_credentials` unit tests (no DB)** — `tests/testthat/test-credentials.R`: resolves creds set in the immediate caller, in a nested helper, and via a `purrr::map`/`lapply` frame; `stop()`s with a clear message when any of DB/HOST/PWD/USER is absent.
- **Integration (existing 45 must stay green)** — behavior preserved; add: atomic rollback (a bad row aborts the whole batch — nothing persists), affected-row-count return values, large batched upsert & update round-trips (≥2 chunks), temp-table-join correctness incl. composite keys & skip-NULL.
- **Benchmark** — a `bench/` or PR-comment script timing 10k-row insert/upsert/update before vs after (not part of the test suite).

## Verification target
`R CMD check`: 0 errors, 0 warnings, ≤1 NOTE (WTFPL license — the user's deliberate choice). Full `testthat` suite green against dockerized MariaDB. Benchmark shows the ~100–225× upsert/update win.

## Out of scope (future)
Connection pooling (`pool`); converting `pull_data`/`exec_query`/`delete`/`truncate` to the builder layer (they're single statements already); CRAN-formalizing the license.
