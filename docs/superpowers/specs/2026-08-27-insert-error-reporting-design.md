# insert_table_local: report failures instead of swallowing them

**Date:** 2026-08-27
**Status:** Design — approved, pending implementation
**Repo:** `rmaria` (Vongo/rmaria)
**Branch:** `insert_error_reporting`

---

## 1. Problem

`insert_table_local()` catches its own errors, logs them, and never rethrows:

```r
  }, error = function(e) {
    logging::logerror("Error while inserting data into table %s: %s", ...)
  }, finally = {
    if (!is.null(con)) RMariaDB::dbDisconnect(con)
  })
}
```

The `tryCatch` is the function's last expression, so its value is the function's return. On the error path that value is whatever `logerror()` returns — which is `TRUE`. So a failed INSERT is indistinguishable from a successful one **by exception and by return value simultaneously**. There is no signal at all beyond a log line.

This is not a defensible contract. It is a divergence from the one this package already has.

### 1.1 The sibling already does it right

`insert_table()` — which `insertq()` delegates to — handles the same situation correctly:

```r
    error = function(e) {
      if (!nolog) logging::logerror("Error inserting into %s: %s", ...)
      stop(e)
    }
  )
  invisible(affected)
```

Log, then rethrow, and return the affected row count. `insert_table_local` is the outlier.

### 1.2 How it surfaced

In `mega`, `insert_domain_aggregate()` sets `success <- FALSE`, calls `insert_table_local()`, then sets `success <- TRUE` on the following line. Because nothing ever throws, `success` is unconditionally `TRUE` — the function can only report failure if the *pre-insert* duplicate check throws, never if the write itself fails. A backfill job built on it logs "wrote N rows" while writing none.

### 1.3 Measured blast radius

In `mega/src`, re-derived so all four numbers come from the same, consistent basis (the
previous pass mixed three different grep patterns and was internally inconsistent: 141 + 0 did
not equal the claimed 147):

| measurement | value | grep basis |
|---|---|---|
| files containing a call | 63 | `grep -rl "insert_table_local(" src/ \| wc -l` |
| call lines | 141 | `grep -rn "insert_table_local(" src/ \| wc -l` |
| call sites assigning the result | 0 | `grep -rnE "<-\s*insert_table_local\(\|=\s*insert_table_local\(" src/ \| wc -l` |
| call sites piping the result | 0 | `grep -rnE "insert_table_local\([^)]*\)\s*%>%\|%>%\s*insert_table_local\(\|\|>\s*insert_table_local\(\|insert_table_local\([^)]*\)\s*\|>" src/ \| wc -l` |

Nobody uses the return value: every call is bare. `rutils` has no callers.

Nobody reads the return today. That makes the return half of this change purely additive; the throwing half is where the risk sits.

---

## 2. Decision

**Match `insert_table` fully: log, rethrow, and return the row count.**

Considered and rejected: returning a status without throwing (leaves silent data loss in place), and an `on_error` parameter defaulting to today's behaviour (leaves the wrong default forever and gives the package two insert contracts instead of one).

The cost is accepted deliberately: 141 bare call sites in `mega`, plus any other consumer, begin propagating errors on a single package reinstall. Jobs that were quietly writing nothing will start failing loudly. That is the point.

---

## 3. Design

### 3.1 Error handling

Replace the swallowing handler with `insert_table`'s shape:

```r
  }, error = function(e) {
    logging::logerror("Error while inserting data into table %s (%s of %s rows written): %s",
                      table_name_in_base, written, nrow(table), conditionMessage(e),
                      logger = LOGGER.MAIN)
    stop(e)
  }, finally = {
    if (!is.null(con)) RMariaDB::dbDisconnect(con)
  })
```

`finally` is unchanged — the connection must still close on both paths.

### 3.2 Return value

`invisible(written)` — the same counter the error path reports, so there is one source of truth rather than two. On success it equals `nrow(table)`.

The counter is initialised to `0L` before the `tryCatch`, incremented by each chunk's row count on the chunked path and by `nrow(table)` on the single-write path. It is therefore meaningful in both places: the return value on success, and the "how much landed" figure in the error message.

This means something subtly different from `insert_table`'s return, and the difference must be documented rather than glossed. `insert_table` returns `dbExecute`'s affected count, which under `ignore=TRUE` can be *less* than the rows supplied. `dbWriteTable` returns no count at all, but `insert_table_local` has no `IGNORE` path — a duplicate key raises rather than skipping. So on success every supplied row landed, and rows-written equals `nrow(table)`.

### 3.3 Partial writes — reported, not prevented

`insert_table_local` is **not transactional**. `dbWithTransaction` appears once in `R/insert.R`, inside `insert_table`. The chunked path writes each batch directly, so a failure on chunk 3 of 5 leaves chunks 1 and 2 committed.

Today that is invisible. Under a rethrow it would become visible but unquantified — the caller learns it failed, not that 200,000 of 500,000 rows are now in the table, which is precisely what someone deciding whether to re-run needs.

So the loop tracks rows written and the error message reports it, as in §3.1.

**Wrapping the loop in a transaction is deliberately rejected.** The function's own roxygen example is `preface_queries="SET session rocksdb_bulk_load=1"`, and RocksDB bulk load requires autocommit; a transaction would likely break the documented use case. Making the partial write *reportable* is honest. Making it *atomic* is a larger, riskier change that would need its own design.

### 3.4 Documentation

The roxygen has **no `@return` tag at all**, while its example writes `data <- insert_table_local(iris, "iris")` — implying a useful return that does not exist. Fix all three:

- add `@return (invisibly) the number of rows written`, noting it equals `nrow(table)` on success and explaining why that differs from `insert_table`'s affected count
- record that `insert_table`'s `nolog` parameter has no counterpart here, so a caller cannot suppress the error log — see §3.5
- state that errors are logged and rethrown
- state that a failed call may have written a prefix of the data, and that the error message reports how much
- correct the examples so they do not imply a meaningful assignment

### 3.5 Not in scope

`insert_table`'s `nolog` parameter is **not** being added. Nothing has asked for it, no caller needs it, and adding a knob with no consumer is speculative. If a caller later needs silence, that is a small additive change.

---

## 4. Testing

Integration tests, following the `expect_error` precedent already in `tests/testthat/test-insert2.R`, added to `tests/testthat/test-insert-local.R` — which currently pins only three happy-path behaviours and nothing about errors.

| case | expectation |
|---|---|
| insert into a non-existent table | raises, rather than returning |
| a value violating the target schema | raises |
| successful insert | returns `nrow(table)` invisibly |
| successful chunked insert | returns the full row count, not the last chunk's |
| failure mid-chunk | raises, and the logged message names rows written before failing |
| any failure | connection still closed (the `finally` path) |

**These require a live MariaDB.** `skip_if_no_db()` gates on `RMARIA_TEST_HOST`; CI supplies a service container and `docker-compose.test.yml` covers local runs. They will **skip** in the authoring environment, so they are verified by CI rather than by the author — this is stated plainly rather than left as an assumption.

---

## 5. Rollout

Deploying means reinstalling `rmaria` on the host, which changes behaviour for **every** consumer at once, not just the aggregate jobs. There is no per-caller staging available.

Consequences to expect on the first run after reinstall:

- jobs whose inserts have been failing silently will begin failing loudly — the intended outcome, but it will look like a regression to anyone who does not know this landed
- any caller already wrapping the call in `tryCatch` absorbs the throw and behaves as before
- 48 of the 63 calling files contain a `tryCatch` somewhere, though not necessarily around the insert

## 6. Downstream follow-up (not this repo)

Once this lands, `mega`'s `insert_domain_aggregate()` and `insert_root_domain_aggregate()` can be made to actually report failure, and the `isTRUE(landed)` guard in `backfill_dark_aggregates.R` — currently decorative — becomes real. That is a separate change in a separate repo, gated on this being deployed.
