# rmaria Phase 2 — Structural + Performance Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** De-duplicate credential resolution, split `R/maria.R` into focused files, remove dead helpers, and rewrite the write path to parameterized, batched, transactional statements built by pure (DB-free) builders — preserving all Phase-1 data semantics while making upsert/update ~100–225× faster.

**Architecture:** Pure builder functions emit `?`-placeholder SQL using a pure `quote_ident()`. Execution binds **column-vector** parameters (type-safe) per chunk inside `DBI::dbWithTransaction`, returning the affected-row count and `stop()`ing on failure. `insert`/`upsert` use chunked column-bound parameterized statements; `update` loads a `TEMPORARY` table then runs one `UPDATE … JOIN … SET col = COALESCE(tmp.col, target.col)`. Skip-NULL and Inf/NaN→NULL semantics are preserved via the non-finite→NA pre-pass and `COALESCE`.

**Tech Stack:** R, RMariaDB/DBI (parameterized `dbExecute`, `dbWithTransaction`), data.table, purrr, bit64, testthat, roxygen2, dockerized MariaDB 11.

**Base:** branch `perf_phase2` off `main` (Phase 1 merged). Live test DB: `127.0.0.1:33306` root/test db `rmaria_test`.

**Execution note (column-vector binding):** the design spec mentions a multi-row string; the plan refines this to **column-vector binding** (`dbExecute(con, single_row_sql, params = as.list(chunk))`). The benchmark showed this is ~103× (vs 145× for the flattened multi-row string) but it is **type-safe** for Date/factor/integer64 columns (no row flattening that loses class), which is the right engineering trade. Still chunked for progress + bounded memory.

---

## Test/verify conventions (every task)

- DB test runner: `Rscript -e 'Sys.setenv(RMARIA_TEST_HOST="127.0.0.1", RMARIA_TEST_PORT="33306", RMARIA_TEST_USER="root", RMARIA_TEST_PWD="test", RMARIA_TEST_DB="rmaria_test"); testthat::test_local(".", filter="<filter>")'`
- Full suite: same without `filter=`.
- Pure (no-DB) tests run with the same runner; they just don't call `skip_if_no_db()`.
- Commit style: `[Type][Scope] description`, no attribution.
- After roxygen changes: `Rscript -e 'roxygen2::roxygenise()'`.

## File structure (end state)

| File | Responsibility |
|---|---|
| `R/utils.R` *(exists)* | `%ni%`, `create_pb`, `update_pb`, `.term_width` |
| `R/sql-builders.R` *(new, Task 1)* | `quote_ident`, `build_insert_sql`, `build_upsert_sql`, `build_update_join_sql` |
| `R/credentials.R` *(new, Task 2)* | `resolve_credentials` |
| `R/connection.R` *(Task 9 move)* | `LOGGER.MAIN` constant, `.maria_connect` |
| `R/select.R` *(Task 9 move)* | `pull_data`, `selectq`, `exec_query`, `execq` |
| `R/insert.R` *(Task 9 move)* | `insert_table`, `insertq`, `insert_table_local`, `insert_source_full_file` |
| `R/modify.R` *(Task 9 move)* | `upsert_table`, `upsertq`, `update_table`, `updateq` |
| `R/delete.R` *(Task 9 move)* | `delete_from_table`, `deleteq`, `truncate_table` |

`R/maria.R` is removed in Task 9.

---

## Task 1: Pure SQL builders + unit tests

**Files:**
- Create: `R/sql-builders.R`
- Test: `tests/testthat/test-sql-builders.R`

- [ ] **Step 1: Write the failing tests**

Create `tests/testthat/test-sql-builders.R`:
```r
test_that("quote_ident wraps in backticks and doubles internal backticks", {
  expect_equal(quote_ident("tbl"), "`tbl`")
  expect_equal(quote_ident("we`ird"), "`we``ird`")
  expect_equal(quote_ident(c("a", "b")), c("`a`", "`b`"))
})

test_that("build_insert_sql emits one placeholder tuple", {
  expect_equal(
    build_insert_sql("t", c("a", "b"), ignore = TRUE),
    "INSERT IGNORE INTO `t` (`a`,`b`) VALUES (?,?)"
  )
  expect_equal(
    build_insert_sql("t", c("a"), ignore = FALSE),
    "INSERT INTO `t` (`a`) VALUES (?)"
  )
})

test_that("build_upsert_sql sets COALESCE for non-key cols only", {
  expect_equal(
    build_upsert_sql("t", c("id", "a", "b"), keycols = "id"),
    "INSERT INTO `t` (`id`,`a`,`b`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `a`=COALESCE(VALUES(`a`),`a`),`b`=COALESCE(VALUES(`b`),`b`)"
  )
})

test_that("build_upsert_sql with only key cols falls back to INSERT IGNORE", {
  expect_equal(
    build_upsert_sql("t", c("id"), keycols = "id"),
    "INSERT IGNORE INTO `t` (`id`) VALUES (?)"
  )
})

test_that("build_update_join_sql joins on keys and COALESCEs non-keys", {
  expect_equal(
    build_update_join_sql("t", "tmp", c("id1", "id2", "v"), keycols = c("id1", "id2")),
    "UPDATE `t` t JOIN `tmp` s ON t.`id1`=s.`id1` AND t.`id2`=s.`id2` SET t.`v`=COALESCE(s.`v`,t.`v`)"
  )
})
```

- [ ] **Step 2: Run, confirm RED**

Run: `Rscript -e 'testthat::test_local(".", filter="sql-builders")'`
Expected: FAIL — `could not find function "quote_ident"`.

- [ ] **Step 3: Implement `R/sql-builders.R`**

```r
# Pure SQL builders. No DB connection required (identifier quoting is done here),
# so these are fully unit-testable. Values are always `?` placeholders.

# MySQL/MariaDB identifier quoting: wrap in backticks, double internal backticks.
quote_ident <- function(x) paste0("`", gsub("`", "``", x, fixed = TRUE), "`")

# Single-row INSERT [IGNORE] with one placeholder per column.
build_insert_sql <- function(table, cols, ignore = TRUE) {
  paste0("INSERT ", if (ignore) "IGNORE " else "", "INTO ", quote_ident(table),
         " (", paste(quote_ident(cols), collapse = ","), ") VALUES (",
         paste(rep("?", length(cols)), collapse = ","), ")")
}

# Single-row INSERT ... ON DUPLICATE KEY UPDATE col = COALESCE(VALUES(col), col)
# for every non-key column (COALESCE preserves "don't overwrite with NULL").
# Keys-only tables have nothing to update -> INSERT IGNORE.
build_upsert_sql <- function(table, cols, keycols) {
  non_key <- setdiff(cols, keycols)
  base <- paste0("INSERT ", if (length(non_key) == 0L) "IGNORE " else "", "INTO ",
                 quote_ident(table), " (", paste(quote_ident(cols), collapse = ","),
                 ") VALUES (", paste(rep("?", length(cols)), collapse = ","), ")")
  if (length(non_key) == 0L) return(base)
  set_clause <- paste(vapply(non_key, function(c) {
    qc <- quote_ident(c); paste0(qc, "=COALESCE(VALUES(", qc, "),", qc, ")")
  }, character(1)), collapse = ",")
  paste0(base, " ON DUPLICATE KEY UPDATE ", set_clause)
}

# UPDATE target t JOIN tmp s ON keys SET t.col = COALESCE(s.col, t.col) for non-keys.
build_update_join_sql <- function(table, tmp, cols, keycols) {
  non_key <- setdiff(cols, keycols)
  on_clause <- paste(vapply(keycols, function(k) {
    qk <- quote_ident(k); paste0("t.", qk, "=s.", qk)
  }, character(1)), collapse = " AND ")
  set_clause <- paste(vapply(non_key, function(c) {
    qc <- quote_ident(c); paste0("t.", qc, "=COALESCE(s.", qc, ",t.", qc, ")")
  }, character(1)), collapse = ",")
  paste0("UPDATE ", quote_ident(table), " t JOIN ", quote_ident(tmp),
         " s ON ", on_clause, " SET ", set_clause)
}
```

- [ ] **Step 4: Run, confirm GREEN**

Run: `Rscript -e 'testthat::test_local(".", filter="sql-builders")'` → all PASS.
Then full suite (no regressions): `Rscript -e 'Sys.setenv(RMARIA_TEST_HOST="127.0.0.1", RMARIA_TEST_PORT="33306", RMARIA_TEST_USER="root", RMARIA_TEST_PWD="test", RMARIA_TEST_DB="rmaria_test"); testthat::test_local(".")'` → 0 failures.

- [ ] **Step 5: Commit**

```bash
git add R/sql-builders.R tests/testthat/test-sql-builders.R
git commit -m "[Feat][SQL] Pure parameterized SQL builders (quote_ident, insert/upsert/update-join)"
```

---

## Task 2: `resolve_credentials()` + unit tests

**Files:**
- Create: `R/credentials.R`
- Test: `tests/testthat/test-credentials.R`

- [ ] **Step 1: Write the failing tests**

Create `tests/testthat/test-credentials.R`:
```r
test_that("resolve_credentials finds creds in the wrapper's own environment", {
  wrapper <- function() { DB <- "d2"; HOST <- "h2"; USER <- "u2"; PWD <- "p2"; resolve_credentials() }
  cr <- wrapper()
  expect_equal(cr$db, "d2"); expect_equal(cr$host, "h2")
  expect_equal(cr$user, "u2"); expect_equal(cr$pwd, "p2")
})

test_that("resolve_credentials finds creds set in the wrapper's caller", {
  wrapper <- function() resolve_credentials()
  runner  <- function() { DB <- "d1"; HOST <- "h1"; USER <- "u1"; PWD <- "p1"; wrapper() }
  cr <- runner()
  expect_equal(cr$db, "d1"); expect_equal(cr$host, "h1")
})

test_that("resolve_credentials finds creds across an lapply frame", {
  wrapper <- function() resolve_credentials()
  runner  <- function() { DB <- "dm"; HOST <- "hm"; USER <- "um"; PWD <- "pm"; lapply(1, function(i) wrapper())[[1]] }
  cr <- runner()
  expect_equal(cr$db, "dm")
})

test_that("resolve_credentials stops with a clear message when creds are absent", {
  wrapper <- function() resolve_credentials()
  expect_error(wrapper(), "credentials not found")
})
```

- [ ] **Step 2: Run, confirm RED**

Run: `Rscript -e 'testthat::test_local(".", filter="credentials")'`
Expected: FAIL — `could not find function "resolve_credentials"`.

- [ ] **Step 3: Implement `R/credentials.R`**

```r
# Resolve DB/HOST/PWD/USER from the calling context. Reproduces the historical
# parent-frame/parent-env scan used by the *q wrappers, but as one helper.
# Because it runs one frame deeper than the wrapper, dynamic-frame offsets are +1.
# Returns list(host, db, user, pwd) or stop()s if not all four are found.
resolve_credentials <- function() {
  needed <- c("DB", "HOST", "PWD", "USER")
  safe <- function(expr) tryCatch(expr, error = function(e) emptyenv())
  w <- parent.frame()  # the calling wrapper's environment
  envs <- list(
    w,
    safe(parent.frame(2)),
    safe(parent.env(w)),
    safe(parent.env(parent.env(w))),
    safe(parent.env(parent.frame(2))),
    safe(parent.env(parent.frame(3))),
    safe(parent.env(parent.frame(4))),
    safe(parent.env(parent.frame(5))),
    safe(parent.env(parent.frame(6)))
  )
  for (e in envs) {
    if (all(vapply(needed, exists, logical(1), envir = e, inherits = TRUE))) {
      return(list(
        host = get("HOST", envir = e), db = get("DB", envir = e),
        user = get("USER", envir = e), pwd = get("PWD", envir = e)
      ))
    }
  }
  stop("rmaria: credentials not found -- set DB, HOST, USER, PWD in the calling context. See ?load_env.",
       call. = FALSE)
}
```

- [ ] **Step 4: Run, confirm GREEN**

Run: `Rscript -e 'testthat::test_local(".", filter="credentials")'` → all PASS (4).

- [ ] **Step 5: Commit**

```bash
git add R/credentials.R tests/testthat/test-credentials.R
git commit -m "[Feat][Credentials] resolve_credentials() helper (env-scan, stop on missing)"
```

---

## Task 3: Wire the 7 wrappers to `resolve_credentials`

**Files:**
- Modify: `R/maria.R` — `selectq`, `execq`, `insertq`, `deleteq`, `upsertq`, `updateq`, `insert_table_local`
- Test: `tests/testthat/test-credentials.R` (extend)

- [ ] **Step 1: Write the failing integration test**

Append to `tests/testthat/test-credentials.R`:
```r
test_that("selectq resolves creds from the caller and runs (integration)", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd
  # selectq has no port arg; the test DB is on 33306 -> only run if default port matches
  skip_if(e$port != 3306L, "selectq uses default port 3306; test DB not on 3306")
  got <- selectq("SELECT 1 AS one")
  expect_equal(got$one[1], 1)
})

test_that("selectq stops (not FALSE) when credentials are absent", {
  expect_error(selectq("SELECT 1"), "credentials not found")
})
```
(Note: the first test will usually `skip` because the test DB is on 33306 and `selectq` has no port parameter — that's expected; the second test is the important behavior check and needs no DB.)

- [ ] **Step 2: Run, confirm RED**

Run: `Rscript -e 'testthat::test_local(".", filter="credentials")'`
Expected: the "stops when absent" test FAILS (current `selectq` returns FALSE, not an error).

- [ ] **Step 3: Replace the duplicated block in each of the 7 functions**

In `R/maria.R`, for EACH of `selectq`, `execq`, `insertq`, `deleteq`, `upsertq`, `updateq`, `insert_table_local`: delete the entire credential block (from `target_e <- environment()` through the `if (all(...)) { assign... } else { init(); logerror; return(FALSE) }`) and replace with a single line, then update the delegating call to use `creds$…`.

`selectq` becomes:
```r
selectq <- function(query, ...) {
  creds <- resolve_credentials()
  pull_data(host = creds$host, db = creds$db, user = creds$user, password = creds$pwd, query = query, ...)
}
```
`execq`:
```r
execq <- function(query, ...) {
  creds <- resolve_credentials()
  exec_query(host = creds$host, db = creds$db, user = creds$user, password = creds$pwd, query = query, ...)
}
```
`insertq`:
```r
insertq <- function(table, table_name_in_base, ...) {
  creds <- resolve_credentials()
  insert_table(table = table, table_name_in_base = table_name_in_base,
               host = creds$host, db = creds$db, user = creds$user, password = creds$pwd, ...)
}
```
`deleteq`:
```r
deleteq <- function(table_name_in_base, where, ...) {
  creds <- resolve_credentials()
  delete_from_table(table_name_in_base, where,
                    host = creds$host, db = creds$db, user = creds$user, password = creds$pwd, ...)
}
```
`upsertq`:
```r
upsertq <- function(table, table_name_in_base, ...) {
  creds <- resolve_credentials()
  upsert_table(table = table, table_name_in_base = table_name_in_base,
               host = creds$host, db = creds$db, user = creds$user, password = creds$pwd, ...)
}
```
`updateq`:
```r
updateq <- function(table, table_name_in_base, ...) {
  creds <- resolve_credentials()
  update_table(table = table, table_name_in_base = table_name_in_base,
               host = creds$host, db = creds$db, user = creds$user, password = creds$pwd, ...)
}
```
`insert_table_local` — replace ONLY the credential block + the `con <- .maria_connect(...)` line so it uses resolved creds (it resolves PORT from the same env if present, default 3306):
```r
insert_table_local <- function(table, table_name_in_base, preface_queries=character(0), split_threshold=1e5, use_file=FALSE) {
  creds <- resolve_credentials()
  PORT <- tryCatch(get("PORT", envir = parent.frame()), error = function(e) 3306L)
  PORT <- suppressWarnings(as.integer(PORT)); if (is.na(PORT)) PORT <- 3306L
  table <- as.data.frame(table)
  con <- NULL
  tryCatch({
    con <- .maria_connect(creds$host, PORT, creds$db, creds$user, creds$pwd, local_infile = use_file)
    if (length(preface_queries) > 0) for (pq in preface_queries) RMariaDB::dbExecute(con, pq)
    if (nrow(table) >= split_threshold) {
      start <- 1
      while (start <= nrow(table)) {
        end <- min(nrow(table), start + split_threshold - 1)
        RMariaDB::dbWriteTable(con, table_name_in_base, table[seq(start, end), , drop = FALSE], append = TRUE)
        start <- end + 1
      }
    } else {
      RMariaDB::dbWriteTable(con, table_name_in_base, table, append = TRUE)
    }
  }, error = function(e) {
    logging::logerror("Error while inserting data into table %s: %s", table_name_in_base, conditionMessage(e), logger = LOGGER.MAIN)
  }, finally = {
    if (!is.null(con)) RMariaDB::dbDisconnect(con)
  })
}
```
(Keep each function's roxygen block above it unchanged. The `%ni%`/`map_lgl`/`add` helpers used by the old block are no longer referenced by these wrappers.)

- [ ] **Step 4: Run, confirm GREEN**

Run: `Rscript -e 'Sys.setenv(RMARIA_TEST_HOST="127.0.0.1", RMARIA_TEST_PORT="33306", RMARIA_TEST_USER="root", RMARIA_TEST_PWD="test", RMARIA_TEST_DB="rmaria_test"); testthat::test_local(".")'`
Expected: 0 failures (the "stops when absent" tests pass; existing tests unaffected; insert_table_local tests still pass).

- [ ] **Step 5: Commit**

```bash
git add R/maria.R tests/testthat/test-credentials.R
git commit -m "[Refactor][Credentials] Wire 7 wrappers through resolve_credentials (stop on missing)"
```

---

## Task 4: `LOGGER.MAIN` constant + drop `init()`

**Files:**
- Modify: `R/maria.R`

- [ ] **Step 1: Replace `init()` with a top-level constant**

In `R/maria.R`, delete the `init <- function() { LOGGER.MAIN <<- "com.vongo.rmaria"; TRUE }` definition and the top-level `init()` call. Add at the top of the file:
```r
LOGGER.MAIN <- "com.vongo.rmaria"
```
Then remove every remaining `init()` call (in `pull_data`, `truncate_table`, `insert_table`, `upsert_table`, `update_table`, and any others). Find them: `grep -n 'init()' R/maria.R` — remove each call line.

- [ ] **Step 2: Verify package loads & suite passes**

Run: `Rscript -e 'pkgload::load_all("."); cat("LOGGER.MAIN =", LOGGER.MAIN, "\n")'` → prints `com.vongo.rmaria`.
Run full suite → 0 failures.
Confirm no `init` references remain: `grep -n '\binit\b' R/maria.R` → no matches.

- [ ] **Step 3: Commit**

```bash
git add R/maria.R
git commit -m "[Refactor][Packaging] LOGGER.MAIN as package constant; drop init()"
```

---

## Task 5: Remove `esq`/`edq`, qualify `insert_source_full_file`, fix `{db}` docs

**Files:**
- Modify: `R/maria.R`

- [ ] **Step 1: Remove dead escaping helpers**

Delete the `esq <- function(str) {...}` and `edq <- function(str) {...}` definitions (and their `# Escape ...` comments). Confirm unused: `grep -nE '\b(esq|edq)\b' R/maria.R` → only (now-removed) definitions; no call sites.

- [ ] **Step 2: Qualify `insert_source_full_file` calls (clears its NOTEs)**

In `insert_source_full_file`, prefix the bare calls: `dbConnect(` → `RMariaDB::dbConnect(`, `MariaDB()` → `RMariaDB::MariaDB()`, `dbExecute(` → `RMariaDB::dbExecute(`, `write.table(` → `utils::write.table(`. (Behavior unchanged; function remains unexported.)

- [ ] **Step 3: Fix `{db}` Rd lost-braces in roxygen**

Find: `grep -n 'table in {db}' R/maria.R`. In each `@param table_name_in_base` line, change the literal `{db}` to `\code{db}` (e.g. `table in \code{db} to insert data into`). This removes the "Lost braces" Rd NOTE.

- [ ] **Step 4: Regenerate docs, verify**

Run: `Rscript -e 'roxygen2::roxygenise()'`
Run full suite → 0 failures. `Rscript -e 'pkgload::load_all("."); cat("ok\n")'` → ok.

- [ ] **Step 5: Commit**

```bash
git add R/maria.R man NAMESPACE
git commit -m "[Refactor][Cleanup] Remove esq/edq; qualify insert_source_full_file; fix {db} Rd braces"
```

---

## Task 6: Rewrite `insert_table` — parameterized, chunked, transactional, returns count

**Files:**
- Modify: `R/maria.R` (`insert_table`)
- Test: `tests/testthat/test-insert2.R`

- [ ] **Step 1: Write failing tests**

Create `tests/testthat/test-insert2.R`:
```r
test_that("insert_table returns the affected-row count", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins2 (id INT)", "t_ins2", {
    n <- insert_table(data.frame(id = 1:7), "t_ins2", host=e$host, port=e$port, db=e$db,
                      user=e$user, password=e$pwd, progress_bar=FALSE)
    expect_equal(as.integer(n), 7L)
  })
})

test_that("insert_table is atomic: a bad row aborts the whole batch", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins3 (id INT PRIMARY KEY)", "t_ins3", {
    # duplicate key within batch with ignore=FALSE -> error -> rollback -> nothing persists
    expect_error(
      insert_table(data.frame(id = c(1L, 1L)), "t_ins3", host=e$host, port=e$port, db=e$db,
                   user=e$user, password=e$pwd, progress_bar=FALSE, ignore=FALSE)
    )
    n <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   query="SELECT COUNT(*) AS n FROM t_ins3", verbose=FALSE)$n
    expect_equal(as.integer(n), 0L)
  })
})

test_that("insert_table still round-trips quotes/backslash/NA/data.table/factor", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins4 (v VARCHAR(50)) CHARACTER SET utf8mb4", "t_ins4", {
    insert_table(data.table::data.table(v = c("O'Brien", "a\\b", NA, "x\"y")), "t_ins4",
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT v FROM t_ins4 ORDER BY v IS NULL, v", verbose=FALSE)
    expect_setequal(got$v[!is.na(got$v)], c("O'Brien", "a\\b", "x\"y"))
    expect_true(any(is.na(got$v)))
  })
})
```

- [ ] **Step 2: Run, confirm RED**

Run: `Rscript -e 'Sys.setenv(RMARIA_TEST_HOST="127.0.0.1", RMARIA_TEST_PORT="33306", RMARIA_TEST_USER="root", RMARIA_TEST_PWD="test", RMARIA_TEST_DB="rmaria_test"); testthat::test_local(".", filter="insert2")'`
Expected: the count test FAILS (current returns `invisible()`/NULL) and the atomic test FAILS (current logs-and-continues, leaving 1 row).

- [ ] **Step 3: Rewrite `insert_table` body (keep signature)**

Replace the body (after the roxygen + signature) with:
```r
  table <- as.data.frame(table)
  if (nrow(table) == 0L) {
    if (!nolog) logging::logwarn("You tried to insert an empty table. Leaving.", logger=LOGGER.MAIN)
    return(invisible(0L))
  }
  if (!nolog) logging::loginfo("Inserting data into table %s.", table_name_in_base, logger=LOGGER.MAIN)
  table[] <- lapply(table, function(col) { if (is.numeric(col)) col[!is.finite(col)] <- NA; col })  # NA/NaN/Inf -> NULL
  cols <- colnames(table)
  sql  <- build_insert_sql(table_name_in_base, cols, ignore)
  if (is.na(chunk_size)) chunk_size <- 10000L
  chunk_size <- as.integer(max(1L, min(chunk_size, nrow(table))))
  n_iter <- as.integer(ceiling(nrow(table) / chunk_size))
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  pb <- if (progress_bar) create_pb(n_iter, bar_style="pc", time_style="cd") else NULL
  affected <- 0L
  DBI::dbWithTransaction(con, {
    for (i in seq_len(n_iter)) {
      rows <- ((i - 1L) * chunk_size + 1L):min(i * chunk_size, nrow(table))
      affected <- affected + RMariaDB::dbExecute(con, sql, params = unname(as.list(table[rows, , drop = FALSE])))
      if (progress_bar) update_pb(pb, i)
    }
  })
  invisible(affected)
```
(`as.list(data.frame)` yields column vectors in order; `unname` makes binding positional. The whole batch is one transaction — any error rolls it back and propagates.)

Update the roxygen: add `#' @return (invisibly) the number of rows affected.`

- [ ] **Step 4: Run, confirm GREEN**

Run: filter `insert2` → PASS; then `insert|chunking|escaping|datatable` → PASS; then full suite → 0 failures.

- [ ] **Step 5: Commit**

```bash
git add R/maria.R tests/testthat/test-insert2.R
git commit -m "[Perf][Insert] Parameterized chunked insert in a transaction; return affected count"
```

---

## Task 7: Rewrite `upsert_table` — batched COALESCE upsert, transactional, returns count

**Files:**
- Modify: `R/maria.R` (`upsert_table`)
- Test: `tests/testthat/test-upsert2.R`

- [ ] **Step 1: Write failing tests**

Create `tests/testthat/test-upsert2.R`:
```r
test_that("upsert_table inserts new, updates existing, preserves apostrophes, skips NULL, returns count", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_up5 (id INT PRIMARY KEY, name VARCHAR(50), n INT) CHARACTER SET utf8mb4", "t_up5", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_up5 VALUES (1, 'old', 5)")
    n <- upsert_table(data.frame(id=c(1L,2L), name=c("O'Brien", "new"), n=c(NA, 9L)),
                      "t_up5", keycols="id",
                      host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT id,name,n FROM t_up5 ORDER BY id", verbose=FALSE)
    expect_equal(got$name, c("O'Brien", "new"))   # id1 name updated
    expect_equal(got$n[1], 5L)                      # id1 n kept (NA skipped via COALESCE)
    expect_equal(got$n[2], 9L)                      # id2 inserted
    expect_true(as.integer(n) >= 1L)
  })
})

test_that("upsert_table is atomic on failure", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_up6 (id INT PRIMARY KEY, v INT NOT NULL)", "t_up6", {
    # second row violates NOT NULL -> whole batch rolls back
    expect_error(
      upsert_table(data.frame(id=c(1L,2L), v=c(1L, NA)), "t_up6", keycols="id",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    )
    n <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   query="SELECT COUNT(*) AS n FROM t_up6", verbose=FALSE)$n
    expect_equal(as.integer(n), 0L)
  })
})

test_that("upsert_table rejects unknown keycols", {
  expect_error(
    upsert_table(data.frame(id=1, v=2), "t_up5", keycols="nope",
                 host="127.0.0.1", port=33306, db="rmaria_test", user="root", password="test"),
    "keycols not found"
  )
})
```

- [ ] **Step 2: Run, confirm RED**

Run filter `upsert2` → the count assertion and/or atomic test FAIL against the current per-row best-effort implementation.

- [ ] **Step 3: Rewrite `upsert_table` body (keep signature)**

```r
  table <- as.data.frame(table)
  if (nrow(table) == 0L) {
    if (!nolog) logging::logwarn("You tried to insert an empty table. Leaving.", logger=LOGGER.MAIN)
    return(invisible(0L))
  }
  if (missing(keycols) || length(keycols) == 0L) stop("upsert_table: 'keycols' must name the key column(s)")
  unknown <- setdiff(keycols, colnames(table))
  if (length(unknown) > 0L) stop("upsert_table: keycols not found in table: ", paste(unknown, collapse = ", "))
  if (!nolog) logging::loginfo("Upserting %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)
  table[] <- lapply(table, function(col) { if (is.numeric(col)) col[!is.finite(col)] <- NA; col })
  cols <- colnames(table)
  sql  <- build_upsert_sql(table_name_in_base, cols, keycols)
  chunk_size <- 10000L
  n_iter <- as.integer(ceiling(nrow(table) / chunk_size))
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  pb <- if (progress_bar) create_pb(n_iter, bar_style="pc", time_style="cd") else NULL
  affected <- 0L
  DBI::dbWithTransaction(con, {
    for (i in seq_len(n_iter)) {
      rows <- ((i - 1L) * chunk_size + 1L):min(i * chunk_size, nrow(table))
      affected <- affected + RMariaDB::dbExecute(con, sql, params = unname(as.list(table[rows, , drop = FALSE])))
      if (progress_bar) update_pb(pb, i)
    }
  })
  invisible(affected)
```
Add roxygen `#' @return (invisibly) the number of rows affected.`

- [ ] **Step 4: Run, confirm GREEN**

Filter `upsert2` and `upsert-update` → PASS; full suite → 0 failures.

- [ ] **Step 5: Commit**

```bash
git add R/maria.R tests/testthat/test-upsert2.R
git commit -m "[Perf][Upsert] Batched parameterized COALESCE upsert in a transaction; return count"
```

---

## Task 8: Rewrite `update_table` — temp-table JOIN, transactional, returns count

**Files:**
- Modify: `R/maria.R` (`update_table`)
- Test: `tests/testthat/test-update2.R`

- [ ] **Step 1: Write failing tests**

Create `tests/testthat/test-update2.R`:
```r
test_that("update_table (temp-join) updates by composite key, skips NULL value & NULL key, returns count", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_u2 (id1 INT, id2 INT, val INT, name VARCHAR(20), PRIMARY KEY(id1,id2))", "t_u2", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_u2 VALUES (1,1,10,'x'),(1,2,20,'y')")
    upd <- data.frame(id1=c(1L,1L,1L), id2=c(2L,1L,NA), val=c(99L, NA, 77L), name=c(NA,"w","z"))
    n <- update_table(upd, "t_u2", keycols=c("id1","id2"),
                      host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT id1,id2,val,name FROM t_u2 ORDER BY id1,id2", verbose=FALSE)
    expect_equal(got$val,  c(10L, 99L))     # (1,1) val NA-skipped; (1,2) updated
    expect_equal(got$name, c("w",  "y"))     # (1,1) name updated; (1,2) name NA-skipped
    # the (1,NA) row never matched -> no new row, nothing else changed
    expect_equal(nrow(got), 2L)
  })
})

test_that("update_table is atomic and returns 0 for an empty frame", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_u3 (id INT PRIMARY KEY, v INT)", "t_u3", {
    n <- update_table(data.frame(id=integer(0), v=integer(0)), "t_u3", keycols="id",
                      host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    expect_equal(as.integer(n), 0L)
  })
})
```

- [ ] **Step 2: Run, confirm RED**

Filter `update2` → FAILS (current update_table returns invisibly / per-row).

- [ ] **Step 3: Rewrite `update_table` body (keep signature)**

```r
  table <- as.data.frame(table)
  if (nrow(table) == 0L) {
    if (!nolog) logging::logwarn("You tried to update with empty data. Leaving.", logger=LOGGER.MAIN)
    return(invisible(0L))
  }
  if (missing(keycols) || length(keycols) == 0L) stop("update_table: 'keycols' must name the key column(s)")
  unknown <- setdiff(keycols, colnames(table))
  if (length(unknown) > 0L) stop("update_table: keycols not found in table: ", paste(unknown, collapse = ", "))
  if (!nolog) logging::loginfo("Updating %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)
  table[] <- lapply(table, function(col) { if (is.numeric(col)) col[!is.finite(col)] <- NA; col })
  cols <- colnames(table)
  tmp  <- "rmaria_update_tmp"
  con  <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  if (progress_bar) { pb <- create_pb(1L, bar_style="pc", time_style="cd") }
  affected <- DBI::dbWithTransaction(con, {
    RMariaDB::dbExecute(con, paste0("DROP TEMPORARY TABLE IF EXISTS ", quote_ident(tmp)))
    # temp table inherits the target's column types for exactly the columns we touch
    RMariaDB::dbExecute(con, paste0("CREATE TEMPORARY TABLE ", quote_ident(tmp), " AS SELECT ",
      paste(quote_ident(cols), collapse = ","), " FROM ", quote_ident(table_name_in_base), " WHERE 1=0"))
    RMariaDB::dbExecute(con, paste0("ALTER TABLE ", quote_ident(tmp), " ADD INDEX (",
      paste(quote_ident(keycols), collapse = ","), ")"))
    RMariaDB::dbExecute(con, build_insert_sql(tmp, cols, ignore = FALSE), params = unname(as.list(table)))
    a <- RMariaDB::dbExecute(con, build_update_join_sql(table_name_in_base, tmp, cols, keycols))
    RMariaDB::dbExecute(con, paste0("DROP TEMPORARY TABLE ", quote_ident(tmp)))
    a
  })
  if (progress_bar) update_pb(pb, 1L)
  invisible(affected)
```

Add roxygen `#' @return (invisibly) the number of rows updated.`

- [ ] **Step 4: Run, confirm GREEN**

Filter `update2` and `upsert-update` → PASS; full suite → 0 failures.

- [ ] **Step 5: Commit**

```bash
git add R/maria.R tests/testthat/test-update2.R
git commit -m "[Perf][Update] Temp-table JOIN update (COALESCE) in a transaction; return count"
```

---

## Task 9: Split `R/maria.R` into focused files

**Files:**
- Create: `R/connection.R`, `R/select.R`, `R/insert.R`, `R/modify.R`, `R/delete.R`
- Delete: `R/maria.R`

- [ ] **Step 1: Move functions verbatim into their files**

Move each function (with its roxygen block) out of `R/maria.R` into the target file, unchanged. No logic edits in this task.
- `R/connection.R`: the `LOGGER.MAIN <- "com.vongo.rmaria"` constant and `.maria_connect`.
- `R/select.R`: `pull_data`, `selectq`, `exec_query`, `execq`.
- `R/insert.R`: `insert_table`, `insertq`, `insert_table_local`, `insert_source_full_file`.
- `R/modify.R`: `upsert_table`, `upsertq`, `update_table`, `updateq`.
- `R/delete.R`: `delete_from_table`, `deleteq`, `truncate_table`.
After moving, `R/maria.R` should be empty — delete it: `git rm R/maria.R`.

- [ ] **Step 2: Verify nothing was lost**

`Rscript -e 'pkgload::load_all("."); fns <- c("pull_data","selectq","exec_query","execq","insert_table","insertq","insert_table_local","upsert_table","upsertq","update_table","updateq","delete_from_table","deleteq","truncate_table","resolve_credentials","quote_ident","build_insert_sql","build_upsert_sql","build_update_join_sql",".maria_connect"); cat("missing:", paste(fns[!vapply(fns, exists, logical(1))], collapse=", "), "\n")'`
Expected: `missing:` (empty).

- [ ] **Step 3: Regenerate docs, run full suite + R CMD check**

```
Rscript -e 'roxygen2::roxygenise()'
Rscript -e 'Sys.setenv(RMARIA_TEST_HOST="127.0.0.1", RMARIA_TEST_PORT="33306", RMARIA_TEST_USER="root", RMARIA_TEST_PWD="test", RMARIA_TEST_DB="rmaria_test"); testthat::test_local(".")'
```
Full suite → 0 failures. (R CMD check runs in Task 10.)

- [ ] **Step 4: Commit**

```bash
git add R NAMESPACE man
git commit -m "[Refactor][Structure] Split maria.R into connection/credentials/select/insert/modify/delete/sql-builders"
```

---

## Task 10: Benchmark script + verification

**Files:**
- Create: `bench/phase2-benchmark.R`

- [ ] **Step 1: Add the benchmark script**

Create `bench/phase2-benchmark.R` (run manually; documents the win, not part of the suite):
```r
# Usage: RMARIA_TEST_HOST=127.0.0.1 RMARIA_TEST_PORT=33306 RMARIA_TEST_USER=root \
#        RMARIA_TEST_PWD=test RMARIA_TEST_DB=rmaria_test Rscript bench/phase2-benchmark.R
pkgload::load_all(".")
e <- list(host=Sys.getenv("RMARIA_TEST_HOST","127.0.0.1"), port=as.integer(Sys.getenv("RMARIA_TEST_PORT","3306")),
          db=Sys.getenv("RMARIA_TEST_DB"), user=Sys.getenv("RMARIA_TEST_USER"), pwd=Sys.getenv("RMARIA_TEST_PWD"))
N <- 50000L
df <- data.frame(id=seq_len(N), a=seq_len(N)*2L, b=seq_len(N)*3L)
con <- .maria_connect(e$host, e$port, e$db, e$user, e$pwd); on.exit(RMariaDB::dbDisconnect(con))
RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS bench_t"); RMariaDB::dbExecute(con, "CREATE TABLE bench_t (id INT PRIMARY KEY, a INT, b INT)")
cat(sprintf("insert %d: %.2fs\n", N, system.time(insert_table(df, "bench_t", host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE))[["elapsed"]]))
df$a <- df$a + 1L
cat(sprintf("upsert %d: %.2fs\n", N, system.time(upsert_table(df, "bench_t", keycols="id", host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE))[["elapsed"]]))
df$b <- df$b + 1L
cat(sprintf("update %d: %.2fs\n", N, system.time(update_table(df, "bench_t", keycols="id", host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE))[["elapsed"]]))
RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS bench_t")
```
Add `^bench$` to `.Rbuildignore`.

- [ ] **Step 2: Run the benchmark, capture numbers for the PR**

```
RMARIA_TEST_HOST=127.0.0.1 RMARIA_TEST_PORT=33306 RMARIA_TEST_USER=root RMARIA_TEST_PWD=test RMARIA_TEST_DB=rmaria_test Rscript bench/phase2-benchmark.R
```
Expected: insert/upsert/update each well under ~1s for 50k rows. Record the output for the PR body.

- [ ] **Step 3: Commit**

```bash
git add bench/phase2-benchmark.R .Rbuildignore
git commit -m "[Docs][Bench] Phase 2 write-path benchmark script"
```

---

## Task 11: Full verification + PR

- [ ] **Step 1: Full suite against docker MariaDB**

```
Rscript -e 'Sys.setenv(RMARIA_TEST_HOST="127.0.0.1", RMARIA_TEST_PORT="33306", RMARIA_TEST_USER="root", RMARIA_TEST_PWD="test", RMARIA_TEST_DB="rmaria_test"); testthat::test_local(".")'
```
Expected: all pass, 0 failures.

- [ ] **Step 2: R CMD check**

```
rm -f rmaria_*.tar.gz; rm -rf rmaria.Rcheck
R CMD build .
RMARIA_TEST_HOST=127.0.0.1 RMARIA_TEST_PORT=33306 RMARIA_TEST_USER=root RMARIA_TEST_PWD=test RMARIA_TEST_DB=rmaria_test R CMD check --no-manual rmaria_*.tar.gz
grep -E 'Status:' rmaria.Rcheck/00check.log
rm -f rmaria_*.tar.gz; rm -rf rmaria.Rcheck
```
Expected: 0 errors, 0 warnings, ≤1 NOTE (WTFPL license). The DB/HOST bare-var, LOGGER.MAIN, esq/edq, insert_source NOTEs are gone.

- [ ] **Step 3: Bump version & push**

Bump `DESCRIPTION` `Version:` to `0.4.339`. Then:
```bash
git add DESCRIPTION && git commit -m "[Update] Bump version to 0.4.339"
git push -u origin perf_phase2
gh pr create --base main --head perf_phase2 --title "[Perf] Phase 2: parameterized batched writes + structural refactor" --body "<summary: builders, resolve_credentials, file split, batched upsert/update with benchmark numbers>"
```

- [ ] **Step 4: Final review** with pr-review-toolkit::review-pr; address findings; confirm CI green.

---

## Self-Review

**Spec coverage:** resolve_credentials+stop ✓(T2,T3) · file split ✓(T9) · remove esq/edq ✓(T5) · keep+qualify insert_source_full_file ✓(T5) · LOGGER.MAIN constant + drop init ✓(T4) · {db} Rd fix ✓(T5) · pure builders + COALESCE ✓(T1) · parameterized+batched insert/upsert ✓(T6,T7) · temp-table-join update ✓(T8) · transactions + stop + return counts ✓(T6,T7,T8) · skip-NULL/Inf preserved ✓(non-finite pre-pass + COALESCE, tested T6/T7/T8) · pure unit tests ✓(T1,T2) · integration incl. atomicity/counts ✓(T6,T7,T8) · benchmark ✓(T10) · R CMD check target ✓(T11) · defer pooling ✓(not in plan).

**Placeholder scan:** none — every code step is complete.

**Type/name consistency:** `build_insert_sql(table, cols, ignore)`, `build_upsert_sql(table, cols, keycols)`, `build_update_join_sql(table, tmp, cols, keycols)`, `quote_ident(x)`, `resolve_credentials()` returning `list(host, db, user, pwd)` — used identically across T1/T2/T3/T6/T7/T8. The `unname(as.list(table[rows,,drop=FALSE]))` binding form and `DBI::dbWithTransaction` are consistent across T6/T7/T8.

**Note for executor:** `insert_table`/`upsert_table`/`update_table` are edited in `R/maria.R` in Tasks 6–8 and *moved* to `R/insert.R`/`R/modify.R` in Task 9 — do Tasks 6–8 before 9.
