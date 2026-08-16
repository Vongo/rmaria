# A bad value must not be silently coerced just because it sits in row 2+ of a batch.
#
# MariaDB downgrades error -> warning for an invalid value in the SECOND OR LATER row of a
# multi-row statement. Per-row execution never met that rule, so batching quietly converted a
# raised error into a coerced value plus a full success count. The warnings that record it die
# with the connection, so nothing reaches the caller.
#
# It needs a target where strictness does not apply: a non-transactional engine (STRICT_TRANS_TABLES
# only makes TRANSACTIONAL tables strict), or a server without STRICT_* at all. InnoDB under the
# default mode is unaffected, which is why the rest of the suite never saw this.
#
# See Vongo/rmaria#7.

.badrow_df <- function() {
  # Bad value deliberately in row 2 of 3 -- row 1 would raise even in a multi-row statement.
  data.frame(id = 1:3, v = c("ok", "WAY-TOO-LONG", "ok3"), stringsAsFactors = FALSE)
}

test_that("a too-long value in row 2 raises on a non-transactional engine", {
  tbl <- "rmaria_strict_myisam"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(3), PRIMARY KEY (id)) ENGINE=MyISAM", tbl),
    tbl, {
      e <- db_env()
      expect_error(
        upsert_table(.badrow_df(), tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      )
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      # MyISAM cannot roll back, so rows 1 (and possibly 3) may remain -- what must NOT happen
      # is a truncated "WAY" being stored as though it were the caller's data.
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT v FROM `%s` WHERE id = 2", tbl))
      expect_equal(nrow(got), 0L)
    })
})

test_that("a NULL into NOT NULL in row 2 raises on a non-transactional engine", {
  tbl <- "rmaria_strict_notnull"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(8) NOT NULL, PRIMARY KEY (id)) ENGINE=MyISAM", tbl),
    tbl, {
      e <- db_env()
      df <- data.frame(id = 1:3, v = c("ok", NA, "ok3"), stringsAsFactors = FALSE)
      expect_error(
        upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      )
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT v FROM `%s` WHERE id = 2", tbl))
      expect_equal(nrow(got), 0L)   # not stored as ''
    })
})

test_that("an out-of-range value in row 2 raises on a non-transactional engine", {
  tbl <- "rmaria_strict_range"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, n TINYINT, PRIMARY KEY (id)) ENGINE=MyISAM", tbl),
    tbl, {
      e <- db_env()
      df <- data.frame(id = 1:3, n = c(1L, 9999L, 3L))
      expect_error(
        upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      )
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT n FROM `%s` WHERE id = 2", tbl))
      expect_equal(nrow(got), 0L)   # not clamped to 127
    })
})

test_that("clean data still writes correctly to a non-transactional engine", {
  # The safety fallback must not break the ordinary case, only slow it.
  tbl <- "rmaria_strict_clean"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(16), PRIMARY KEY (id)) ENGINE=MyISAM", tbl),
    tbl, {
      e <- db_env()
      df <- data.frame(id = 1:5, v = c("a", "b", "c", "d", "e"), stringsAsFactors = FALSE)
      affected <- upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                               user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      expect_equal(as.integer(affected), 5L)
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT id, v FROM `%s` ORDER BY id", tbl))
      expect_equal(got$id, 1:5)
      expect_equal(got$v, c("a", "b", "c", "d", "e"))   # correct rows, not transposed
    })
})

test_that("a transactional engine under strict mode is unaffected and still raises", {
  # The case that always worked; pinned so a future change to the safety check cannot
  # accidentally route InnoDB down a path that stops raising.
  tbl <- "rmaria_strict_innodb"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(3), PRIMARY KEY (id)) ENGINE=InnoDB", tbl),
    tbl, {
      e <- db_env()
      expect_error(
        upsert_table(.badrow_df(), tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      )
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT COUNT(*) c FROM `%s`", tbl))
      expect_equal(got$c, 0)   # transactional: whole thing rolls back
    })
})

# --- the safety predicate, as a pure decision ------------------------------------

test_that("batching is judged safe only when strictness actually applies", {
  # STRICT_ALL_TABLES makes every engine strict.
  expect_true(batching_preserves_errors("STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION", TRUE))
  expect_true(batching_preserves_errors("STRICT_ALL_TABLES", FALSE))
  # STRICT_TRANS_TABLES only reaches transactional tables.
  expect_true(batching_preserves_errors("STRICT_TRANS_TABLES", TRUE))
  expect_false(batching_preserves_errors("STRICT_TRANS_TABLES", FALSE))
  # No strict mode at all: never safe.
  expect_false(batching_preserves_errors("", TRUE))
  expect_false(batching_preserves_errors("NO_ZERO_DATE", TRUE))
  # Unknown engine (lookup failed) is treated as unsafe.
  expect_false(batching_preserves_errors("STRICT_TRANS_TABLES", NA))
})

test_that("the safe path batches and the unsafe path does not -- counted, not timed", {
  # Round-trip count is the thing batching actually buys, and unlike wall-clock it is
  # deterministic and machine-independent. This fails loudly if a future change either stops
  # batching InnoDB or starts batching a non-transactional engine.
  skip_if_no_db()
  e <- db_env()
  con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  stmts <- function() as.numeric(
    RMariaDB::dbGetQuery(con, "SHOW GLOBAL STATUS LIKE 'Com_stmt_execute'")$Value)

  counts <- vapply(c("InnoDB", "MyISAM"), function(eng) {
    tbl <- paste0("rmaria_rt_", eng)
    RMariaDB::dbExecute(con, sprintf("DROP TABLE IF EXISTS `%s`", tbl))
    RMariaDB::dbExecute(con, sprintf(
      "CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(16), PRIMARY KEY (id)) ENGINE=%s", tbl, eng))
    on.exit(RMariaDB::dbExecute(con, sprintf("DROP TABLE IF EXISTS `%s`", tbl)), add = TRUE)
    df <- data.frame(id = 1:500, v = "x", stringsAsFactors = FALSE)
    before <- stmts()
    upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                 user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
    stmts() - before
  }, numeric(1))

  # InnoDB: a handful (the upsert plus the safety/limit lookups). MyISAM: one per row.
  expect_lt(counts[["InnoDB"]], 50)
  expect_gte(counts[["MyISAM"]], 500)
})
