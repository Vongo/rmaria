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
      con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add = TRUE)
      # Precondition, not decoration: on a STRICT_ALL_TABLES server MyISAM is batched and
      # raises anyway, so without this the assertions below would pass without ever
      # exercising the fallback they exist to cover.
      expect_false(upsert_batching_is_safe(con0, tbl)$safe,
                   info = paste("This server judges", tbl, "safe to batch, so the fallback these",
                                "assertions cover is never reached. They need a server whose",
                                "sql_mode is STRICT_TRANS_TABLES (not STRICT_ALL_TABLES, which",
                                "covers every engine) -- see docker-compose.test.yml."))
      e <- db_env()
      expect_error(
        upsert_table(.badrow_df(), tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE),
        regexp = "Data too long"
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
      con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add = TRUE)
      # Precondition, not decoration: on a STRICT_ALL_TABLES server MyISAM is batched and
      # raises anyway, so without this the assertions below would pass without ever
      # exercising the fallback they exist to cover.
      expect_false(upsert_batching_is_safe(con0, tbl)$safe,
                   info = paste("This server judges", tbl, "safe to batch, so the fallback these",
                                "assertions cover is never reached. They need a server whose",
                                "sql_mode is STRICT_TRANS_TABLES (not STRICT_ALL_TABLES, which",
                                "covers every engine) -- see docker-compose.test.yml."))
      e <- db_env()
      df <- data.frame(id = 1:3, v = c("ok", NA, "ok3"), stringsAsFactors = FALSE)
      expect_error(
        upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE),
        regexp = "cannot be null"
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
      con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add = TRUE)
      # Precondition, not decoration: on a STRICT_ALL_TABLES server MyISAM is batched and
      # raises anyway, so without this the assertions below would pass without ever
      # exercising the fallback they exist to cover.
      expect_false(upsert_batching_is_safe(con0, tbl)$safe,
                   info = paste("This server judges", tbl, "safe to batch, so the fallback these",
                                "assertions cover is never reached. They need a server whose",
                                "sql_mode is STRICT_TRANS_TABLES (not STRICT_ALL_TABLES, which",
                                "covers every engine) -- see docker-compose.test.yml."))
      e <- db_env()
      df <- data.frame(id = 1:3, n = c(1L, 9999L, 3L))
      expect_error(
        upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE),
        regexp = "Out of range"
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
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE),
        regexp = "Data too long"
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
  # No strict mode at all: the server coerces invalid values whichever way they are sent, so
  # per-row buys back exactly one class -- NULL into NOT NULL -- and only if a NULL is bound.
  expect_false(batching_preserves_errors("", TRUE))                          # may bind NULL
  expect_false(batching_preserves_errors("NO_ZERO_DATE", TRUE))
  expect_true(batching_preserves_errors("", TRUE, may_bind_null = FALSE))    # cannot
  expect_true(batching_preserves_errors("", FALSE, may_bind_null = FALSE))   # engine is moot
  # Conservative default, and an unknown answer is not a FALSE.
  expect_false(batching_preserves_errors("", TRUE, may_bind_null = NA))
  # may_bind_null is irrelevant wherever strict mode actually applies.
  expect_true(batching_preserves_errors("STRICT_TRANS_TABLES", TRUE, may_bind_null = TRUE))
  expect_false(batching_preserves_errors("STRICT_TRANS_TABLES", FALSE, may_bind_null = FALSE))
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

test_that("MyRocks is judged safe, and behaves that way", {
  # mega -- the reason this package exists -- stores its large tables on MyRocks, not InnoDB.
  # If the engine lookup misjudged ROCKSDB the 200M-row loads would silently drop to one
  # statement per row. It reports TRANSACTIONS=YES and honours STRICT_TRANS_TABLES like InnoDB;
  # this pins both the judgement and the behaviour it is predicting.
  skip_if_no_db()
  con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  supported <- RMariaDB::dbGetQuery(
    con, "SELECT SUPPORT FROM information_schema.ENGINES WHERE ENGINE = 'ROCKSDB'")
  skip_if(nrow(supported) == 0L || !supported$SUPPORT[1] %in% c("YES", "DEFAULT"),
          "RocksDB engine not available on this server")

  tbl <- "rmaria_strict_rocksdb"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(3), PRIMARY KEY (id)) ENGINE=RocksDB", tbl),
    tbl, {
      expect_true(upsert_batching_is_safe(con, tbl)$safe)
      e <- db_env()
      expect_error(
        upsert_table(.badrow_df(), tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE),
        regexp = "Data too long"
      )
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT COUNT(*) c FROM `%s`", tbl))
      expect_equal(as.numeric(got$c), 0)   # transactional: rolled back, nothing truncated
    })
})

# --- the scalar extractor that keeps a malformed answer from becoming a crash ------

test_that("a scalar is taken from a result only when the result actually has one", {
  expect_equal(.scalar_or_na(data.frame(m = "STRICT_ALL_TABLES"), "m"), "STRICT_ALL_TABLES")
  expect_equal(.scalar_or_na(data.frame(m = c("a", "b")), "m"), "a")
  # The shapes that used to make `is.na(df$col[1])` a length-zero condition, i.e. an error.
  expect_true(is.na(.scalar_or_na(data.frame(other = 1), "m")))
  expect_true(is.na(.scalar_or_na(data.frame(m = character(0)), "m")))
  expect_true(is.na(.scalar_or_na(NULL, "m")))
})
test_that("the one-row short-circuit still raises on a bad value", {
  # The short-circuit skips the safety lookups on the grounds that a single-row statement has no
  # "row 2+" to be downgraded. Pin that reasoning: one bad row must still raise on MyISAM.
  tbl <- "rmaria_strict_one"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(3), PRIMARY KEY (id)) ENGINE=MyISAM", tbl),
    tbl, {
      e <- db_env()
      expect_error(
        upsert_table(data.frame(id = 1L, v = "WAY-TOO-LONG", stringsAsFactors = FALSE), tbl,
                     keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE),
        regexp = "Data too long"
      )
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT COUNT(*) c FROM `%s`", tbl))
      expect_equal(as.numeric(got$c), 0)
    })
})

test_that("two rows are enough to need the check -- the boundary, not just the comfortable case", {
  # Every other non-transactional test here uses 3 or 5 rows, so widening the short-circuit from
  # `== 1L` to `<= 2L` would pass the whole suite while silently reopening the bug for 2-row
  # frames. This is the case that kills that mutant.
  tbl <- "rmaria_strict_two"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(3), PRIMARY KEY (id)) ENGINE=MyISAM", tbl),
    tbl, {
      con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add = TRUE)
      # Precondition, not decoration: on a STRICT_ALL_TABLES server MyISAM is batched and
      # raises anyway, so without this the assertions below would pass without ever
      # exercising the fallback they exist to cover.
      expect_false(upsert_batching_is_safe(con0, tbl)$safe,
                   info = paste("This server judges", tbl, "safe to batch, so the fallback these",
                                "assertions cover is never reached. They need a server whose",
                                "sql_mode is STRICT_TRANS_TABLES (not STRICT_ALL_TABLES, which",
                                "covers every engine) -- see docker-compose.test.yml."))
      e <- db_env()
      df <- data.frame(id = 1:2, v = c("ok", "WAY-TOO-LONG"), stringsAsFactors = FALSE)
      expect_error(
        upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE),
        regexp = "Data too long"
      )
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT v FROM `%s` WHERE id = 2", tbl))
      expect_equal(nrow(got), 0L)   # not stored as "WAY"
    })
})


test_that("a frame is judged to bind NULL only when it actually can", {
  expect_false(.frame_may_bind_null(data.frame(id = 1:3, v = c("a", "b", "c"))))
  expect_true(.frame_may_bind_null(data.frame(id = 1:3, v = c("a", NA, "c"))))
  expect_true(.frame_may_bind_null(data.frame(id = c(1L, NA, 3L))))
  # A blob/raw column arrives as a list whose elements can be NULL one by one, which anyNA()
  # does not see -- so any list column counts as "can bind NULL".
  df <- data.frame(id = 1:2)
  df$blob <- list(as.raw(1:3), NULL)
  expect_true(.frame_may_bind_null(df))
})

test_that("the reason distinguishes the situations a bare TRUE/FALSE cannot", {
  expect_match(.batching_reason(FALSE, NA, NA, "t"), "did not answer")
  expect_match(.batching_reason(TRUE, "STRICT_ALL_TABLES", NA, "t"), "every engine")
  expect_match(.batching_reason(FALSE, "STRICT_TRANS_TABLES", NA, "t"), "could not be identified")
  expect_match(.batching_reason(FALSE, "STRICT_TRANS_TABLES", NA, "widgets"), "widgets")
  expect_match(.batching_reason(FALSE, "STRICT_TRANS_TABLES", FALSE, "t"), "non-transactional")
  expect_match(.batching_reason(TRUE, "STRICT_TRANS_TABLES", TRUE, "t"), "transactional engine")
  expect_match(.batching_reason(FALSE, "", TRUE, "t"), "not in strict mode")
  expect_match(.batching_reason(TRUE, "", TRUE, "t"), "binds no NULL")
})

test_that("the live lookup follows the session it is asked about, not the server default", {
  # upsert_table cannot be pointed at a non-strict server from here (it opens its own
  # connection), but the lookup reads @@SESSION.sql_mode -- so a session with strict mode off
  # exercises the same code path the CRITICAL case would, on the server we already have.
  skip_if_no_db()
  tbl <- "rmaria_strict_session"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, v VARCHAR(16), PRIMARY KEY (id)) ENGINE=InnoDB", tbl),
    tbl, {
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      expect_true(upsert_batching_is_safe(con, tbl)$safe)          # strict by default

      RMariaDB::dbExecute(con, "SET SESSION sql_mode = ''")
      loose_null <- upsert_batching_is_safe(con, tbl, may_bind_null = TRUE)
      loose_clean <- upsert_batching_is_safe(con, tbl, may_bind_null = FALSE)
      # Same server, same table, same engine: only the data decides now.
      expect_false(loose_null$safe)
      expect_match(loose_null$reason, "not in strict mode")
      expect_true(loose_clean$safe)
    })
})
