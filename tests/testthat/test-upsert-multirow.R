# upsert_table must send ONE statement per chunk, not one per row.
#
# dbExecute(sql, params = <list of column vectors>) binds the vectors and executes the
# single-row statement once per row, so a chunk of N rows costs N round trips. Over a link with
# any latency that dominates completely: a 54,525-row upsert to a remote host measured 23
# minutes, ~40 rows/sec, while the same work locally is seconds. Multi-row VALUES collapses a
# chunk to one round trip while keeping every value bound (no literal interpolation, no
# escaping surface).
#
# These tests pin the observable contract -- what ends up in the table -- plus the statement
# shape that makes it fast, since correctness alone cannot tell the two implementations apart.

test_that("build_upsert_sql emits one placeholder tuple per row", {
  one <- build_upsert_sql("t", c("a", "b"), "a")
  expect_equal(lengths(regmatches(one, gregexpr("(?,?)", one, fixed = TRUE))), 1L)

  three <- build_upsert_sql("t", c("a", "b"), "a", n_rows = 3L)
  expect_equal(lengths(regmatches(three, gregexpr("(?,?)", three, fixed = TRUE))), 3L)
  # The tuples are comma-separated, and the ON DUPLICATE clause is emitted once.
  expect_true(grepl("VALUES (?,?),(?,?),(?,?)", three, fixed = TRUE))
  expect_equal(lengths(regmatches(three, gregexpr("ON DUPLICATE KEY UPDATE", three, fixed = TRUE))), 1L)
})

test_that("build_upsert_sql defaults to one row, preserving the previous output exactly", {
  expect_identical(
    build_upsert_sql("t", c("a", "b"), "a"),
    build_upsert_sql("t", c("a", "b"), "a", n_rows = 1L)
  )
})

test_that("n_rows is validated", {
  expect_error(build_upsert_sql("t", c("a", "b"), "a", n_rows = 0L))
  expect_error(build_upsert_sql("t", c("a", "b"), "a", n_rows = -1L))
})

# --- behaviour against a real database -------------------------------------------

.mr_tbl <- "rmaria_multirow_test"
.mr_create <- sprintf(
  "CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, label VARCHAR(64) NULL, n BIGINT NULL, PRIMARY KEY (id))",
  .mr_tbl)

test_that("a multi-row chunk inserts every row with the right values", {
  with_test_table(.mr_create, .mr_tbl, {
    e <- db_env()
    df <- data.frame(id = 1:5, label = c("a", "b", NA, "d", "e"),
                     n = c(10, 20, 30, NA, 50), stringsAsFactors = FALSE)
    upsert_table(df, .mr_tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                 user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
    con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
    got <- RMariaDB::dbGetQuery(con, sprintf("SELECT id, label, n FROM `%s` ORDER BY id", .mr_tbl))
    expect_equal(got$id, 1:5)
    expect_equal(got$label, c("a", "b", NA, "d", "e"))
    expect_equal(got$n, c(10, 20, 30, NA, 50))
  })
})

test_that("values land on the correct row -- column order is not transposed", {
  # The failure mode a row-major flattening bug produces: every value present, every value on
  # the wrong row. A per-row assertion catches it; a COUNT(*) would not.
  with_test_table(.mr_create, .mr_tbl, {
    e <- db_env()
    df <- data.frame(id = c(7L, 8L, 9L), label = c("seven", "eight", "nine"),
                     n = c(700, 800, 900), stringsAsFactors = FALSE)
    upsert_table(df, .mr_tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                 user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
    con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
    got <- RMariaDB::dbGetQuery(con, sprintf("SELECT id, label, n FROM `%s` ORDER BY id", .mr_tbl))
    expect_equal(got$label, c("seven", "eight", "nine"))
    expect_equal(got$n, c(700, 800, 900))
  })
})

test_that("a second upsert updates in place rather than duplicating", {
  with_test_table(.mr_create, .mr_tbl, {
    e <- db_env()
    args <- list(table_name_in_base = .mr_tbl, keycols = "id", host = e$host, port = e$port,
                 db = e$db, user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
    do.call(upsert_table, c(list(data.frame(id = 1:3, label = c("x", "y", "z"), n = c(1, 2, 3))), args))
    do.call(upsert_table, c(list(data.frame(id = 2:4, label = c("Y", "Z", "w"), n = c(22, 33, 44))), args))
    con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
    got <- RMariaDB::dbGetQuery(con, sprintf("SELECT id, label, n FROM `%s` ORDER BY id", .mr_tbl))
    expect_equal(got$id, 1:4)
    expect_equal(got$label, c("x", "Y", "Z", "w"))
    expect_equal(got$n, c(1, 22, 33, 44))
  })
})

test_that("a NULL in an update does not overwrite a stored value (COALESCE contract)", {
  # Pre-existing behaviour of the ON DUPLICATE clause; pinned here because multi-row VALUES()
  # semantics are the part most likely to break it.
  with_test_table(.mr_create, .mr_tbl, {
    e <- db_env()
    args <- list(table_name_in_base = .mr_tbl, keycols = "id", host = e$host, port = e$port,
                 db = e$db, user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
    do.call(upsert_table, c(list(data.frame(id = 1:2, label = c("keep", "keep2"), n = c(5, 6))), args))
    do.call(upsert_table, c(list(data.frame(id = 1:2, label = c(NA, "new"), n = c(NA, 66))), args))
    con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
    got <- RMariaDB::dbGetQuery(con, sprintf("SELECT id, label, n FROM `%s` ORDER BY id", .mr_tbl))
    expect_equal(got$label, c("keep", "new"))
    expect_equal(got$n, c(5, 66))
  })
})

test_that("a chunk boundary does not drop or duplicate rows", {
  with_test_table(.mr_create, .mr_tbl, {
    e <- db_env()
    n <- 250L
    df <- data.frame(id = seq_len(n), label = paste0("k", seq_len(n)),
                     n = as.numeric(seq_len(n)), stringsAsFactors = FALSE)
    # chunk_size deliberately does not divide n evenly.
    upsert_table(df, .mr_tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                 user = e$user, password = e$pwd, chunk_size = 60L,
                 progress_bar = FALSE, nolog = TRUE)
    con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
    got <- RMariaDB::dbGetQuery(con, sprintf("SELECT COUNT(*) c, SUM(n) s, MAX(id) m FROM `%s`", .mr_tbl))
    expect_equal(got$c, n)
    expect_equal(got$s, sum(as.numeric(seq_len(n))))
    expect_equal(got$m, n)
  })
})

test_that("a chunk wider than the placeholder limit is split rather than rejected", {
  # MySQL caps a prepared statement at 65535 placeholders. With 3 columns a 30k-row chunk asks
  # for 90k, so the implementation must sub-split internally instead of erroring -- a caller
  # passing a large chunk_size should not have to know the limit exists.
  with_test_table(.mr_create, .mr_tbl, {
    e <- db_env()
    n <- 25000L
    df <- data.frame(id = seq_len(n), label = "x", n = 1, stringsAsFactors = FALSE)
    expect_no_error(
      upsert_table(df, .mr_tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                   user = e$user, password = e$pwd, chunk_size = 30000L,
                   progress_bar = FALSE, nolog = TRUE)
    )
    con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
    got <- RMariaDB::dbGetQuery(con, sprintf("SELECT COUNT(*) c FROM `%s`", .mr_tbl))
    expect_equal(got$c, n)
  })
})

test_that("the empty-table and bad-keycols contracts are unchanged", {
  e <- db_env()
  expect_equal(
    upsert_table(data.frame(id = integer(0)), .mr_tbl, keycols = "id", host = e$host,
                 port = e$port, db = e$db, user = e$user, password = e$pwd, nolog = TRUE),
    0L
  )
  expect_error(
    upsert_table(data.frame(a = 1), .mr_tbl, keycols = "missing", host = e$host, port = e$port,
                 db = e$db, user = e$user, password = e$pwd, nolog = TRUE),
    "keycols not found"
  )
})
