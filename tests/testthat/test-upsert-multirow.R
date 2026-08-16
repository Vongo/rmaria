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

test_that("a BLOB column round-trips byte-identically through a multi-row batch", {
  # Regression: a raw column arrives as a LIST column, and as.list() leaves a list unchanged
  # where it splits an atomic vector. Flattening then handed each placeholder the whole column
  # instead of one value ("Parameter 2 does not have length 1") -- a hard failure on input the
  # package explicitly supports (normalize_table_utf8 branches on is.list/is.raw).
  tbl <- "rmaria_blob_multirow"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, payload BLOB NULL, PRIMARY KEY (id))", tbl),
    tbl, {
      e <- db_env()
      payloads <- list(as.raw(c(1, 2, 3)), as.raw(c(255, 0, 128)), as.raw(9))
      df <- data.frame(id = 1:3)
      df$payload <- payloads
      upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                   user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT id, payload FROM `%s` ORDER BY id", tbl))
      expect_equal(got$id, 1:3)
      # Byte-identical, and on the right rows -- not merely "three blobs arrived".
      expect_equal(lapply(got$payload, as.integer), lapply(payloads, as.integer))
    })
})

test_that("a mixed frame carrying both atomic and blob columns binds both correctly", {
  tbl <- "rmaria_blob_mixed"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, label VARCHAR(16) NULL, payload BLOB NULL, PRIMARY KEY (id))", tbl),
    tbl, {
      e <- db_env()
      df <- data.frame(id = 1:2, label = c("a", "b"), stringsAsFactors = FALSE)
      df$payload <- list(as.raw(c(7, 8)), as.raw(c(9)))
      upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                   user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT id, label, payload FROM `%s` ORDER BY id", tbl))
      expect_equal(got$label, c("a", "b"))
      expect_equal(lapply(got$payload, as.integer), list(c(7L, 8L), 9L))
    })
})

test_that("a payload larger than max_allowed_packet is split rather than rejected", {
  # Regression + improvement. Binding a chunk sends all of its data in one packet under BOTH
  # implementations, so this ceiling is not new -- but the multi-row statement text adds
  # ~(2*ncol + 2) bytes per row on top, which lowered the effective limit by ~1% and broke
  # payloads sitting just under it (measured: 4 cols x 10000 rows at 1670 B/row worked before,
  # failed after). Sizing the batch to the server's own limit removes that band, and makes
  # payloads that never fit before work now.
  tbl <- "rmaria_packet_split"
  with_test_table(
    sprintf("CREATE TABLE `%s` (id INT UNSIGNED NOT NULL, a VARCHAR(12), b VARCHAR(12), v LONGTEXT, PRIMARY KEY (id))", tbl),
    tbl, {
      e <- db_env()
      con <- test_con(); on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      limit <- as.numeric(RMariaDB::dbGetQuery(con, "SELECT @@max_allowed_packet p")$p[1])
      # ~1.5x the packet limit in one call, at the default chunk_size.
      width <- 1700L
      n <- as.integer(ceiling(limit * 1.5 / width))
      df <- data.frame(id = seq_len(n), a = "aa", b = "bb", v = strrep("x", width),
                       stringsAsFactors = FALSE)
      expect_no_error(
        upsert_table(df, tbl, keycols = "id", host = e$host, port = e$port, db = e$db,
                     user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      )
      got <- RMariaDB::dbGetQuery(con, sprintf("SELECT COUNT(*) c, MIN(LENGTH(v)) mn, MAX(LENGTH(v)) mx FROM `%s`", tbl))
      expect_equal(got$c, n)
      expect_equal(got$mn, width)   # values intact, not truncated by the split
      expect_equal(got$mx, width)
    })
})

test_that("upsert_batch_rows respects whichever ceiling binds first", {
  con <- structure(list(), class = "fake")   # never queried: the estimator short-circuits below
  # Narrow frame, small chunk_size -> the caller's hint wins.
  narrow <- data.frame(id = 1:10, v = 1)
  expect_equal(upsert_batch_rows(narrow, 5L, con = NULL, nolog = TRUE), 5L)
  # Never returns 0, whatever the inputs.
  expect_gte(upsert_batch_rows(narrow, 1L, con = NULL, nolog = TRUE), 1L)
})

test_that("estimate_row_bytes counts character columns in bytes, not characters", {
  ascii <- data.frame(v = strrep("x", 10), stringsAsFactors = FALSE)
  utf8  <- data.frame(v = strrep("é", 10), stringsAsFactors = FALSE)   # 2 bytes per char
  expect_gt(estimate_row_bytes(utf8), estimate_row_bytes(ascii))
  expect_gte(estimate_row_bytes(data.frame()), 1)
})
