test_that("insert_table_local writes into the resolved database/host/port", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local (id INT, v VARCHAR(10))")

  insert_table_local(data.frame(id = 1:2, v = c("x", "y")), "t_local")

  got <- pull_data(host = HOST, port = PORT, db = DB, user = USER, password = PWD,
                   query = "SELECT id, v FROM t_local ORDER BY id", verbose = FALSE)
  expect_equal(got$id, 1:2)
  expect_equal(got$v, c("x", "y"))
})

test_that("insert_table_local chunking path writes ALL rows (nrow = k*thr + 1)", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_chunk")
  RMariaDB::dbExecute(con, "CREATE TABLE t_chunk (id INT)")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_chunk"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  insert_table_local(data.frame(id = 1:3), "t_chunk", split_threshold = 2L)  # forces chunking, k*thr+1 pattern
  got <- pull_data(host=HOST, port=PORT, db=DB, user=USER, password=PWD,
                   query="SELECT id FROM t_chunk ORDER BY id", verbose=FALSE)
  expect_equal(got$id, 1:3)   # before the fix this returns 1:2 (row 3 lost)
})

test_that("insert_table_local accepts a data.table", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_dtl")
  RMariaDB::dbExecute(con, "CREATE TABLE t_dtl (id INT, v VARCHAR(10))")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_dtl"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  insert_table_local(data.table::data.table(id = 1:2, v = c("a", "b")), "t_dtl")
  got <- pull_data(host=HOST, port=PORT, db=DB, user=USER, password=PWD,
                   query="SELECT id, v FROM t_dtl ORDER BY id", verbose=FALSE)
  expect_equal(got$id, 1:2)
  expect_equal(got$v, c("a", "b"))
})

# --- error reporting -------------------------------------------------------------------------
#
# insert_table_local used to catch its own errors, log them, and never rethrow. Because the
# tryCatch was the function's last expression, its value was the return -- and on the error path
# that value was logerror()'s, which is TRUE. A failed INSERT was therefore indistinguishable
# from a successful one by exception AND by return value at once.
#
# Its sibling insert_table (which insertq delegates to) logs and then stop(e)s, returning the
# affected count. These tests pin insert_table_local to that same contract.

test_that("a missing target table is CREATED rather than raising -- pinned, not endorsed", {
  # dbWriteTable(append=TRUE) creates the table when it does not exist, so a typo'd table name
  # silently produces a new table instead of failing. Verified against MariaDB 11: the row lands
  # in a freshly created table.
  #
  # This is pre-existing behaviour, unrelated to the error-reporting contract the rest of this
  # block pins, and it is recorded here so it cannot drift unnoticed -- NOT because it is
  # obviously right. Making a missing table an error would be its own change with its own blast
  # radius.
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_absent")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_absent"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  expect_equal(insert_table_local(data.frame(id = 1L), "t_local_absent"), 1L)
  got <- pull_data(host = HOST, port = PORT, db = DB, user = USER, password = PWD,
                   query = "SELECT id FROM t_local_absent", verbose = FALSE)
  expect_equal(got$id, 1L)
})

test_that("insert_table_local raises when a value violates the target schema", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_strict")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local_strict (id INT, v VARCHAR(3))")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_strict"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  expect_error(insert_table_local(data.frame(id = 1L, v = "WAY-TOO-LONG"), "t_local_strict"))
})

test_that("insert_table_local returns the number of rows written, invisibly", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_ret")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local_ret (id INT)")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_ret"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  got <- insert_table_local(data.frame(id = 1:5), "t_local_ret")
  expect_equal(got, 5L)
  # Invisible, like insert_table's return -- withVisible is the only way to assert that.
  expect_false(withVisible(insert_table_local(data.frame(id = 6:7), "t_local_ret"))$visible)
})

test_that("insert_table_local returns the FULL count across the chunked path", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_retchunk")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local_retchunk (id INT)")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_retchunk"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  # Not the last chunk's size (1) and not the threshold (2) -- the total.
  got <- insert_table_local(data.frame(id = 1:5), "t_local_retchunk", split_threshold = 2L)
  expect_equal(got, 5L)
})

test_that("a mid-chunk failure raises AND leaves the earlier chunks committed", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_partial")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local_partial (id INT, v VARCHAR(3))")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_partial"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)

  # Capture the logged error so its reported count is itself asserted -- nothing previously
  # checked the message content, which let two independent mutations of `written` (incrementing
  # before dbWriteTable() rather than after, and logging nrow(table) instead of `written`) escape
  # the suite untouched. Pattern matches test-log-truncation.R's capture handler.
  captured <- character(0)
  capture_written_count <- function(msg, handler, ...) {
    if (isTRUE(list(...)$dry)) return(TRUE)   # logging:: probes handlers with dry=TRUE
    captured <<- c(captured, msg)
  }
  logging::addHandler(capture_written_count, level = "ERROR", logger = "com.vongo.rmaria")
  on.exit(logging::removeHandler("capture_written_count", logger = "com.vongo.rmaria"), add = TRUE)

  # split_threshold=1 puts each row in its own write; row 2 violates VARCHAR(3).
  # This function is NOT transactional -- insert_table_local does not use dbWithTransaction -- so
  # row 1 is already committed when row 2 raises. That is the documented behaviour, pinned here
  # so nobody later assumes a failed call wrote nothing.
  expect_error(insert_table_local(
    data.frame(id = 1:3, v = c("ok", "WAY-TOO-LONG", "ok3"), stringsAsFactors = FALSE),
    "t_local_partial", split_threshold = 1L))
  got <- pull_data(host = HOST, port = PORT, db = DB, user = USER, password = PWD,
                   query = "SELECT id FROM t_local_partial ORDER BY id", verbose = FALSE)
  expect_equal(got$id, 1L)

  # Exactly 1, not 0 (the pre-fix state that motivated this whole PR) and not 3 (the full total,
  # which either mutation above would report instead of the true in-progress count).
  expect_length(captured, 1L)
  expect_match(captured, "(1 of 3 rows written)", fixed = TRUE)
})

test_that("the reported count is a FLOOR: a failing batch may have committed rows it does not count", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_floor")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local_floor (id INT, v VARCHAR(3)) ENGINE=MyISAM")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_floor"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  # MyISAM cannot roll back, so dbWriteTable's own multi-row call commits row 1 before row 2
  # raises -- but `written` is only assigned after the call returns, so it reports 0. The count
  # is therefore a lower bound, not an exact figure. Pinned so the docs and the behaviour cannot
  # drift apart; making it exact would need the row-by-row approach .upsert_row_by_row uses.
  expect_error(insert_table_local(
    data.frame(id = 1:3, v = c("ok", "WAY-TOO-LONG", "ok3"), stringsAsFactors = FALSE),
    "t_local_floor"))
  got <- pull_data(host = HOST, port = PORT, db = DB, user = USER, password = PWD,
                   query = "SELECT id FROM t_local_floor ORDER BY id", verbose = FALSE)
  # The logged error reports 0 rows written (asserted by name in the mid-chunk-failure test
  # below); the table actually holds 1 -- proof the reported count under-reports reality.
  expect_equal(got$id, 1L)
})

test_that("a malformed preface query is named as such, not mistaken for the INSERT failing", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_preface")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local_preface (id INT)")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_preface"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  # A malformed preface query used to share the outer handler with the INSERT itself, so the
  # logged/thrown message read as though the row data had failed to insert. Pin that it now
  # identifies itself as a preface query and names the offending one.
  msg <- tryCatch({
    insert_table_local(data.frame(id = 1L), "t_local_preface", preface_queries = "THIS IS NOT SQL")
    NA_character_
  }, error = function(e) conditionMessage(e))
  expect_match(msg, "preface query failed", fixed = TRUE)
  expect_match(msg, "THIS IS NOT SQL", fixed = TRUE)
})

test_that("the connection is released even when the insert fails", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_leak")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local_leak (id INT, v VARCHAR(3))")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_leak"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  # The `finally` disconnect must survive the rethrow. Fail repeatedly, then succeed: if the
  # failed calls leaked connections this would eventually exhaust max_connections instead.
  for (i in 1:25) {
    expect_error(insert_table_local(data.frame(id = 1L, v = "WAY-TOO-LONG"), "t_local_leak"))
  }
  expect_equal(insert_table_local(data.frame(id = 2L, v = "ok"), "t_local_leak"), 1L)
})

test_that("use_file=TRUE truncates silently under STRICT_TRANS_TABLES on InnoDB -- pinned, not endorsed", {
  # LOAD DATA LOCAL INFILE (what use_file=TRUE uses) does not honour STRICT_TRANS_TABLES: an
  # over-long value is truncated rather than raising, no warning reaches R, and the call reports
  # success with the full row count -- exactly as if the value had been valid. Verified against
  # MariaDB 11 under STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION on
  # InnoDB, i.e. the exact configuration the roxygen documents as the intended bulk-load usage.
  #
  # This is pre-existing behaviour, NOT fixed by this PR's log-and-rethrow contract -- there is
  # nothing to rethrow, because MariaDB itself never reports it as an error. Recorded here so it
  # cannot drift unnoticed -- NOT because it is obviously right. Detecting it would need
  # inspecting SHOW WARNINGS after every load, which is its own change with its own risk.
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  con <- test_con()
  RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_usefile")
  RMariaDB::dbExecute(con, "CREATE TABLE t_local_usefile (id INT, v VARCHAR(3)) ENGINE=InnoDB")
  on.exit(RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS t_local_usefile"), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)

  got_n <- NA_integer_
  expect_no_error(got_n <- insert_table_local(
    data.frame(id = 1L, v = "WAY-TOO-LONG", stringsAsFactors = FALSE),
    "t_local_usefile", use_file = TRUE))
  expect_equal(got_n, 1L)   # reports full success -- no signal anything was wrong

  got <- pull_data(host = HOST, port = PORT, db = DB, user = USER, password = PWD,
                   query = "SELECT id, v FROM t_local_usefile", verbose = FALSE)
  expect_equal(got$v, "WAY")   # silently truncated to the column's VARCHAR(3) width
})
