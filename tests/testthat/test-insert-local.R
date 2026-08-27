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
  # split_threshold=1 puts each row in its own write; row 2 violates VARCHAR(3).
  # This function is NOT transactional -- dbWithTransaction is used only by insert_table -- so
  # row 1 is already committed when row 2 raises. That is the documented behaviour, pinned here
  # so nobody later assumes a failed call wrote nothing.
  expect_error(insert_table_local(
    data.frame(id = 1:3, v = c("ok", "WAY-TOO-LONG", "ok3"), stringsAsFactors = FALSE),
    "t_local_partial", split_threshold = 1L))
  got <- pull_data(host = HOST, port = PORT, db = DB, user = USER, password = PWD,
                   query = "SELECT id FROM t_local_partial ORDER BY id", verbose = FALSE)
  expect_equal(got$id, 1L)
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
