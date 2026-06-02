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
