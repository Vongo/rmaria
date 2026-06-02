test_that("pull_data keep_int64 controls integer64 vs numeric for BIGINT", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_big (v BIGINT)", "t_big", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_big VALUES (9007199254740993)")  # > 2^53
    keep <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                      query="SELECT v FROM t_big", verbose=FALSE, keep_int64=TRUE)
    expect_true(bit64::is.integer64(keep$v))
    conv <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                      query="SELECT v FROM t_big", verbose=FALSE, keep_int64=FALSE)
    expect_true(is.numeric(conv$v) && !bit64::is.integer64(conv$v))
  })
})
