test_that("exec_query stores and pull_data reads 4-byte utf8mb4 characters intact", {
  skip_if_no_db(); e <- db_env()
  with_test_table(
    "CREATE TABLE t_charset (v VARCHAR(20)) CHARACTER SET utf8mb4",
    "t_charset", {
      exec_query(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                 query="INSERT INTO t_charset (v) VALUES ('a\U0001F600b')")
      got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user,
                       password=e$pwd, query="SELECT v FROM t_charset", verbose=FALSE)
      expect_equal(got$v[1], "a\U0001F600b")
    })
})

test_that("delete_from_table deletes matching rows (refactored path)", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_del (id INT)", "t_del", {
    exec_query(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
               query="INSERT INTO t_del (id) VALUES (1),(2),(3)")
    delete_from_table("t_del", "id < 3", host=e$host, port=e$port, db=e$db,
                      user=e$user, password=e$pwd)
    n <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   query="SELECT COUNT(*) AS n FROM t_del", verbose=FALSE)$n
    expect_equal(n, 1)
  })
})
