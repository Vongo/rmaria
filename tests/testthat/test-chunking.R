test_that("insert_table inserts every row when nrow forces a fractional chunk size", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_count (id INT)", "t_count", {
    df <- data.frame(id = seq_len(2501))   # 2501/25 = 100.04 -> old code drops last row
    insert_table(df, "t_count", host=e$host, port=e$port, db=e$db,
                 user=e$user, password=e$pwd, progress_bar=FALSE)
    n <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   query="SELECT COUNT(*) AS n FROM t_count", verbose=FALSE)$n
    expect_equal(n, 2501)
  })
})
