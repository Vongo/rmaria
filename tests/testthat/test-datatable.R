test_that("insert_table round-trips data.table values correctly", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_dt (id INT, v VARCHAR(20))", "t_dt", {
    dt <- data.table::data.table(id = 1:3, v = c("a", "b", "c"))
    insert_table(dt, "t_dt", host=e$host, port=e$port, db=e$db,
                 user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT id, v FROM t_dt ORDER BY id", verbose=FALSE)
    expect_equal(nrow(got), 3L)
    expect_equal(got$id, 1:3)
    expect_equal(got$v, c("a", "b", "c"))
  })
})

test_that("insert_table handles a factor column (round-trips label)", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_fac (sp VARCHAR(20)) CHARACTER SET utf8mb4", "t_fac", {
    df <- data.frame(sp = factor(c("setosa", "virginica")))
    insert_table(df, "t_fac", host=e$host, port=e$port, db=e$db,
                 user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT sp FROM t_fac ORDER BY sp", verbose=FALSE)
    expect_equal(sort(got$sp), c("setosa", "virginica"))
  })
})
