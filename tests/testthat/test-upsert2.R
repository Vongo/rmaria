test_that("upsert_table inserts new, updates existing, preserves apostrophes, skips NULL, returns count", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_up5 (id INT PRIMARY KEY, name VARCHAR(50), n INT) CHARACTER SET utf8mb4", "t_up5", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_up5 VALUES (1, 'old', 5)")
    n <- upsert_table(data.frame(id=c(1L,2L), name=c("O'Brien", "new"), n=c(NA, 9L)),
                      "t_up5", keycols="id",
                      host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT id,name,n FROM t_up5 ORDER BY id", verbose=FALSE)
    expect_equal(got$name, c("O'Brien", "new"))
    expect_equal(got$n[1], 5L)    # id1 n kept (NA skipped via COALESCE)
    expect_equal(got$n[2], 9L)    # id2 inserted
    expect_equal(as.integer(n), 3L)   # insert(1) + update(2)
  })
})

test_that("upsert_table is atomic on failure", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_up6 (id INT PRIMARY KEY, v INT NOT NULL)", "t_up6", {
    expect_error(
      upsert_table(data.frame(id=c(1L,2L), v=c(1L, NA)), "t_up6", keycols="id",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    )
    n <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   query="SELECT COUNT(*) AS n FROM t_up6", verbose=FALSE)$n
    expect_equal(as.integer(n), 0L)
  })
})

test_that("upsert_table rejects unknown keycols", {
  expect_error(
    upsert_table(data.frame(id=1, v=2), "t_up5", keycols="nope",
                 host="127.0.0.1", port=33306, db="rmaria_test", user="root", password="test"),
    "keycols not found"
  )
})

test_that("upsert_table handles a multi-chunk batch", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_up7 (id INT PRIMARY KEY, v INT)", "t_up7", {
    upsert_table(data.frame(id=1:2500, v=1:2500), "t_up7", keycols="id", chunk_size=1000,
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    cnt <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT COUNT(*) AS n FROM t_up7", verbose=FALSE)$n
    expect_equal(as.integer(cnt), 2500L)
  })
})
