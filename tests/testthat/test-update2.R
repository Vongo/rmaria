test_that("update_table (temp-join) updates by composite key, skips NULL value & NULL key", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_u2 (id1 INT, id2 INT, val INT, name VARCHAR(20), PRIMARY KEY(id1,id2))", "t_u2", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_u2 VALUES (1,1,10,'x'),(1,2,20,'y')")
    upd <- data.frame(id1=c(1L,1L,1L), id2=c(2L,1L,NA), val=c(99L, NA, 77L), name=c(NA,"w","z"))
    update_table(upd, "t_u2", keycols=c("id1","id2"),
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT id1,id2,val,name FROM t_u2 ORDER BY id1,id2", verbose=FALSE)
    expect_equal(got$val,  c(10L, 99L))   # (1,1) val NA-skipped; (1,2) updated
    expect_equal(got$name, c("w",  "y"))  # (1,1) name updated; (1,2) name NA-skipped
    expect_equal(nrow(got), 2L)            # (1,NA) never matched -> no new row
  })
})

test_that("update_table preserves apostrophes and returns a count", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_u4 (id INT PRIMARY KEY, name VARCHAR(50)) CHARACTER SET utf8mb4", "t_u4", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_u4 VALUES (1,'old')")
    n <- update_table(data.frame(id=1L, name="O'Brien"), "t_u4", keycols="id",
                      host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT name FROM t_u4 WHERE id=1", verbose=FALSE)
    expect_equal(got$name[1], "O'Brien")
    expect_equal(as.integer(n), 1L)
  })
})

test_that("update_table returns 0 for an empty frame", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_u3 (id INT PRIMARY KEY, v INT)", "t_u3", {
    n <- update_table(data.frame(id=integer(0), v=integer(0)), "t_u3", keycols="id",
                      host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    expect_equal(as.integer(n), 0L)
  })
})

test_that("update_table no-ops (returns 0) when there are no non-key columns", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_u5 (id INT PRIMARY KEY)", "t_u5", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_u5 VALUES (1)")
    n <- update_table(data.frame(id=1L), "t_u5", keycols="id",
                      host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    expect_equal(as.integer(n), 0L)
  })
})

test_that("update_table temp-load handles a multi-chunk batch", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_u6 (id INT PRIMARY KEY, v INT)", "t_u6", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_u6 (id,v) SELECT seq, 0 FROM seq_1_to_2500")
    update_table(data.frame(id=1:2500, v=1:2500), "t_u6", keycols="id", chunk_size=1000,
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT v FROM t_u6 WHERE id=2500", verbose=FALSE)
    expect_equal(as.integer(got$v[1]), 2500L)
  })
})

test_that("update_table skips NA in a NOT NULL non-key column (partial update, row not dropped)", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_unn (id INT PRIMARY KEY, a INT NOT NULL, b INT)", "t_unn", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_unn VALUES (1, 10, 20)")
    update_table(data.frame(id=1L, a=NA_integer_, b=99L), "t_unn", keycols="id",
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT a, b FROM t_unn WHERE id=1", verbose=FALSE)
    expect_equal(got$a[1], 10L)   # NOT NULL col kept (NA skipped; row NOT dropped)
    expect_equal(got$b[1], 99L)   # other col updated
  })
})

test_that("update_table rolls back on error (atomic)", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_uatom (id INT PRIMARY KEY, v INT)", "t_uatom", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_uatom VALUES (1, 10)")
    expect_error(
      update_table(data.frame(id=1L, v=99L, bogus=5L), "t_uatom", keycols="id",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    )
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT v FROM t_uatom WHERE id=1", verbose=FALSE)
    expect_equal(got$v[1], 10L)   # unchanged -- rolled back
  })
})

test_that("update_table stops on missing/empty keycols", {
  expect_error(update_table(data.frame(id=1, v=2), "t", host="127.0.0.1", port=33306, db="rmaria_test", user="root", password="test"), "keycols")
  expect_error(update_table(data.frame(id=1, v=2), "t", keycols=character(0), host="127.0.0.1", port=33306, db="rmaria_test", user="root", password="test"), "keycols")
})
