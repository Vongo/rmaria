test_that("upsert_table inserts then updates on duplicate key, quotes preserved", {
  skip_if_no_db(); e <- db_env()
  with_test_table(
    "CREATE TABLE t_up (id INT PRIMARY KEY, name VARCHAR(50)) CHARACTER SET utf8mb4",
    "t_up", {
      upsert_table(data.frame(id=1, name="O'Brien"), "t_up", keycols="id",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   progress_bar=FALSE)
      upsert_table(data.frame(id=1, name="O'Reilly"), "t_up", keycols="id",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   progress_bar=FALSE)
      got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                       query="SELECT name FROM t_up WHERE id=1", verbose=FALSE)
      expect_equal(got$name[1], "O'Reilly")
    })
})

test_that("upsert_table does not emit invalid SQL when all non-key cols are NA", {
  skip_if_no_db(); e <- db_env()
  with_test_table(
    "CREATE TABLE t_up2 (id INT PRIMARY KEY, name VARCHAR(50))", "t_up2", {
      upsert_table(data.frame(id=1L, name=NA_character_), "t_up2", keycols="id",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   progress_bar=FALSE)
      got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                       query="SELECT id, name FROM t_up2 WHERE id=1", verbose=FALSE)
      expect_equal(nrow(got), 1L)          # INSERT IGNORE fired, not a silent no-op
      expect_true(is.na(got$name[1]))
    })
})

test_that("upsert_table maps non-finite numerics to NULL without error", {
  skip_if_no_db(); e <- db_env()
  with_test_table(
    "CREATE TABLE t_up3 (id INT PRIMARY KEY, val DOUBLE)", "t_up3", {
      expect_no_error(
        upsert_table(data.frame(id=1, val=Inf), "t_up3", keycols="id",
                     host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     progress_bar=FALSE)
      )
      got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                       query="SELECT val FROM t_up3 WHERE id=1", verbose=FALSE)
      expect_true(is.na(got$val[1]))
    })
})

test_that("upsert_table rejects keycols that are not columns of the table", {
  expect_error(
    upsert_table(data.frame(id=1, name="x"), "t_up", keycols="missing_col",
                 host="127.0.0.1", port=33306, db="rmaria_test", user="root", password="test"),
    "keycols not found"
  )
})

test_that("update_table updates by key and preserves apostrophes", {
  skip_if_no_db(); e <- db_env()
  with_test_table(
    "CREATE TABLE t_upd (id INT PRIMARY KEY, name VARCHAR(50)) CHARACTER SET utf8mb4",
    "t_upd", {
      con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
      RMariaDB::dbExecute(con0, "INSERT INTO t_upd VALUES (1, 'old')")
      update_table(data.frame(id=1, name="O'Brien"), "t_upd", keycols="id",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   progress_bar=FALSE)
      got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                       query="SELECT name FROM t_upd WHERE id=1", verbose=FALSE)
      expect_equal(got$name[1], "O'Brien")
    })
})

test_that("update_table skips a non-finite SET value, updates the rest", {
  skip_if_no_db(); e <- db_env()
  with_test_table(
    "CREATE TABLE t_upd2 (id INT PRIMARY KEY, a DOUBLE, b INT)", "t_upd2", {
      con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
      RMariaDB::dbExecute(con0, "INSERT INTO t_upd2 VALUES (1, 10, 20)")
      update_table(data.frame(id=1, a=Inf, b=99), "t_upd2", keycols="id",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   progress_bar=FALSE)
      got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                       query="SELECT a, b FROM t_upd2 WHERE id=1", verbose=FALSE)
      expect_equal(got$a[1], 10)   # a (Inf) skipped -> unchanged
      expect_equal(got$b[1], 99)   # b updated
    })
})

test_that("update_table rejects keycols that are not columns of the table", {
  expect_error(
    update_table(data.frame(id=1, name="x"), "t_upd", keycols="missing_col",
                 host="127.0.0.1", port=33306, db="rmaria_test", user="root", password="test"),
    "keycols not found"
  )
})

test_that("update_table skips a row whose single key is NA (no full-table update)", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_nak (id INT PRIMARY KEY, val INT)", "t_nak", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_nak VALUES (1,10)")
    update_table(data.frame(id=NA_integer_, val=99L), "t_nak", keycols="id",
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT val FROM t_nak WHERE id=1", verbose=FALSE)
    expect_equal(got$val[1], 10L)   # unchanged
  })
})

test_that("update_table with composite keys updates only the matching row", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ck (id1 INT, id2 INT, val INT, PRIMARY KEY(id1,id2))", "t_ck", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_ck VALUES (1,1,10),(1,2,20)")
    update_table(data.frame(id1=1, id2=2, val=99), "t_ck", keycols=c("id1","id2"),
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT val FROM t_ck ORDER BY id1,id2", verbose=FALSE)
    expect_equal(got$val, c(10, 99))   # only (1,2) changed
  })
})

test_that("update_table skips an incomplete composite key instead of broadening WHERE", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ck2 (id1 INT, id2 INT, val INT, PRIMARY KEY(id1,id2))", "t_ck2", {
    con0 <- test_con(); on.exit(RMariaDB::dbDisconnect(con0), add=TRUE)
    RMariaDB::dbExecute(con0, "INSERT INTO t_ck2 VALUES (1,1,10),(1,2,20)")
    update_table(data.frame(id1=1L, id2=NA_integer_, val=99L), "t_ck2", keycols=c("id1","id2"),
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT val FROM t_ck2 ORDER BY id1,id2", verbose=FALSE)
    expect_equal(got$val, c(10, 20))   # NOTHING changed; with the bug both would become 99
  })
})
