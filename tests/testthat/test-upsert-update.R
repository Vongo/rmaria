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
