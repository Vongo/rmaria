test_that("insert_table returns the affected-row count", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins2 (id INT)", "t_ins2", {
    n <- insert_table(data.frame(id = 1:7), "t_ins2", host=e$host, port=e$port, db=e$db,
                      user=e$user, password=e$pwd, progress_bar=FALSE)
    expect_equal(as.integer(n), 7L)
  })
})

test_that("insert_table is atomic: a bad row aborts the whole batch", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins3 (id INT PRIMARY KEY)", "t_ins3", {
    expect_error(
      insert_table(data.frame(id = c(1L, 1L)), "t_ins3", host=e$host, port=e$port, db=e$db,
                   user=e$user, password=e$pwd, progress_bar=FALSE, ignore=FALSE)
    )
    n <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   query="SELECT COUNT(*) AS n FROM t_ins3", verbose=FALSE)$n
    expect_equal(as.integer(n), 0L)
  })
})

test_that("insert_table still round-trips quotes/backslash/NA/data.table/factor", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins4 (v VARCHAR(50)) CHARACTER SET utf8mb4", "t_ins4", {
    insert_table(data.table::data.table(v = c("O'Brien", "a\\b", NA, "x\"y")), "t_ins4",
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT v FROM t_ins4 ORDER BY v IS NULL, v", verbose=FALSE)
    expect_setequal(got$v[!is.na(got$v)], c("O'Brien", "a\\b", "x\"y"))
    expect_true(any(is.na(got$v)))
  })
})

test_that("insert_table handles a factor column without warning and round-trips labels", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_insf (sp VARCHAR(20)) CHARACTER SET utf8mb4", "t_insf", {
    expect_no_warning(
      insert_table(data.frame(sp = factor(c("setosa", "virginica"))), "t_insf",
                   host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    )
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT sp FROM t_insf ORDER BY sp", verbose=FALSE)
    expect_equal(sort(got$sp), c("setosa", "virginica"))
  })
})

test_that("insert_table inserts every row for a >1-chunk batch (chunk_size small)", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins5 (id INT)", "t_ins5", {
    n <- insert_table(data.frame(id = 1:2501), "t_ins5", host=e$host, port=e$port, db=e$db,
                      user=e$user, password=e$pwd, progress_bar=FALSE, chunk_size=1000)
    cnt <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT COUNT(*) AS n FROM t_ins5", verbose=FALSE)$n
    expect_equal(as.integer(cnt), 2501L)
    expect_equal(as.integer(n), 2501L)
  })
})

test_that("insert_table rolls back ALL chunks when a later chunk fails (cross-chunk atomicity)", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins_atom2 (id INT PRIMARY KEY)", "t_ins_atom2", {
    expect_error(
      insert_table(data.frame(id=c(1L,1L)), "t_ins_atom2", host=e$host, port=e$port, db=e$db,
                   user=e$user, password=e$pwd, progress_bar=FALSE, chunk_size=1L, ignore=FALSE)
    )
    n <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   query="SELECT COUNT(*) AS n FROM t_ins_atom2", verbose=FALSE)$n
    expect_equal(as.integer(n), 0L)   # chunk 1 rolled back when chunk 2 failed
  })
})

test_that("insert_table returns 0 for an empty frame", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins_e (id INT)", "t_ins_e", {
    n <- insert_table(data.frame(id=integer(0)), "t_ins_e", host=e$host, port=e$port, db=e$db,
                      user=e$user, password=e$pwd, progress_bar=FALSE)
    expect_equal(as.integer(n), 0L)
  })
})

test_that("insert_table with ignore=TRUE (default) silently skips duplicate keys", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_ins_ig (id INT PRIMARY KEY)", "t_ins_ig", {
    expect_no_error(
      insert_table(data.frame(id=c(1L,1L,2L)), "t_ins_ig", host=e$host, port=e$port, db=e$db,
                   user=e$user, password=e$pwd, progress_bar=FALSE)  # ignore=TRUE default
    )
    n <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                   query="SELECT COUNT(*) AS n FROM t_ins_ig", verbose=FALSE)$n
    expect_equal(as.integer(n), 2L)   # dup id=1 skipped; 1 and 2 present
  })
})
