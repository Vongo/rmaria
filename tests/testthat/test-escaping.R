roundtrip_one <- function(value) {
  e <- db_env()
  with_test_table("CREATE TABLE t_esc (v VARCHAR(255)) CHARACTER SET utf8mb4", "t_esc", {
    insert_table(data.frame(v = value, stringsAsFactors = FALSE), "t_esc",
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                 progress_bar=FALSE)
    pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
              query="SELECT v FROM t_esc", verbose=FALSE)$v[1]
  })
}

test_that("insert_table stores apostrophes, quotes and backslashes verbatim", {
  skip_if_no_db()
  expect_equal(roundtrip_one("O'Brien"),  "O'Brien")
  expect_equal(roundtrip_one('say "hi"'), 'say "hi"')
  expect_equal(roundtrip_one("a\\b"),     "a\\b")
})

test_that("insert_table neutralizes an injection attempt as literal data", {
  skip_if_no_db()
  payload <- "x'); DROP TABLE t_esc; --"
  expect_equal(roundtrip_one(payload), payload)
})

test_that("NA inserts as SQL NULL", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_na (v VARCHAR(10))", "t_na", {
    insert_table(data.frame(v = NA_character_), "t_na", host=e$host, port=e$port,
                 db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT v FROM t_na", verbose=FALSE)
    expect_true(is.na(got$v[1]))
  })
})

test_that("Inf, -Inf and NaN insert as SQL NULL", {
  skip_if_no_db(); e <- db_env()
  with_test_table("CREATE TABLE t_inf (v DOUBLE)", "t_inf", {
    insert_table(data.frame(v = c(1.0, Inf, -Inf, NaN, NA_real_)), "t_inf",
                 host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                 progress_bar=FALSE)
    got <- pull_data(host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd,
                     query="SELECT v FROM t_inf ORDER BY v", verbose=FALSE)
    expect_equal(sum(is.na(got$v)), 4L)            # Inf, -Inf, NaN, NA -> NULL
    expect_equal(got$v[!is.na(got$v)], 1.0)
  })
})
