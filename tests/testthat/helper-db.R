# Integration tests need a MariaDB. Set these env vars (CI sets them via the
# service container; locally use docker-compose.test.yml). Without them, tests skip.
db_env <- function() {
  list(
    host = Sys.getenv("RMARIA_TEST_HOST", ""),
    port = as.integer(Sys.getenv("RMARIA_TEST_PORT", "3306")),
    db   = Sys.getenv("RMARIA_TEST_DB", "rmaria_test"),
    user = Sys.getenv("RMARIA_TEST_USER", "root"),
    pwd  = Sys.getenv("RMARIA_TEST_PWD", "")
  )
}

skip_if_no_db <- function() {
  testthat::skip_if(identical(Sys.getenv("RMARIA_TEST_HOST", ""), ""),
                    "RMARIA_TEST_HOST not set; skipping DB integration test")
}

test_con <- function() {
  e <- db_env()
  con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), dbname = e$db, host = e$host,
                             port = e$port, user = e$user, password = e$pwd)
  RMariaDB::dbExecute(con, "SET NAMES utf8mb4")
  con
}

# Create a fresh table for a test and drop it afterwards.
with_test_table <- function(create_sql, table_name, code) {
  skip_if_no_db()
  con <- test_con()
  RMariaDB::dbExecute(con, sprintf("DROP TABLE IF EXISTS `%s`", table_name))
  RMariaDB::dbExecute(con, create_sql)
  on.exit(RMariaDB::dbExecute(con, sprintf("DROP TABLE IF EXISTS `%s`", table_name)), add = TRUE)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  force(code)
}
