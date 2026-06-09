# Live-DB integration tests for embedded-NUL / UTF-16 recovery in pull_data.
#
# Uses the shared helpers in helper-db.R (db_env / test_con / with_test_table /
# skip_if_no_db). Skipped unless RMARIA_TEST_HOST is set. A real (non-TEMPORARY)
# table is used on purpose: pull_data opens its own connection, so it must be able
# to see the table created here.

test_that("pull_data auto-recovers a UTF-16 row (on_nul='decode')", {
  e <- db_env()
  with_test_table(
    "CREATE TABLE rmaria_nul_test (id INT, value TEXT)",
    "rmaria_nul_test",
    {
      con <- test_con()
      on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      # FF FE 70 00 72 00 ... 0D 00 == "premiere pro\r" in UTF-16LE
      RMariaDB::dbExecute(con, paste0(
        "INSERT INTO rmaria_nul_test VALUES (1, ",
        "0xFFFE700072006500", "6D00690065007200", "6500200070007200", "6F000D00", ")"
      ))

      out <- pull_data(host = e$host, port = e$port, db = e$db, user = e$user, password = e$pwd,
                       query = "SELECT id, value FROM rmaria_nul_test", verbose = FALSE,
                       on_nul = "decode")
      expect_equal(out$value[1], "premiere pro\r")
    }
  )
})

test_that("pull_data with on_nul='error' raises a classed, actionable error", {
  e <- db_env()
  with_test_table(
    "CREATE TABLE rmaria_nul_test2 (id INT, value TEXT)",
    "rmaria_nul_test2",
    {
      con <- test_con()
      on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
      RMariaDB::dbExecute(con, "INSERT INTO rmaria_nul_test2 VALUES (1, 0xFFFE70000D00)")

      expect_error(
        pull_data(host = e$host, port = e$port, db = e$db, user = e$user, password = e$pwd,
                  query = "SELECT id, value FROM rmaria_nul_test2", verbose = FALSE,
                  on_nul = "error"),
        class = "rmaria_embedded_nul"
      )
    }
  )
})

test_that("insert_table normalizes a latin1 column to UTF-8 (round-trip)", {
  e <- db_env()
  with_test_table(
    "CREATE TABLE rmaria_enc_test (id INT, name VARCHAR(64)) CHARACTER SET utf8mb4",
    "rmaria_enc_test",
    {
      x <- "caf\xe9"; Encoding(x) <- "latin1"
      df <- data.frame(id = 1L, name = x, stringsAsFactors = FALSE)
      insert_table(df, "rmaria_enc_test", host = e$host, port = e$port, db = e$db,
                   user = e$user, password = e$pwd, progress_bar = FALSE, nolog = TRUE)
      out <- pull_data(host = e$host, port = e$port, db = e$db, user = e$user, password = e$pwd,
                       query = "SELECT name FROM rmaria_enc_test", verbose = FALSE)
      expect_equal(out$name[1], "café")
      expect_equal(Encoding(out$name[1]), "UTF-8")
    }
  )
})
