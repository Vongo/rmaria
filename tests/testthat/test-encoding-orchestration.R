# Mocked orchestration tests for dbGetQuery_nul_safe.
#
# These tests use testthat 3's with_mocked_bindings to replace RMariaDB namespace
# functions, so no live database connection is required.

# Build UTF-16LE bytes (with BOM) for an ASCII string — same helper as test-encoding.R.
u16le_bom_orch <- function(s) {
  ascii <- charToRaw(s)
  out <- raw(0)
  for (b in ascii) out <- c(out, b, as.raw(0x00))
  c(as.raw(0xFF), as.raw(0xFE), out)
}

test_that("dbGetQuery_nul_safe decode-mode: recovers NUL column and balances dbSendQuery/dbClearResult", {
  send_count  <- 0L
  clear_count <- 0L
  mock_res    <- structure(list(), class = "MockResult")

  con <- structure(list(), class = "dummy")

  result <- testthat::with_mocked_bindings(
    dbGetQuery = function(conn, statement, ...) {
      if (grepl("rmaria_sub", statement, fixed = TRUE)) {
        # Recovery query: return a list-of-raw text column representing "hello"
        df <- data.frame(id = 1L, stringsAsFactors = FALSE)
        df$value <- list(u16le_bom_orch("hello"))
        df
      } else {
        stop("embedded nul in string: 'x'")
      }
    },
    dbSendQuery = function(conn, statement, ...) {
      send_count <<- send_count + 1L
      mock_res
    },
    dbColumnInfo = function(res, ...) {
      data.frame(
        name = c("id", "value"),
        type = c("integer", "character"),
        stringsAsFactors = FALSE
      )
    },
    dbClearResult = function(res, ...) {
      clear_count <<- clear_count + 1L
      invisible(TRUE)
    },
    .package = "RMariaDB",
    rmaria:::dbGetQuery_nul_safe(con, "SELECT id, value FROM t", on_nul = "decode", verbose = FALSE)
  )

  # Result should be decoded
  expect_equal(result$value, "hello")
  expect_equal(result$id, 1L)

  # Every dbSendQuery must be matched by a dbClearResult (handle balance)
  expect_equal(send_count, clear_count,
    label = "dbSendQuery/dbClearResult handle balance")
})

test_that("dbGetQuery_nul_safe error-mode: raises rmaria_embedded_nul condition", {
  mock_res <- structure(list(), class = "MockResult")
  con      <- structure(list(), class = "dummy")

  expect_error(
    testthat::with_mocked_bindings(
      dbGetQuery = function(conn, statement, ...) {
        stop("embedded nul in string: 'x'")
      },
      dbSendQuery = function(conn, statement, ...) mock_res,
      dbColumnInfo = function(res, ...) {
        data.frame(
          name = c("id", "value"),
          type = c("integer", "character"),
          stringsAsFactors = FALSE
        )
      },
      dbClearResult = function(res, ...) invisible(TRUE),
      .package = "RMariaDB",
      rmaria:::dbGetQuery_nul_safe(con, "SELECT id, value FROM t", on_nul = "error", verbose = FALSE)
    ),
    class = "rmaria_embedded_nul"
  )
})
