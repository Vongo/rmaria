test_that("short queries pass through .truncate_query_for_log unchanged", {
  q <- "SELECT * FROM t WHERE id IN (1,2,3)"
  expect_identical(rmaria:::.truncate_query_for_log(q), q)
})

test_that("a query exactly at the limit is not truncated", {
  q <- strrep("x", 2000L)
  expect_identical(rmaria:::.truncate_query_for_log(q, max_chars = 2000L), q)
})

test_that("an oversized query is cut at max_chars and reports its full length", {
  # The percent signs pin sprintf safety: the query must always travel as a
  # format ARGUMENT, so %s/%d/%% survive truncation and logging literally.
  q <- paste0("SELECT 1 -- %s %d %% ", strrep("a", 70000L))
  out <- rmaria:::.truncate_query_for_log(q, max_chars = 2000L)
  expect_lt(nchar(out), 2100L)
  expect_identical(substr(out, 1L, 2000L), substr(q, 1L, 2000L))
  expect_match(out, "%s %d %%", fixed = TRUE)
  expect_match(out, sprintf("truncated, %d chars total", nchar(q)), fixed = TRUE)
})

test_that("non-scalar or NA input is returned as-is rather than erroring", {
  # The helper sits on the failure-logging path; it must never throw and mask
  # the real error being reported.
  expect_identical(rmaria:::.truncate_query_for_log(NA_character_), NA_character_)
  expect_identical(rmaria:::.truncate_query_for_log(character(0)), character(0))
})

test_that("the default cap keeps a huge query near .LOG_QUERY_MAX_CHARS", {
  # Every other test passes max_chars= explicitly, so this is the only pin on
  # the default constant itself: drift to a larger value stays visible here.
  out <- rmaria:::.truncate_query_for_log(strrep("k", 1000000L))
  expect_lt(nchar(out), 2100L)
  expect_match(out, "truncated, 1000000 chars total", fixed = TRUE)
})

test_that("an invalid multibyte string degrades to a marker, never an error", {
  # nchar() and substr() both throw on a string whose declared encoding does
  # not match its bytes. pull_data's own validation rejects such input before
  # any logging happens, so this pins a helper-level guarantee only: swallow
  # the throw, emit a bounded marker, never the raw query.
  bad <- rawToChar(as.raw(c(0x63, 0x61, 0x66, 0xe9)))   # latin1 bytes...
  Encoding(bad) <- "UTF-8"                               # ...declared UTF-8
  out <- NULL
  expect_no_error(out <- rmaria:::.truncate_query_for_log(bad, max_chars = 2L))
  expect_true(is.character(out) && length(out) == 1L)
  expect_lt(nchar(out, type = "bytes"), 100L)
})

test_that("pull_data failure log lines stay bounded for a multi-megabyte query", {
  # The incident this pins: 65MB IN-list queries logged whole on every failed
  # attempt and once more on the final error turned a DB outage into
  # multi-gigabyte daily log files. Every line rmaria logs about a failing
  # query must stay bounded no matter how large the query is.
  big_query <- sprintf("SELECT * FROM t WHERE keyword_id IN (%s)",
                       paste(seq_len(500000L), collapse = ","))
  expect_gt(nchar(big_query), 3000000L)

  captured <- character(0)
  capture_action <- function(msg, handler, ...) {
    if (isTRUE(list(...)$dry)) return(TRUE)   # logging:: probes handlers with dry=TRUE
    captured <<- c(captured, msg)
  }
  logging::addHandler(capture_action, level = "FINEST", logger = "com.vongo.rmaria")
  on.exit(logging::removeHandler("capture_action", logger = "com.vongo.rmaria"), add = TRUE)
  # The logger itself defaults to INFO, which would filter FINEST records
  # before they reach the handler -- and silently exempt the verbose
  # fetch-trace call site from this test. Lower it so all three truncated
  # sites are exercised.
  old_level <- logging::getLogger("com.vongo.rmaria")$level
  logging::setLevel("FINEST", "com.vongo.rmaria")
  on.exit(logging::setLevel(old_level, "com.vongo.rmaria"), add = TRUE)

  # Port 1 on localhost: the connection is refused, so both the per-attempt
  # warning and the final error log fire without needing a database.
  expect_error(
    pull_data(host = "127.0.0.1", port = 1, db = "nodb", user = "nouser",
              password = "nopass", query = big_query, retries = 2, retry_delay = 0),
    "pull_data failed"
  )

  expect_gt(length(captured), 0L)
  expect_true(all(nchar(captured) < 10000L))
  expect_true(any(grepl("truncated", captured, fixed = TRUE)))
})
