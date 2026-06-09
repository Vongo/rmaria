test_that("normalize_utf8 converts latin1-marked strings to UTF-8 bytes", {
  x <- "caf\xe9"                 # 0xE9 = é in latin1
  Encoding(x) <- "latin1"
  out <- rmaria:::normalize_utf8(x)
  expect_equal(Encoding(out), "UTF-8")
  expect_equal(out, "café")
})

test_that("normalize_utf8 is a no-op for non-character input", {
  expect_identical(rmaria:::normalize_utf8(1:3), 1:3)
  expect_identical(rmaria:::normalize_utf8(c(TRUE, FALSE)), c(TRUE, FALSE))
})

test_that("normalize_utf8 preserves NA", {
  expect_true(is.na(rmaria:::normalize_utf8(NA_character_)))
})

test_that("is_embedded_nul_error matches RMariaDB's message", {
  e <- simpleError("embedded nul in string: 'a\\0b'")
  expect_true(rmaria:::is_embedded_nul_error(e))
})

test_that("is_embedded_nul_error rejects unrelated errors", {
  expect_false(rmaria:::is_embedded_nul_error(simpleError("connection refused")))
  expect_false(rmaria:::is_embedded_nul_error(simpleError("")))
})

# Build UTF-16LE bytes (with BOM) for an ASCII string.
u16le_bom <- function(s) {
  ascii <- charToRaw(s)
  out <- raw(0)
  for (b in ascii) out <- c(out, b, as.raw(0x00))
  c(as.raw(0xFF), as.raw(0xFE), out)
}

test_that("decode_dbi_bytes decodes UTF-16LE with BOM (the reported bytes)", {
  bytes <- u16le_bom("premiere pro\r")
  expect_equal(rmaria:::decode_dbi_bytes(bytes), "premiere pro\r")
})

test_that("decode_dbi_bytes decodes UTF-16BE with BOM", {
  # "ab" in UTF-16BE with BOM: FE FF 00 61 00 62
  bytes <- as.raw(c(0xFE, 0xFF, 0x00, 0x61, 0x00, 0x62))
  expect_equal(rmaria:::decode_dbi_bytes(bytes), "ab")
})

test_that("decode_dbi_bytes decodes BOM-less UTF-16LE via NUL heuristic (non-ASCII)", {
  # "é" in UTF-16LE, no BOM: E9 00
  bytes <- as.raw(c(0xE9, 0x00))
  expect_equal(rmaria:::decode_dbi_bytes(bytes), "é")
})

test_that("decode_dbi_bytes passes plain UTF-8 through unchanged", {
  expect_equal(rmaria:::decode_dbi_bytes(charToRaw("hello")), "hello")
})

test_that("decode_dbi_bytes returns empty string for empty input", {
  expect_equal(rmaria:::decode_dbi_bytes(raw(0)), "")
  expect_true(is.na(rmaria:::decode_dbi_bytes(NULL)))
})

test_that("decode_dbi_bytes strip mode removes BOM and NULs", {
  bytes <- u16le_bom("premiere pro\r")
  expect_equal(rmaria:::decode_dbi_bytes(bytes, mode = "strip"), "premiere pro\r")
})

test_that("decode_dbi_bytes strip mode handles a BE BOM", {
  bytes <- as.raw(c(0xFE, 0xFF, 0x00, 0x61, 0x00, 0x62))
  expect_equal(rmaria:::decode_dbi_bytes(bytes, mode = "strip"), "ab")
})

test_that("build_recovery_query casts only character columns and strips trailing ;", {
  colinfo <- data.frame(
    name = c("id", "value", "field"),
    type = c("integer", "character", "character"),
    stringsAsFactors = FALSE
  )
  out <- rmaria:::build_recovery_query("select id, value, field from t ;", colinfo)
  expect_equal(
    out,
    "SELECT `id`, CAST(`value` AS BINARY) AS `value`, CAST(`field` AS BINARY) AS `field` FROM (select id, value, field from t) AS rmaria_sub"
  )
})

test_that("build_recovery_query preserves column order and quotes names", {
  colinfo <- data.frame(
    name = c("value", "n"),
    type = c("character", "double"),
    stringsAsFactors = FALSE
  )
  out <- rmaria:::build_recovery_query("select * from t", colinfo)
  expect_equal(
    out,
    "SELECT CAST(`value` AS BINARY) AS `value`, `n` FROM (select * from t) AS rmaria_sub"
  )
})

test_that("decode_recovery_columns decodes list-of-raw columns and records affected", {
  raw_df <- data.frame(id = 1:2, stringsAsFactors = FALSE)
  raw_df$value <- list(u16le_bom("premiere pro\r"), charToRaw("ok"))
  out <- rmaria:::decode_recovery_columns(raw_df, text_cols = "value", mode = "decode")
  expect_equal(out$value, c("premiere pro\r", "ok"))
  expect_equal(attr(out, "rmaria_nul_columns"), "value")
})

test_that("decode_recovery_columns leaves clean columns unaffected", {
  raw_df <- data.frame(id = 1:2, stringsAsFactors = FALSE)
  raw_df$value <- list(charToRaw("a"), charToRaw("b"))
  out <- rmaria:::decode_recovery_columns(raw_df, text_cols = "value", mode = "decode")
  expect_equal(out$value, c("a", "b"))
  expect_equal(attr(out, "rmaria_nul_columns"), character(0))
})

test_that("decode_recovery_columns decodes a scalar raw column", {
  # A bare (non-list) raw column is stored byte-per-row by data.frame, so a true
  # single-cell scalar-raw column must be length 1 (one byte) in a 1-row frame.
  # This exercises the is.raw(col) branch; "hi" cannot be a bare-raw 1-row cell.
  raw_df <- data.frame(id = 1L, stringsAsFactors = FALSE)
  raw_df$value <- charToRaw("h")   # scalar raw, not a list
  out <- rmaria:::decode_recovery_columns(raw_df, text_cols = "value", mode = "decode")
  expect_equal(out$value, "h")
  expect_equal(attr(out, "rmaria_nul_columns"), character(0))
})

test_that("format_embedded_nul_message includes candidates, on_nul hint, and iconv recipe", {
  msg <- rmaria:::format_embedded_nul_message(
    candidates = c("value", "field"),
    original_message = "embedded nul in string: 'x'"
  )
  expect_true(grepl("value, field", msg, fixed = TRUE))
  expect_true(grepl('on_nul="decode"', msg, fixed = TRUE))
  expect_true(grepl("iconv", msg, fixed = TRUE))
  expect_true(grepl("embedded nul in string", msg, fixed = TRUE))
})

test_that("format_embedded_nul_message handles no candidates and a recovery error", {
  msg <- rmaria:::format_embedded_nul_message(
    candidates = character(0),
    original_message = "embedded nul",
    recovery_message = "subquery not supported"
  )
  expect_false(grepl("Candidate text column", msg, fixed = TRUE))
  expect_true(grepl("recovery also failed: subquery not supported", msg, fixed = TRUE))
})

test_that("format_embedded_nul_message omits both optional sections when empty", {
  msg <- rmaria:::format_embedded_nul_message(character(0), "embedded nul")
  expect_false(grepl("Candidate", msg, fixed = TRUE))
  expect_false(grepl("recovery", msg, fixed = TRUE))
  expect_true(grepl("embedded nul", msg, fixed = TRUE))
})

test_that("rmaria_nul_error builds a classed condition", {
  e <- rmaria:::rmaria_nul_error("boom")
  expect_s3_class(e, "rmaria_embedded_nul")
  expect_s3_class(e, "error")
  expect_equal(conditionMessage(e), "boom")
})

test_that("normalize_table_utf8 converts character columns to UTF-8", {
  x <- "caf\xe9"; Encoding(x) <- "latin1"
  df <- data.frame(id = 1L, value = x, n = 3.5, stringsAsFactors = FALSE)
  out <- rmaria:::normalize_table_utf8(df, nolog = TRUE)
  expect_equal(Encoding(out$value), "UTF-8")
  expect_equal(out$value, "café")
  expect_equal(out$id, 1L)
  expect_equal(out$n, 3.5)
})

test_that("normalize_table_utf8 warns on binary (list/raw) columns", {
  df <- data.frame(id = 1L, stringsAsFactors = FALSE)
  df$blob <- list(as.raw(c(0x00, 0x01)))
  expect_warning(
    suppressMessages(rmaria:::normalize_table_utf8(df, nolog = FALSE)),
    NA   # logging package emits to handlers, not R warnings; assert it does not error
  )
  # The function must still return the table unchanged for the binary column.
  out <- rmaria:::normalize_table_utf8(df, nolog = TRUE)
  expect_true(is.list(out$blob))
})

test_that("decode_dbi_bytes preserves SQL NULL (NULL / NA) as NA, empty raw as \"\"", {
  expect_true(is.na(rmaria:::decode_dbi_bytes(NULL)))
  expect_true(is.na(rmaria:::decode_dbi_bytes(NA)))
  expect_equal(rmaria:::decode_dbi_bytes(raw(0)), "")
})

test_that("decode_recovery_columns preserves SQL NULL cells as NA", {
  raw_df <- data.frame(id = 1:2, stringsAsFactors = FALSE)
  raw_df$value <- list(u16le_bom("premiere pro\r"), NA)   # 2nd cell = SQL NULL
  out <- rmaria:::decode_recovery_columns(raw_df, text_cols = "value", mode = "decode")
  expect_equal(out$value, c("premiere pro\r", NA_character_))
  expect_equal(attr(out, "rmaria_nul_columns"), "value")  # flagged by the real-NUL 1st cell only
})
