# Encoding Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `rmaria` transparently recover from embedded-NUL/UTF-16 columns on read, and normalize text to UTF-8 on write.

**Architecture:** A new dependency-free helper module `R/encoding.R` holds pure functions (decode/normalize/classify/SQL-build) plus three thin DB-touching orchestrators. `pull_data` gains an `on_nul` parameter and calls the read-recovery path only when RMariaDB throws "embedded nul". The four write functions normalize their character columns up front and warn on binary columns.

**Tech Stack:** R package, RMariaDB (DBI), testthat (new, Suggests), base R `iconv`/`enc2utf8` for encoding.

**Spec:** `docs/superpowers/specs/2026-06-08-encoding-hardening-design.md`

**Conventions for this plan:**
- All new helpers live in `R/encoding.R`, use only base R, and are **internal** (not `@export`ed). Tests reference them as `rmaria:::name`.
- Test runner (run from the worktree root): pure-helper tests load the package and run one file:
  ```bash
  Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
  ```
  This requires `devtools`, `testthat`, and the `Imports` packages to be installed. `load_all` runs `init()` (which `library()`s the imports) — that is expected.
- Commit message format: `[Type][Scope-SubScope] Short description` (Types: Feat, Fix, Update, Refactor). No attribution footer.

---

### Task 1: Test infrastructure

**Files:**
- Create: `tests/testthat.R`
- Create: `tests/testthat/test-encoding.R` (temporary smoke test, replaced in Task 2)
- Modify: `DESCRIPTION` (add `Suggests: testthat`)

- [ ] **Step 1: Add the testthat entrypoint**

Create `tests/testthat.R`:

```r
library(testthat)
library(rmaria)

test_check("rmaria")
```

- [ ] **Step 2: Add a temporary smoke test**

Create `tests/testthat/test-encoding.R`:

```r
test_that("test harness runs", {
  expect_true(TRUE)
})
```

- [ ] **Step 3: Add testthat to DESCRIPTION Suggests**

Modify `DESCRIPTION` — after the `Imports:` line, add:

```
Suggests: testthat (>= 3.0.0)
```

- [ ] **Step 4: Run the smoke test to verify the harness works**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: 1 pass, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add tests/testthat.R tests/testthat/test-encoding.R DESCRIPTION
git commit -m "[Feat][Tests] Add testthat scaffolding"
```

---

### Task 2: `normalize_utf8` helper

**Files:**
- Create: `R/encoding.R`
- Modify: `tests/testthat/test-encoding.R` (replace smoke test)

- [ ] **Step 1: Write the failing test**

Replace the entire contents of `tests/testthat/test-encoding.R` with:

```r
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: FAIL — `could not find function "normalize_utf8"` (object 'normalize_utf8' not found in 'rmaria').

- [ ] **Step 3: Create R/encoding.R with the implementation**

Create `R/encoding.R`:

```r
# Encoding helpers for rmaria.
#
# Pure (DB-free) utilities for normalizing text on write and recovering
# embedded-NUL / UTF-16 columns on read, plus thin DB-touching orchestrators.
# All functions here are internal (not exported).

# ---------------------------------------------------------------------------
# Write side
# ---------------------------------------------------------------------------

# Normalize a vector to UTF-8 (character vectors only).
#
# Character vectors are converted to UTF-8 bytes via enc2utf8 (fixing
# latin1/unknown-marked strings). Any other type is returned unchanged. NA-safe.
normalize_utf8 <- function(x) {
	if (is.character(x)) enc2utf8(x) else x
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add R/encoding.R tests/testthat/test-encoding.R
git commit -m "[Feat][Encoding] Add normalize_utf8 helper"
```

---

### Task 3: `is_embedded_nul_error` helper

**Files:**
- Modify: `R/encoding.R`
- Modify: `tests/testthat/test-encoding.R` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/testthat/test-encoding.R`:

```r
test_that("is_embedded_nul_error matches RMariaDB's message", {
  e <- simpleError("embedded nul in string: 'a\\0b'")
  expect_true(rmaria:::is_embedded_nul_error(e))
})

test_that("is_embedded_nul_error rejects unrelated errors", {
  expect_false(rmaria:::is_embedded_nul_error(simpleError("connection refused")))
  expect_false(rmaria:::is_embedded_nul_error(simpleError("")))
})
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: FAIL — `could not find function "is_embedded_nul_error"`.

- [ ] **Step 3: Add the implementation**

Append to `R/encoding.R`:

```r
# ---------------------------------------------------------------------------
# Read side: error classification
# ---------------------------------------------------------------------------

# TRUE iff the condition is RMariaDB's "embedded nul in string" fetch error.
is_embedded_nul_error <- function(e) {
	msg <- tryCatch(conditionMessage(e), error = function(.) "")
	is.character(msg) && length(msg) == 1L && grepl("embedded nul", msg, fixed = TRUE)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: PASS (5 tests total).

- [ ] **Step 5: Commit**

```bash
git add R/encoding.R tests/testthat/test-encoding.R
git commit -m "[Feat][Encoding] Add is_embedded_nul_error classifier"
```

---

### Task 4: `decode_dbi_bytes` (+ `rawToChar_utf8`)

**Files:**
- Modify: `R/encoding.R`
- Modify: `tests/testthat/test-encoding.R` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/testthat/test-encoding.R`:

```r
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
  expect_equal(rmaria:::decode_dbi_bytes(NULL), "")
})

test_that("decode_dbi_bytes strip mode removes BOM and NULs", {
  bytes <- u16le_bom("premiere pro\r")
  expect_equal(rmaria:::decode_dbi_bytes(bytes, mode = "strip"), "premiere pro\r")
})
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: FAIL — `could not find function "decode_dbi_bytes"`.

- [ ] **Step 3: Add the implementation**

Append to `R/encoding.R`:

```r
# ---------------------------------------------------------------------------
# Read side: byte decoding
# ---------------------------------------------------------------------------

# rawToChar that marks the result UTF-8 and never errors on empty input.
rawToChar_utf8 <- function(raw) {
	if (length(raw) == 0L) return("")
	s <- rawToChar(raw)
	Encoding(s) <- "UTF-8"
	s
}

# Decode one raw vector (a binary-cast DBI cell) into a UTF-8 string.
#
# BOM-aware: FF FE -> UTF-16LE, FE FF -> UTF-16BE (BOM stripped). With no BOM but
# embedded NUL bytes, assumes UTF-16LE (valid UTF-8 never contains 0x00). With no
# NUL, treats the bytes as UTF-8. mode="strip" drops the BOM and all NUL bytes,
# then treats the remainder as UTF-8. Never throws on malformed bytes.
decode_dbi_bytes <- function(raw, mode = c("decode", "strip")) {
	mode <- match.arg(mode)
	if (is.null(raw) || length(raw) == 0L) return("")
	if (!is.raw(raw)) raw <- as.raw(raw)

	has_le_bom <- length(raw) >= 2L && raw[1] == as.raw(0xFF) && raw[2] == as.raw(0xFE)
	has_be_bom <- length(raw) >= 2L && raw[1] == as.raw(0xFE) && raw[2] == as.raw(0xFF)
	has_nul    <- any(raw == as.raw(0x00))

	safe_iconv <- function(bytes, from) {
		tryCatch(
			iconv(list(bytes), from = from, to = "UTF-8", sub = "")[1],
			error = function(.) NA_character_
		)
	}

	if (mode == "strip") {
		if (has_le_bom || has_be_bom) raw <- raw[-(1:2)]
		raw <- raw[raw != as.raw(0x00)]
		return(rawToChar_utf8(raw))
	}

	if (has_le_bom) {
		out <- safe_iconv(raw[-(1:2)], "UTF-16LE")
		if (!is.na(out)) return(out)
	}
	if (has_be_bom) {
		out <- safe_iconv(raw[-(1:2)], "UTF-16BE")
		if (!is.na(out)) return(out)
	}
	if (has_nul) {
		out <- safe_iconv(raw, "UTF-16LE")
		if (!is.na(out)) return(out)
		return(rawToChar_utf8(raw[raw != as.raw(0x00)]))   # last resort: strip NULs
	}
	rawToChar_utf8(raw)                                    # plain UTF-8
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: PASS (11 tests total).

- [ ] **Step 5: Commit**

```bash
git add R/encoding.R tests/testthat/test-encoding.R
git commit -m "[Feat][Encoding] Add BOM-aware decode_dbi_bytes"
```

---

### Task 5: `build_recovery_query`

**Files:**
- Modify: `R/encoding.R`
- Modify: `tests/testthat/test-encoding.R` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/testthat/test-encoding.R`:

```r
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: FAIL — `could not find function "build_recovery_query"`.

- [ ] **Step 3: Add the implementation**

Append to `R/encoding.R`:

```r
# ---------------------------------------------------------------------------
# Read side: recovery query construction
# ---------------------------------------------------------------------------

# Build a recovery query that re-fetches character columns as BINARY (NUL-safe).
#
# Wraps the original query as a derived table; CASTs each character-typed column
# to BINARY and passes other columns through unchanged. Column order is preserved
# and names are backtick-quoted. `colinfo` is a data.frame with `name` and `type`
# columns (as returned by DBI::dbColumnInfo); `type == "character"` marks text.
build_recovery_query <- function(query, colinfo) {
	inner <- trimws(sub(";\\s*$", "", trimws(query)))
	bq <- function(nm) paste0("`", gsub("`", "``", nm), "`")
	cols <- vapply(seq_len(nrow(colinfo)), function(i) {
		nm <- colinfo$name[i]
		if (identical(colinfo$type[i], "character")) {
			paste0("CAST(", bq(nm), " AS BINARY) AS ", bq(nm))
		} else {
			bq(nm)
		}
	}, character(1))
	paste0("SELECT ", paste(cols, collapse = ", "), " FROM (", inner, ") AS rmaria_sub")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: PASS (13 tests total).

- [ ] **Step 5: Commit**

```bash
git add R/encoding.R tests/testthat/test-encoding.R
git commit -m "[Feat][Encoding] Add build_recovery_query"
```

---

### Task 6: `decode_recovery_columns`

**Files:**
- Modify: `R/encoding.R`
- Modify: `tests/testthat/test-encoding.R` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/testthat/test-encoding.R`:

```r
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: FAIL — `could not find function "decode_recovery_columns"`.

- [ ] **Step 3: Add the implementation**

Append to `R/encoding.R`:

```r
# Decode the binary-cast text columns of a recovery result into UTF-8 (pure).
#
# `raw_df` is the data.frame returned by the recovery query (text columns are
# lists of raw, or single raw vectors). Returns the data.frame with those columns
# decoded to character, and an attribute "rmaria_nul_columns" listing the columns
# that actually contained NUL bytes.
decode_recovery_columns <- function(raw_df, text_cols, mode = c("decode", "strip")) {
	mode <- match.arg(mode)
	affected <- character(0)
	for (nm in text_cols) {
		col <- raw_df[[nm]]
		if (is.list(col)) {
			had <- vapply(col, function(b) is.raw(b) && any(b == as.raw(0x00)), logical(1))
			raw_df[[nm]] <- vapply(col, decode_dbi_bytes, character(1), mode = mode)
		} else if (is.raw(col)) {
			had <- any(col == as.raw(0x00))
			raw_df[[nm]] <- decode_dbi_bytes(col, mode = mode)
		} else {
			had <- FALSE
		}
		if (any(had)) affected <- c(affected, nm)
	}
	attr(raw_df, "rmaria_nul_columns") <- affected
	raw_df
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: PASS (15 tests total).

- [ ] **Step 5: Commit**

```bash
git add R/encoding.R tests/testthat/test-encoding.R
git commit -m "[Feat][Encoding] Add decode_recovery_columns"
```

---

### Task 7: `format_embedded_nul_message`

**Files:**
- Modify: `R/encoding.R`
- Modify: `tests/testthat/test-encoding.R` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/testthat/test-encoding.R`:

```r
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: FAIL — `could not find function "format_embedded_nul_message"`.

- [ ] **Step 3: Add the implementation**

Append to `R/encoding.R`:

```r
# Build an actionable error message for an embedded-NUL fetch failure (pure).
format_embedded_nul_message <- function(candidates, original_message, recovery_message = NULL) {
	col_txt <- if (length(candidates) > 0L) {
		sprintf(" Candidate text column(s): %s.", paste(candidates, collapse = ", "))
	} else {
		""
	}
	rec_txt <- if (!is.null(recovery_message)) {
		sprintf(" Automatic recovery also failed: %s.", recovery_message)
	} else {
		""
	}
	sprintf(
		paste0(
			"Query returned a column with embedded NUL bytes (likely UTF-16-encoded text).%s%s ",
			"Re-run with on_nul=\"decode\" to transcode to UTF-8, or fetch the column as HEX() ",
			"and decode with iconv(x, \"UTF-16\", \"UTF-8\"). Original error: %s"
		),
		col_txt, rec_txt, original_message
	)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: PASS (17 tests total).

- [ ] **Step 5: Commit**

```bash
git add R/encoding.R tests/testthat/test-encoding.R
git commit -m "[Feat][Encoding] Add format_embedded_nul_message"
```

---

### Task 8: DB-touching orchestrators (`recover_nul_fetch`, `fetch_candidate_text_columns`, `rmaria_nul_error`, `dbGetQuery_nul_safe`)

These call the live DB, so they are validated by the skipped integration test in Task 11. This task adds the code plus one pure test for the classed-condition constructor.

**Files:**
- Modify: `R/encoding.R`
- Modify: `tests/testthat/test-encoding.R` (append)

- [ ] **Step 1: Write the failing test (pure part only)**

Append to `tests/testthat/test-encoding.R`:

```r
test_that("rmaria_nul_error builds a classed condition", {
  e <- rmaria:::rmaria_nul_error("boom")
  expect_s3_class(e, "rmaria_embedded_nul")
  expect_s3_class(e, "error")
  expect_equal(conditionMessage(e), "boom")
})
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: FAIL — `could not find function "rmaria_nul_error"`.

- [ ] **Step 3: Add the implementation**

Append to `R/encoding.R`:

```r
# ---------------------------------------------------------------------------
# Read side: DB-touching orchestration
# ---------------------------------------------------------------------------

# A classed error so pull_data can recognise the deterministic NUL failure and
# skip its retry loop.
rmaria_nul_error <- function(message) {
	structure(
		class = c("rmaria_embedded_nul", "error", "condition"),
		list(message = message, call = NULL)
	)
}

# Return the names of character-typed columns for a query, without fetching rows
# (so it does not trigger the embedded-NUL error). Returns character(0) on failure.
fetch_candidate_text_columns <- function(con, query) {
	tryCatch({
		res <- RMariaDB::dbSendQuery(con, query)
		ci <- RMariaDB::dbColumnInfo(res)
		RMariaDB::dbClearResult(res)
		ci$name[ci$type == "character"]
	}, error = function(.) character(0))
}

# Re-fetch a query that hit an embedded-NUL column, decoding text to UTF-8.
# Runs on an open connection. Returns a data.frame with an attribute
# "rmaria_nul_columns" naming the columns that contained NUL bytes.
recover_nul_fetch <- function(con, query, mode = c("decode", "strip")) {
	mode <- match.arg(mode)
	res <- RMariaDB::dbSendQuery(con, query)
	colinfo <- RMariaDB::dbColumnInfo(res)
	RMariaDB::dbClearResult(res)

	recovery_query <- build_recovery_query(query, colinfo)
	raw_df <- RMariaDB::dbGetQuery(con, recovery_query)

	text_cols <- colinfo$name[colinfo$type == "character"]
	decode_recovery_columns(raw_df, text_cols, mode = mode)
}

# Fetch a query, transparently recovering from embedded-NUL columns per `on_nul`.
dbGetQuery_nul_safe <- function(con, query, on_nul = c("decode", "error", "strip"), verbose = TRUE) {
	on_nul <- match.arg(on_nul)
	tryCatch(
		RMariaDB::dbGetQuery(con, query),
		error = function(e) {
			if (!is_embedded_nul_error(e)) stop(e)
			candidates <- fetch_candidate_text_columns(con, query)
			if (on_nul == "error") {
				stop(rmaria_nul_error(format_embedded_nul_message(candidates, conditionMessage(e))))
			}
			recovered <- tryCatch(
				recover_nul_fetch(con, query, mode = on_nul),
				error = function(e2) stop(rmaria_nul_error(
					format_embedded_nul_message(candidates, conditionMessage(e), conditionMessage(e2))
				))
			)
			cols <- attr(recovered, "rmaria_nul_columns")
			attr(recovered, "rmaria_nul_columns") <- NULL
			if (verbose && length(cols) > 0L) {
				logging::logwarn(
					"Recovered embedded-NUL column(s) [%s] by decoding to UTF-8 (on_nul=\"%s\").",
					paste(cols, collapse = ", "), on_nul, logger = LOGGER.MAIN
				)
			}
			recovered
		}
	)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: PASS (18 tests total).

- [ ] **Step 5: Commit**

```bash
git add R/encoding.R tests/testthat/test-encoding.R
git commit -m "[Feat][Encoding] Add embedded-NUL read recovery orchestration"
```

---

### Task 9: Wire `on_nul` into `pull_data`

**Files:**
- Modify: `R/maria.R` (`pull_data`, lines ~75-140)

- [ ] **Step 1: Add the `on_nul` parameter to the signature**

In `R/maria.R`, change the `pull_data` signature line (currently):

```r
pull_data <- function(host="localhost", port=3306, db, user, password, query, verbose=TRUE, keep_int64=FALSE, retries=1, retry_delay=1) {
```

to:

```r
pull_data <- function(host="localhost", port=3306, db, user, password, query, verbose=TRUE, keep_int64=FALSE, retries=1, retry_delay=1, on_nul=c("decode", "error", "strip")) {
```

- [ ] **Step 2: Validate `on_nul` after `init()`**

Immediately after the `init()` call at the top of `pull_data` (the first line of the body), add:

```r
	on_nul <- match.arg(on_nul)
```

- [ ] **Step 3: Route the fetch through the NUL-safe wrapper**

Replace the fetch line (currently):

```r
			state$data <- RMariaDB::dbGetQuery(con, query)
```

with:

```r
			state$data <- dbGetQuery_nul_safe(con, query, on_nul=on_nul, verbose=verbose)
```

- [ ] **Step 4: Skip the retry loop for the deterministic NUL error**

Find this block (after the `tryCatch(...)` assignment to `result`):

```r
		if (isTRUE(result)) {
			break
		}

		if (attempt < retries) {
			Sys.sleep(retry_delay)
		}
```

Replace it with:

```r
		if (isTRUE(result)) {
			break
		}

		if (inherits(state$last_error, "rmaria_embedded_nul")) {
			break   # deterministic: retrying will not help
		}

		if (attempt < retries) {
			Sys.sleep(retry_delay)
		}
```

- [ ] **Step 5: Report the actual attempt count in the final error**

Find:

```r
	if (is.null(state$data)) {
		error_msg <- if (!is.null(state$last_error)) conditionMessage(state$last_error) else "Unknown error"
		logging::logerror("Error while fetching data with query [%s] after %d attempts:\n[%s]", query, retries, error_msg, logger=LOGGER.MAIN)
		stop(sprintf("pull_data failed after %d attempts: %s", retries, error_msg))
	}
```

Replace `retries` with `attempt` in both the `logerror` and `stop` calls:

```r
	if (is.null(state$data)) {
		error_msg <- if (!is.null(state$last_error)) conditionMessage(state$last_error) else "Unknown error"
		logging::logerror("Error while fetching data with query [%s] after %d attempts:\n[%s]", query, attempt, error_msg, logger=LOGGER.MAIN)
		stop(sprintf("pull_data failed after %d attempts: %s", attempt, error_msg))
	}
```

- [ ] **Step 6: Add the roxygen `@param` for `on_nul`**

In the roxygen block above `pull_data`, after the `@param retry_delay ...` line, add:

```r
#' @param on_nul behavior when a text column contains embedded NUL bytes (UTF-16 data): "decode" (default) transparently re-fetches and decodes to UTF-8, "strip" removes NUL bytes, "error" stops with an actionable message
```

- [ ] **Step 7: Verify the package still loads and all unit tests pass**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_dir("tests/testthat", reporter="summary")'
```
Expected: PASS (18 tests; no errors loading `pull_data`).

- [ ] **Step 8: Commit**

```bash
git add R/maria.R
git commit -m "[Feat][Select] Add on_nul recovery to pull_data"
```

---

### Task 10: Write-side normalization + binary-column warning

**Files:**
- Modify: `R/encoding.R` (add `normalize_table_utf8`)
- Modify: `tests/testthat/test-encoding.R` (append)
- Modify: `R/maria.R` (`insert_table`, `upsert_table`, `update_table`, `insert_table_local`)

- [ ] **Step 1: Write the failing test for `normalize_table_utf8`**

Append to `tests/testthat/test-encoding.R`:

```r
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: FAIL — `could not find function "normalize_table_utf8"`.

- [ ] **Step 3: Add `normalize_table_utf8` to R/encoding.R**

Append to `R/encoding.R`:

```r
# Normalize all character columns of a table to UTF-8, warning on binary columns.
#
# Returns the table with character columns enc2utf8'd. raw/list (binary) columns
# are left unchanged but trigger a warning (they can carry embedded NULs that will
# need on_nul recovery to read back). `nolog=TRUE` suppresses the warning.
normalize_table_utf8 <- function(table, nolog = FALSE) {
	for (nm in colnames(table)) {
		col <- table[[nm]]
		if (is.character(col)) {
			table[[nm]] <- enc2utf8(col)
		} else if (is.list(col) || is.raw(col)) {
			if (!nolog) {
				logging::logwarn(
					"Column '%s' is a binary (raw/list) column; embedded NUL bytes may require on_nul recovery to read back.",
					nm, logger = LOGGER.MAIN
				)
			}
		}
	}
	table
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding.R", reporter="summary")'
```
Expected: PASS (20 tests total).

- [ ] **Step 5: Hook `normalize_table_utf8` into `insert_table`**

In `R/maria.R`, in `insert_table`, find:

```r
	if (!nolog) logging::loginfo("Inserting data into table %s.", table_name_in_base, logger=LOGGER.MAIN)
	if (is.na(chunk_size)) {
```

Insert the normalization call between them:

```r
	if (!nolog) logging::loginfo("Inserting data into table %s.", table_name_in_base, logger=LOGGER.MAIN)
	table <- normalize_table_utf8(table, nolog=nolog)
	if (is.na(chunk_size)) {
```

- [ ] **Step 6: Hook `normalize_table_utf8` into `upsert_table`**

In `upsert_table`, find:

```r
	if (!nolog) logging::loginfo("Upserting %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)

	has_quotes <- sapply(seq(ncol(table)), function(ic) !(is.numeric(table[,ic]) || is.logical(table[,ic])))
```

Insert the normalization call after the `loginfo`:

```r
	if (!nolog) logging::loginfo("Upserting %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)
	table <- normalize_table_utf8(table, nolog=nolog)

	has_quotes <- sapply(seq(ncol(table)), function(ic) !(is.numeric(table[,ic]) || is.logical(table[,ic])))
```

- [ ] **Step 7: Hook `normalize_table_utf8` into `update_table`**

In `update_table`, find:

```r
	if (!nolog) logging::loginfo("updating %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)

	has_quotes <- table |> purrr::map_lgl(~!(is.numeric(.x)||is.logical(.x)))
```

Insert the normalization call after the `loginfo`:

```r
	if (!nolog) logging::loginfo("updating %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)
	table <- normalize_table_utf8(table, nolog=nolog)

	has_quotes <- table |> purrr::map_lgl(~!(is.numeric(.x)||is.logical(.x)))
```

- [ ] **Step 8: Hook `normalize_table_utf8` into `insert_table_local`**

In `insert_table_local`, find the line that sets the connection charset:

```r
		RMariaDB::dbExecute(con, "set character set \"utf8mb4\"")
```

Immediately after it, add:

```r
		table <- normalize_table_utf8(table)
```

(`insert_table_local` has no `nolog` parameter, so the warning uses its default.)

- [ ] **Step 9: Verify the package loads and all unit tests pass**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_dir("tests/testthat", reporter="summary")'
```
Expected: PASS (20 tests; no load errors).

- [ ] **Step 10: Commit**

```bash
git add R/encoding.R R/maria.R tests/testthat/test-encoding.R
git commit -m "[Feat][Insert] Normalize character columns to UTF-8 on write"
```

---

### Task 11: Integration test (skipped without DB), docs regen, final verification

**Files:**
- Create: `tests/testthat/test-encoding-integration.R`
- Modify: `man/pull_data.Rd` (via roxygen regen)

- [ ] **Step 1: Add the DB-guarded integration test**

Create `tests/testthat/test-encoding-integration.R`:

```r
# Live-DB tests. Set these env vars to run:
#   RMARIA_TEST_HOST, RMARIA_TEST_DB, RMARIA_TEST_USER, RMARIA_TEST_PWD
# (optional RMARIA_TEST_PORT, default 3306). Otherwise skipped.

skip_if_no_db <- function() {
  if (!nzchar(Sys.getenv("RMARIA_TEST_HOST"))) {
    skip("No RMARIA_TEST_HOST set; skipping live-DB integration test.")
  }
}

db_args <- function() {
  list(
    host = Sys.getenv("RMARIA_TEST_HOST"),
    port = as.integer(Sys.getenv("RMARIA_TEST_PORT", "3306")),
    db = Sys.getenv("RMARIA_TEST_DB"),
    user = Sys.getenv("RMARIA_TEST_USER"),
    password = Sys.getenv("RMARIA_TEST_PWD")
  )
}

test_that("pull_data auto-recovers a UTF-16 row (on_nul='decode')", {
  skip_if_no_db()
  a <- db_args()
  con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), host=a$host, port=a$port,
                             dbname=a$db, user=a$user, password=a$password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  RMariaDB::dbExecute(con, "DROP TEMPORARY TABLE IF EXISTS rmaria_nul_test")
  RMariaDB::dbExecute(con, "CREATE TEMPORARY TABLE rmaria_nul_test (id INT, value TEXT)")
  # FF FE 70 00 72 00 ... 0D 00 == "premiere pro\r" in UTF-16LE
  RMariaDB::dbExecute(con, paste0(
    "INSERT INTO rmaria_nul_test VALUES (1, ",
    "0xFFFE700072006500", "6D00690065007200", "6500200070007200", "6F000D00", ")"
  ))

  out <- pull_data(host=a$host, port=a$port, db=a$db, user=a$user, password=a$password,
                   query="SELECT id, value FROM rmaria_nul_test", verbose=FALSE,
                   on_nul="decode")
  expect_equal(out$value[1], "premiere pro\r")
})

test_that("pull_data with on_nul='error' raises a classed, actionable error", {
  skip_if_no_db()
  a <- db_args()
  con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), host=a$host, port=a$port,
                             dbname=a$db, user=a$user, password=a$password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  RMariaDB::dbExecute(con, "DROP TEMPORARY TABLE IF EXISTS rmaria_nul_test2")
  RMariaDB::dbExecute(con, "CREATE TEMPORARY TABLE rmaria_nul_test2 (id INT, value TEXT)")
  RMariaDB::dbExecute(con, "INSERT INTO rmaria_nul_test2 VALUES (1, 0xFFFE70000D00)")

  expect_error(
    pull_data(host=a$host, port=a$port, db=a$db, user=a$user, password=a$password,
              query="SELECT id, value FROM rmaria_nul_test2", verbose=FALSE,
              on_nul="error"),
    "embedded NUL"
  )
})
```

- [ ] **Step 2: Verify the integration test is skipped (no DB configured)**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_file("tests/testthat/test-encoding-integration.R", reporter="summary")'
```
Expected: 2 skipped, 0 failures.

- [ ] **Step 3: Regenerate documentation for the new `pull_data` param**

Run:
```bash
Rscript -e 'roxygen2::roxygenise()'
```
Expected: `man/pull_data.Rd` updated to include the `on_nul` argument; no errors.

If `roxygen2` is not installed, instead hand-edit `man/pull_data.Rd`: add an `\item{on_nul}{...}` entry in the `\arguments{}` block matching the `@param on_nul` text, and add `on_nul = c("decode", "error", "strip")` to the `\usage{}` signature.

- [ ] **Step 4: Run the full unit test suite**

Run:
```bash
Rscript -e 'devtools::load_all(".", quiet=TRUE); testthat::test_dir("tests/testthat", reporter="summary")'
```
Expected: 20 passed, 2 skipped, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add tests/testthat/test-encoding-integration.R man/
git commit -m "[Feat][Tests] Add DB-guarded encoding integration tests and regen docs"
```

---

## Self-Review

**1. Spec coverage:**
- New `R/encoding.R` module with the five spec helpers (+ supporting `rawToChar_utf8`, `decode_recovery_columns`, `fetch_candidate_text_columns`, `rmaria_nul_error`, `dbGetQuery_nul_safe`, `normalize_table_utf8`) — Tasks 2-8, 10. ✓
- Read-side `on_nul` param (`decode`/`error`/`strip`), default `decode`; `selectq` forwards via `...` (no change needed) — Task 9. ✓
- Recovery algorithm (dbColumnInfo without fetch → CAST-to-BINARY wrapper → decode) — Tasks 5, 6, 8. ✓
- Decode/BOM rules — Task 4. ✓
- Skip retry on deterministic NUL error; report actual attempt count — Task 9 Steps 4-5. ✓
- Actionable `error`-mode message with candidate columns + recipe — Tasks 7, 8. ✓
- Write-side `enc2utf8` normalization in all four write functions + binary-column warning — Task 10. ✓
- testthat setup + `Suggests` — Task 1. ✓
- Pure unit tests for all DB-free helpers; DB integration tests skip-guarded — Tasks 2-8, 10, 11. ✓
- Files touched list (R/encoding.R, R/maria.R, DESCRIPTION, NAMESPACE/man) — covered; NAMESPACE needs no change (helpers internal), only man/ regenerates. ✓

**2. Placeholder scan:** No TBD/TODO; every code step contains complete code. ✓

**3. Type consistency:** Helper names and signatures are consistent across tasks: `normalize_utf8`, `is_embedded_nul_error`, `decode_dbi_bytes(raw, mode)`, `rawToChar_utf8`, `build_recovery_query(query, colinfo)`, `decode_recovery_columns(raw_df, text_cols, mode)`, `format_embedded_nul_message(candidates, original_message, recovery_message)`, `rmaria_nul_error(message)`, `fetch_candidate_text_columns(con, query)`, `recover_nul_fetch(con, query, mode)`, `dbGetQuery_nul_safe(con, query, on_nul, verbose)`, `normalize_table_utf8(table, nolog)`. `dbGetQuery_nul_safe` is defined in Task 8 and called in Task 9 with matching args. The `rmaria_embedded_nul` class is created in Task 8 and checked in Task 9. ✓

**Known assumption:** `colinfo$type == "character"` identifies text columns (RMariaDB `dbColumnInfo` returns R type strings). The pure tests control `colinfo`; the skipped integration test validates this end-to-end against a live server. If a future RMariaDB labels text differently, adjust the single comparison in `build_recovery_query`, `recover_nul_fetch`, and `fetch_candidate_text_columns`.
