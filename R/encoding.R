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

# ---------------------------------------------------------------------------
# Read side: error classification
# ---------------------------------------------------------------------------

# TRUE iff the condition is RMariaDB's "embedded nul in string" fetch error.
is_embedded_nul_error <- function(e) {
	msg <- tryCatch(conditionMessage(e), error = function(.) "")
	is.character(msg) && length(msg) == 1L && grepl("embedded nul", msg, fixed = TRUE)
}

# ---------------------------------------------------------------------------
# Read side: byte decoding
# ---------------------------------------------------------------------------

# rawToChar that yields a valid UTF-8 string, substituting any invalid bytes.
rawToChar_utf8 <- function(bytes) {
	if (length(bytes) == 0L) return("")
	out <- iconv(list(bytes), from = "UTF-8", to = "UTF-8", sub = "")[1]
	if (is.na(out)) "" else out
}

# Decode one raw vector (a binary-cast DBI cell) into a UTF-8 string.
#
# BOM-aware: FF FE -> UTF-16LE, FE FF -> UTF-16BE (BOM stripped). With no BOM but
# embedded NUL bytes, assumes UTF-16LE (valid UTF-8 never contains 0x00). With no
# NUL, treats the bytes as UTF-8. mode="strip" drops the BOM and all NUL bytes,
# then treats the remainder as UTF-8. Never throws on malformed bytes.
decode_dbi_bytes <- function(raw, mode = c("decode", "strip")) {
	mode <- match.arg(mode)
	# A binary-cast cell for SQL NULL arrives as NULL or a scalar NA — preserve NA.
	if (is.null(raw)) return(NA_character_)
	if (length(raw) == 1L && !is.raw(raw) && is.na(raw)) return(NA_character_)
	if (length(raw) == 0L) return("")          # genuine empty string (empty raw)
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
	# Defensive fallback: only reached for BOM-less NUL data, or if a BOM iconv
	# returned NA above (rare, since safe_iconv uses sub="" and rarely fails).
	if (has_nul) {
		out <- safe_iconv(raw, "UTF-16LE")
		if (!is.na(out)) return(out)
		return(rawToChar_utf8(raw[raw != as.raw(0x00)]))   # last resort: strip NULs
	}
	rawToChar_utf8(raw)                                    # plain UTF-8
}

# ---------------------------------------------------------------------------
# Read side: recovery query construction
# ---------------------------------------------------------------------------

# Build a recovery query that re-fetches character columns as BINARY (NUL-safe).
#
# Wraps the original query as a derived table; CASTs each character-typed column
# to BINARY and passes other columns through unchanged. Column order is preserved
# and names are backtick-quoted. `colinfo` is a data.frame with `name` and `type`
# columns (as returned by DBI::dbColumnInfo); `type == "character"` marks text.
#
# Recovery relies on RMariaDB::dbColumnInfo()$type == "character" identifying text columns.
#
# Note: the outer SELECT ... FROM (<query>) AS rmaria_sub has no ORDER BY, so row order
# relies on MariaDB preserving the derived-table order (true in practice; inner
# LIMIT/ORDER BY is preserved).
build_recovery_query <- function(query, colinfo) {
	if (nrow(colinfo) == 0L) stop("build_recovery_query: colinfo has no rows")
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
# Recovery relies on RMariaDB::dbColumnInfo()$type == "character" identifying text columns.
fetch_candidate_text_columns <- function(con, query) {
	tryCatch({
		res <- RMariaDB::dbSendQuery(con, query)
		on.exit(RMariaDB::dbClearResult(res), add = TRUE)
		ci <- RMariaDB::dbColumnInfo(res)
		ci$name[ci$type == "character"]
	}, error = function(.) character(0))
}

# Re-fetch a query that hit an embedded-NUL column, decoding text to UTF-8.
# Runs on an open connection. Returns a data.frame with an attribute
# "rmaria_nul_columns" naming the columns that contained NUL bytes.
# Recovery relies on RMariaDB::dbColumnInfo()$type == "character" identifying text columns.
recover_nul_fetch <- function(con, query, mode = c("decode", "strip")) {
	mode <- match.arg(mode)
	res <- RMariaDB::dbSendQuery(con, query)
	colinfo <- tryCatch(
		RMariaDB::dbColumnInfo(res),
		finally = RMariaDB::dbClearResult(res)
	)

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
			if (on_nul == "error") {
				candidates <- fetch_candidate_text_columns(con, query)
				stop(rmaria_nul_error(format_embedded_nul_message(candidates, conditionMessage(e))))
			}
			recovered <- tryCatch(
				recover_nul_fetch(con, query, mode = on_nul),
				error = function(e2) {
					candidates <- fetch_candidate_text_columns(con, query)
					stop(rmaria_nul_error(
						format_embedded_nul_message(candidates, conditionMessage(e), conditionMessage(e2))
					))
				}
			)
			cols <- attr(recovered, "rmaria_nul_columns")
			attr(recovered, "rmaria_nul_columns") <- NULL
			if (verbose && length(cols) > 0L) {
				logging::logwarn(
					"Recovered embedded-NUL column(s) [%s] using on_nul=\"%s\".",
					paste(cols, collapse = ", "), on_nul, logger = LOGGER.MAIN
				)
			}
			recovered
		}
	)
}

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
		if (!nm %in% names(raw_df)) {
			warning(sprintf("decode_recovery_columns: column '%s' not found; skipping.", nm))
			next
		}
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
