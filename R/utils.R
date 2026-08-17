# Internal utilities vendored from rutils so rmaria has no fragile cross-package
# build-time dependency. Not exported.

# Best-effort terminal width, degrading gracefully when there is no TTY
# (e.g. non-interactive sessions, CI). Falls back to $COLUMNS then options(width).
.term_width <- function() {
	w <- suppressWarnings(tryCatch(
		as.integer(strsplit(system("stty size", intern = TRUE, ignore.stderr = TRUE), " ")[[1]])[2],
		error = function(e) NA_integer_,
		warning = function(w) NA_integer_
	))
	if (length(w) != 1L || is.na(w) || w <= 0L) {
		cols <- suppressWarnings(as.integer(Sys.getenv("COLUMNS", "")))
		w <- if (length(cols) == 1L && !is.na(cols) && cols > 0L) cols else getOption("width", 80L)
	}
	as.integer(w)
}

# Lightweight progress bar (vendored from rutils). Cosmetic; used only when
# progress_bar = TRUE (interactive). create_pb() builds state, update_pb() draws.
create_pb <- function(nb_iter, bar_style = sample(c("simple", "pc"), 1),
                      time_style = sample(c("cd", "end"), 1)) {
	list(
		dep_time   = Sys.time(),
		tot_iter   = nb_iter,
		bar_style  = bar_style,
		time_style = time_style
	)
}

update_pb <- function(pb, index) {
	terminal_width <- .term_width()
	progress <- index / pb$tot_iter
	elapsed <- Sys.time() - pb$dep_time
	total_time <- (elapsed / progress)
	exp_end <- pb$dep_time + total_time
	rmg_time <- exp_end - Sys.time()
	time <- if (pb$time_style == "cd") round(rmg_time, 2) else exp_end
	time_width <- nchar(as.character(time))
	bar_width <- ifelse(pb$bar_style == "simple",
		terminal_width - time_width - 6,
		terminal_width - time_width - 10)
	bar_width <- max(bar_width, 0L)
	bar_nb <- floor(progress * bar_width)
	tip <- ifelse(bar_nb > 0 & bar_nb < bar_width, ">", "")
	done <- paste(rep("=", bar_nb), collapse = "")
	rest <- paste(rep(" ", bar_width - bar_nb), collapse = "")
	bar <- if (pb$bar_style == "simple") {
		paste("|", done, tip, rest, "| ", sep = "")
	} else {
		paste("|", done, tip, rest, "| ", floor(100 * progress), "% | ", sep = "")
	}
	cat(paste("\r", paste(rep(" ", terminal_width), collapse = ""), sep = ""))
	cat(paste("\r", bar, time, sep = ""))
	if (progress >= 1) cat("\n")
}


# Prepared statements in MySQL/MariaDB accept at most 65535 placeholders.
.UPSERT_MAX_PLACEHOLDERS <- 65535L

# A data.frame to a flat list of scalars in ROW-MAJOR order, which is the order a multi-row
# "VALUES (?,?),(?,?)" statement binds its placeholders.
#
# Element types are preserved per value (as.list on each column), so integers stay integers and
# NA stays a typed NA that binds as NULL -- an as.matrix() flattening would coerce a mixed-type
# frame to character and silently change what gets written.
flatten_rowwise <- function(df) {
  nr <- nrow(df); nc <- ncol(df)
  if (nr == 0L || nc == 0L) return(list())
  # as.list() on an ATOMIC column yields length-1 scalars, but on a LIST column (how a BLOB /
  # raw column arrives -- see normalize_table_utf8, which explicitly handles is.list/is.raw)
  # it returns the list unchanged. unlist(recursive=FALSE) would then strip the wrapper and
  # hand each placeholder a bare raw vector of length nrow instead of one value, which DBI
  # rejects with "Parameter N does not have length 1". col[i] keeps the single-element list
  # DBI expects for a blob.
  vals <- unlist(
    lapply(df, function(col) {
      if (is.data.frame(col)) {
        # A nested data.frame column: seq_along() would walk its COLUMNS, silently yielding the
        # wrong number of values. Refuse rather than mis-bind.
        stop("flatten_rowwise: nested data.frame columns are not supported")
      } else if (is.matrix(col)) {
        stop("flatten_rowwise: matrix columns are not supported")
      } else if (is.list(col)) {
        lapply(seq_along(col), function(i) col[i])
      } else {
        as.list(col)
      }
    }),
    recursive = FALSE, use.names = FALSE)
  # Checked BEFORE the reindex below, which is the only place it can be caught: that step
  # forces the result to exactly nr*nc, truncating extras and padding shortfalls with NULL, so
  # a length assertion afterwards always passes and a mis-bound column writes silently.
  if (length(vals) != nr * nc) {
    stop(sprintf("flatten_rowwise: expected %d values from %d rows x %d columns, got %d -- a column is not a plain length-nrow vector",
                 nr * nc, nr, nc, length(vals)))
  }
  # vals is column-major: value [r, c] sits at (c - 1) * nr + r. Reading the transpose of the
  # index matrix column-major yields exactly the row-major sequence.
  vals[as.vector(t(matrix(seq_len(nr * nc), nrow = nr)))]
}


# Fraction of max_allowed_packet a single batch may occupy. Headroom for the protocol framing
# and statement text that ride along with the values.
.UPSERT_PACKET_BUDGET <- 0.6

# Exact bytes per row, one element per row of df.
#
# Exact rather than sampled, and per row rather than averaged, because a batch is made of
# ADJACENT rows. A mean cannot bound that: a frame whose wide rows cluster (sorted by something
# correlated with payload size, or a partial backfill where recent rows are fatter) has an
# accurate mean and a catastrophic worst batch. Measured on 20000 rows whose first 1000 carried
# 200 KB each -- mean 10022 B/row, true mean 10010, and a batch sized from it carried 190 MB
# against a 16 MB limit.
#
# Character columns are measured in BYTES, not characters, so UTF-8 is not under-counted.
row_bytes <- function(df) {
  n <- nrow(df)
  if (n == 0L || ncol(df) == 0L) return(numeric(0))
  total <- numeric(n)
  for (col in df) {
    w <- if (is.character(col)) {
      b <- nchar(col, type = "bytes"); b[is.na(b)] <- 4L; as.numeric(b)
    } else if (is.list(col)) {
      as.numeric(lengths(col))
    } else {
      rep(8, n)
    }
    total <- total + w + 2          # per-value protocol framing
  }
  total
}

# Row-index vectors, one per statement. Each batch respects three ceilings: the caller's
# chunk_size, the 65535-placeholder cap, and -- walking actual row sizes rather than an average
# -- the server's max_allowed_packet. A single row too large to fit alone still gets its own
# batch and lets the server report the problem, rather than looping forever.
upsert_batches <- function(table, chunk_size, con, nolog = FALSE) {
  n <- nrow(table)
  by_placeholders <- .UPSERT_MAX_PLACEHOLDERS %/% max(1L, ncol(table))
  cap <- max(1L, as.integer(min(chunk_size, by_placeholders)))

  limit <- tryCatch(
    as.numeric(RMariaDB::dbGetQuery(con, "SELECT @@max_allowed_packet AS p")$p[1]),
    error = function(e) NA_real_)
  if (is.na(limit) || limit <= 0) {
    # Server would not say; fall back to the count-based ceilings alone.
    return(unname(split(seq_len(n), ceiling(seq_len(n) / cap))))   # unnamed, like the walk below
  }

  budget <- limit * .UPSERT_PACKET_BUDGET
  cs <- cumsum(row_bytes(table))
  out <- vector("list", 0L)
  start <- 1L
  while (start <= n) {
    base <- if (start == 1L) 0 else cs[start - 1L]
    # Last row whose cumulative size from `start` still fits the budget.
    j <- findInterval(base + budget, cs)
    end <- min(max(j, start), start + cap - 1L, n)
    out[[length(out) + 1L]] <- start:end
    start <- end + 1L
  }
  if (!nolog && length(out) > ceiling(n / cap)) {
    logging::logdebug("upsert_table: %d statements for %d rows (row sizes, not just counts, bound the batch).",
                      length(out), n, logger = LOGGER.MAIN)
  }
  out
}


# Does batching preserve the errors per-row execution would have raised?
#
# MariaDB downgrades error -> warning for an invalid value in the SECOND OR LATER row of a
# multi-row statement. That never applied to per-row execution, so wherever strictness does NOT
# cover the target, batching silently coerces a bad value and reports success. See Vongo/rmaria#7.
#
# STRICT_ALL_TABLES covers every engine. STRICT_TRANS_TABLES covers only TRANSACTIONAL ones --
# which is why a MyISAM table under the default sql_mode is exposed. Anything else, including an
# engine we could not identify, is treated as unsafe.
#
# On a server with no STRICT_* at all, per-row execution does not raise either: an over-long
# string or an out-of-range number is coerced with a warning whichever way it is sent (measured
# on MariaDB 11 with sql_mode=''). Exactly one class still differs, and the manual is explicit
# about it -- NULL into a NOT NULL column errors for a single-row INSERT and stores the implicit
# default for a multi-row one. So there, falling back is worth its 50x only when the data can
# actually bind a NULL; `may_bind_null` says whether it can.
#
# Pure so the decision is testable without a server: `transactional` is TRUE/FALSE/NA.
# `may_bind_null` is only forced on the non-strict branch, so passing an expression that scans
# the frame costs nothing on a strict server.
batching_preserves_errors <- function(sql_mode, transactional, may_bind_null = TRUE) {
  mode <- if (length(sql_mode) == 0L || is.na(sql_mode[1])) "" else toupper(as.character(sql_mode[1]))
  if (grepl("STRICT_ALL_TABLES", mode, fixed = TRUE)) return(TRUE)
  if (!grepl("STRICT_TRANS_TABLES", mode, fixed = TRUE)) return(isFALSE(may_bind_null))
  isTRUE(transactional)
}

# Can any value in this frame reach the server as NULL? An NA does; so does a list column (the
# shape blob/raw columns arrive in), whose elements can be NULL individually.
.frame_may_bind_null <- function(table) {
  any(vapply(table, function(col) is.list(col) || anyNA(col), logical(1)))
}

# Why the verdict came out as it did. Kept separate from the decision itself -- one place
# decides, this one explains -- so a log line can distinguish an expected fallback from one the
# operator needs to act on.
.batching_reason <- function(safe, sql_mode, transactional, table_name_in_base) {
  if (length(sql_mode) == 0L || is.na(sql_mode[1])) {
    return("the server did not answer @@session.sql_mode")
  }
  mode <- toupper(as.character(sql_mode[1]))
  if (grepl("STRICT_ALL_TABLES", mode, fixed = TRUE)) {
    return("STRICT_ALL_TABLES covers every engine")
  }
  if (!grepl("STRICT_TRANS_TABLES", mode, fixed = TRUE)) {
    return(if (safe) {
      "the server is not in strict mode, so invalid values are coerced whichever way they are sent; this data binds no NULL, which is the one class that would still differ"
    } else {
      "the server is not in strict mode: NULL into a NOT NULL column raises only when rows are sent one at a time (other invalid values are coerced either way -- set STRICT_TRANS_TABLES on the server if that matters)"
    })
  }
  if (is.na(transactional)) {
    return(sprintf("the engine behind '%s' could not be identified", table_name_in_base))
  }
  if (transactional) {
    "STRICT_TRANS_TABLES covers this table's transactional engine"
  } else {
    "STRICT_TRANS_TABLES does not reach this table's non-transactional engine"
  }
}

# One scalar out of a query result, or NA if the server answered with a shape we did not ask
# for. `df$col[1]` is not safe to test with is.na(): a missing column gives NULL, NULL[1] is
# NULL, and `if (is.na(NULL))` is a hard error rather than a verdict -- which would take down an
# upsert that had not yet issued a single statement.
.scalar_or_na <- function(df, col) {
  if (!is.data.frame(df)) return(NA)
  v <- df[[col]]
  if (is.null(v) || length(v) == 0L) NA else v[1]
}

# Asks the server the two questions batching_preserves_errors needs. Any failure to answer is
# reported as unsafe rather than assumed away.
#
# Returns the verdict AND the reason for it: "unsafe" and "could not tell" are different
# situations -- one is a MyISAM table behaving as documented, the other is an engine lookup that
# failed -- and a caller that only gets a logical can never tell an operator which happened.
upsert_batching_is_safe <- function(con, table_name_in_base, may_bind_null = TRUE) {
  mode <- tryCatch(.scalar_or_na(
    RMariaDB::dbGetQuery(con, "SELECT @@session.sql_mode AS m"), "m"),
    error = function(e) NA)
  decide <- function(transactional) {
    safe <- batching_preserves_errors(mode, transactional, may_bind_null)
    list(safe = safe,
         reason = .batching_reason(safe, mode, transactional, table_name_in_base))
  }
  if (is.na(mode)) return(decide(NA))
  # STRICT_ALL_TABLES settles it whatever the engine is, so skip the second round trip.
  if (grepl("STRICT_ALL_TABLES", toupper(mode), fixed = TRUE)) return(decide(NA))
  transactional <- tryCatch({
    eng <- RMariaDB::dbGetQuery(con, paste(
      "SELECT e.TRANSACTIONS AS t FROM information_schema.TABLES tb",
      "JOIN information_schema.ENGINES e ON e.ENGINE = tb.ENGINE",
      "WHERE tb.TABLE_SCHEMA = DATABASE() AND tb.TABLE_NAME = ?"),
      params = list(table_name_in_base))
    # A view has a NULL engine and so joins to nothing; a missing table returns no row. Both
    # mean "we do not know", which is unsafe.
    t <- .scalar_or_na(eng, "t")
    if (is.na(t)) NA else identical(toupper(t), "YES")
  }, error = function(e) NA)
  decide(transactional)
}
