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
