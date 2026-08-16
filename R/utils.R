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
      if (is.list(col)) lapply(seq_along(col), function(i) col[i]) else as.list(col)
    }),
    recursive = FALSE, use.names = FALSE)
  # vals is column-major: value [r, c] sits at (c - 1) * nr + r. Reading the transpose of the
  # index matrix column-major yields exactly the row-major sequence.
  vals[as.vector(t(matrix(seq_len(nr * nc), nrow = nr)))]
}
