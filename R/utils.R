# Internal utilities vendored from rutils so rmaria has no fragile cross-package
# build-time dependency. Not exported.

# "not in" operator.
`%ni%` <- function(a, b) !(a %in% b)

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
