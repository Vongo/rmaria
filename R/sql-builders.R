# Pure SQL builders. No DB connection required (identifier quoting is done here),
# so these are fully unit-testable. Values are always `?` placeholders.

# MySQL/MariaDB identifier quoting: wrap in backticks, double internal backticks.
quote_ident <- function(x) paste0("`", gsub("`", "``", x, fixed = TRUE), "`")

# Single-row INSERT [IGNORE] with one placeholder per column.
build_insert_sql <- function(table, cols, ignore = TRUE) {
  if (length(cols) == 0L) stop("build_insert_sql: 'cols' must be non-empty")
  paste0("INSERT ", if (ignore) "IGNORE " else "", "INTO ", quote_ident(table),
         " (", paste(quote_ident(cols), collapse = ","), ") VALUES (",
         paste(rep("?", length(cols)), collapse = ","), ")")
}

# Single-row INSERT ... ON DUPLICATE KEY UPDATE col = COALESCE(VALUES(col), col)
# for every non-key column (COALESCE preserves "don't overwrite with NULL").
# Keys-only tables have nothing to update -> INSERT IGNORE.
# n_rows emits that many placeholder tuples in one statement, so a batch of rows can be sent as
# a single round trip instead of one per row. Defaults to 1, which reproduces the previous
# output byte for byte.
build_upsert_sql <- function(table, cols, keycols, n_rows = 1L) {
  if (length(cols) == 0L) stop("build_upsert_sql: 'cols' must be non-empty")
  if (length(keycols) == 0L) stop("build_upsert_sql: 'keycols' must be non-empty")
  n_rows <- suppressWarnings(as.integer(n_rows))
  if (length(n_rows) != 1L || is.na(n_rows) || n_rows < 1L) {
    stop("build_upsert_sql: 'n_rows' must be a single positive integer")
  }
  non_key <- setdiff(cols, keycols)
  tuple <- paste0("(", paste(rep("?", length(cols)), collapse = ","), ")")
  base <- paste0("INSERT ", if (length(non_key) == 0L) "IGNORE " else "", "INTO ",
                 quote_ident(table), " (", paste(quote_ident(cols), collapse = ","),
                 ") VALUES ", paste(rep(tuple, n_rows), collapse = ","))
  if (length(non_key) == 0L) return(base)
  set_clause <- paste(vapply(non_key, function(nm) {
    qc <- quote_ident(nm); paste0(qc, "=COALESCE(VALUES(", qc, "),", qc, ")")
  }, character(1)), collapse = ",")
  paste0(base, " ON DUPLICATE KEY UPDATE ", set_clause)
}

# UPDATE target t JOIN tmp s ON keys SET t.col = COALESCE(s.col, t.col) for non-keys.
build_update_join_sql <- function(table, tmp, cols, keycols) {
  if (length(keycols) == 0L) stop("build_update_join_sql: 'keycols' must be non-empty")
  non_key <- setdiff(cols, keycols)
  if (length(non_key) == 0L) stop("build_update_join_sql: no non-key columns to update")
  on_clause <- paste(vapply(keycols, function(nm) {
    qk <- quote_ident(nm); paste0("t.", qk, "=s.", qk)
  }, character(1)), collapse = " AND ")
  set_clause <- paste(vapply(non_key, function(nm) {
    qc <- quote_ident(nm); paste0("t.", qc, "=COALESCE(s.", qc, ",t.", qc, ")")
  }, character(1)), collapse = ",")
  paste0("UPDATE ", quote_ident(table), " t JOIN ", quote_ident(tmp),
         " s ON ", on_clause, " SET ", set_clause)
}
