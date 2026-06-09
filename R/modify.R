#' Simplified upsert
#'
#' Simple method that upserts the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to upsert
#' @param table_name_in_base table in \code{db} to upsert data into
#' @param ... any other parameter that applies to upsert_table
#' @keywords mysql upsert insert update
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, upsert_table, insertq, insert_table
#' @export
#' @examples
#' \dontrun{upsertq(iris, "iris_database_name")}
upsertq <- function(table, table_name_in_base, ...) {
  creds <- resolve_credentials()
  upsert_table(table = table, table_name_in_base = table_name_in_base,
               host = creds$host, port = creds$port, db = creds$db, user = creds$user, password = creds$pwd, ...)
}


#' Upsert
#'
#' Simple method that inserts the input data.frame or data.table into the designated table, or updates it if the key already exists.
#' Uses parameterized batched INSERT ... ON DUPLICATE KEY UPDATE with COALESCE so that NULL values in the
#' incoming data do not overwrite existing non-NULL values in the database.
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @param table data.frame or data.table to upsert (rows are inserted, or update existing rows when the key already exists).
#' @param table_name_in_base table in \code{db} to upsert data into
#' @param keycols character vector naming the key column(s) used to identify rows (excluded from the SET/UPDATE clause)
#' @param chunk_size how many rows to send per batched statement (default 10000)
#' @param progress_bar nice progress bar to use, it's recommended to disable it in log mode
#' @param nolog avoid any writing to the console (when TRUE, errors are not logged either)
#' @return (invisibly) MariaDB's affected-row count: 1 per new row inserted, 2 per existing row updated, 0 per existing row left unchanged.
#' @keywords mysql insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, insertq
#' @export
#' @examples
#' \dontrun{upsert_table(my_data, "table_name", keycols=c("id"), host=HOST, db=DB, user=USER, password=PWD)}
upsert_table <- function(table, table_name_in_base, keycols, host="localhost", port=3306, db, user, password,
	chunk_size=NA, progress_bar=interactive(), nolog=FALSE
) {
  table <- as.data.frame(table)
  if (nrow(table) == 0L) {
    if (!nolog) logging::logwarn("You tried to upsert an empty table. Leaving.", logger=LOGGER.MAIN)
    return(invisible(0L))
  }
  if (missing(keycols) || length(keycols) == 0L) stop("upsert_table: 'keycols' must name the key column(s)")
  unknown <- setdiff(keycols, colnames(table))
  if (length(unknown) > 0L) stop("upsert_table: keycols not found in table: ", paste(unknown, collapse = ", "))
  if (!nolog) logging::loginfo("Upserting %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)
  table <- normalize_table_utf8(table, nolog=nolog)
  table[] <- lapply(table, function(col) { if (is.factor(col)) col <- as.character(col); if (is.numeric(col)) col[!is.finite(col)] <- NA; col })
  cols <- colnames(table)
  sql  <- build_upsert_sql(table_name_in_base, cols, keycols)
  if (is.na(chunk_size)) chunk_size <- 10000L
  chunk_size <- as.integer(max(1L, min(chunk_size, nrow(table))))
  n_iter <- as.integer(ceiling(nrow(table) / chunk_size))
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  pb <- if (progress_bar) create_pb(n_iter, bar_style="pc", time_style="cd") else NULL
  affected <- 0L
  tryCatch(
    DBI::dbWithTransaction(con, {
      for (i in seq_len(n_iter)) {
        rows <- ((i - 1L) * chunk_size + 1L):min(i * chunk_size, nrow(table))
        affected <- affected + RMariaDB::dbExecute(con, sql, params = unname(as.list(table[rows, , drop = FALSE])))
        if (progress_bar) update_pb(pb, i)
      }
    }),
    error = function(e) {
      if (!nolog) logging::logerror("Error upserting into %s: %s", table_name_in_base, conditionMessage(e), logger = LOGGER.MAIN)
      stop(e)
    }
  )
  invisible(affected)
}

#' Simplified update
#'
#' Simple method that updates the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to update
#' @param table_name_in_base table in \code{db} to update data into
#' @param ... any other parameter that applies to update_table
#' @keywords mysql update insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, update_table, insertq, insert_table
#' @export
#' @examples
#' \dontrun{updateq(iris, "iris_database_name")}
updateq <- function(table, table_name_in_base, ...) {
  creds <- resolve_credentials()
  update_table(table = table, table_name_in_base = table_name_in_base,
               host = creds$host, port = creds$port, db = creds$db, user = creds$user, password = creds$pwd, ...)
}


#' Update
#'
#' Updates rows in the designated table by loading the input data into a TEMPORARY table and
#' executing a single \code{UPDATE ... JOIN ... SET col = COALESCE(src.col, tgt.col)}.
#' All work runs inside one transaction. NULL values in the incoming data are preserved as
#' "skip" (COALESCE keeps the existing DB value). NULL key values naturally do not match any
#' existing row so those rows are silently ignored.
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @param table data.frame or data.table whose rows update matching rows in the database
#' @param table_name_in_base table in \code{db} to update rows in
#' @param keycols character vector naming the key column(s) used to identify rows (excluded from the SET/UPDATE clause)
#' @param chunk_size how many rows to load per batch into the temp table (default 10000)
#' @param progress_bar nice progress bar to use, it's recommended to disable it in log mode
#' @param nolog avoid any writing to the console (when TRUE, errors are not logged either)
#' @return (invisibly) the number of rows changed.
#' @keywords mysql update
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @details Input rows must be unique on \code{keycols} across the entire input; duplicate keys resolve non-deterministically (all chunks load into one temporary table before the JOIN update).
#' @seealso pull_data, selectq, insertq
#' @export
#' @examples
#' \dontrun{update_table(my_data, "table_name", keycols=c("id"), host=HOST, db=DB, user=USER, password=PWD)}
update_table <- function(table, table_name_in_base, keycols, host="localhost", port=3306, db, user, password,
  chunk_size=NA, progress_bar=interactive(), nolog=FALSE
) {
  table <- as.data.frame(table)
  if (nrow(table) == 0L) {
    if (!nolog) logging::logwarn("You tried to update with empty data. Leaving.", logger=LOGGER.MAIN)
    return(invisible(0L))
  }
  if (missing(keycols) || length(keycols) == 0L) stop("update_table: 'keycols' must name the key column(s)")
  unknown <- setdiff(keycols, colnames(table))
  if (length(unknown) > 0L) stop("update_table: keycols not found in table: ", paste(unknown, collapse = ", "))
  cols <- colnames(table)
  if (length(setdiff(cols, keycols)) == 0L) {
    if (!nolog) logging::logwarn("update_table: no non-key columns to update. Leaving.", logger=LOGGER.MAIN)
    return(invisible(0L))
  }
  if (!nolog) logging::loginfo("Updating %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)
  table <- normalize_table_utf8(table, nolog=nolog)
  table[] <- lapply(table, function(col) { if (is.factor(col)) col <- as.character(col); if (is.numeric(col)) col[!is.finite(col)] <- NA; col })
  if (is.na(chunk_size)) chunk_size <- 10000L
  chunk_size <- as.integer(max(1L, min(chunk_size, nrow(table))))
  n_iter <- as.integer(ceiling(nrow(table) / chunk_size))
  tmp <- "rmaria_update_tmp"
  ins_sql <- build_insert_sql(tmp, cols, ignore = FALSE)
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  pb <- if (progress_bar) create_pb(n_iter + 1L, bar_style="pc", time_style="cd") else NULL
  affected <- tryCatch(
    DBI::dbWithTransaction(con, {
      RMariaDB::dbExecute(con, paste0("DROP TEMPORARY TABLE IF EXISTS ", quote_ident(tmp)))
      RMariaDB::dbExecute(con, paste0("CREATE TEMPORARY TABLE ", quote_ident(tmp), " AS SELECT ",
        paste(quote_ident(cols), collapse = ","), " FROM ", quote_ident(table_name_in_base), " WHERE 1=0"))
      # The CTAS above inherits the target's NOT NULL constraints. Drop them so NA values
      # (incl. NULL keys) can be loaded: NA non-key values are COALESCE-skipped, NULL keys
      # simply don't match in the JOIN. Column TYPES are preserved exactly.
      ci <- RMariaDB::dbGetQuery(con, paste0("SHOW COLUMNS FROM ", quote_ident(tmp)))
      nn <- ci[ci$Null == "NO", , drop = FALSE]
      if (nrow(nn) > 0L) {
        mods <- paste(sprintf("MODIFY %s %s NULL", quote_ident(nn$Field), nn$Type), collapse = ", ")
        RMariaDB::dbExecute(con, paste0("ALTER TABLE ", quote_ident(tmp), " ", mods))
      }
      RMariaDB::dbExecute(con, paste0("ALTER TABLE ", quote_ident(tmp), " ADD INDEX (",
        paste(quote_ident(keycols), collapse = ","), ")"))
      for (i in seq_len(n_iter)) {
        rows <- ((i - 1L) * chunk_size + 1L):min(i * chunk_size, nrow(table))
        RMariaDB::dbExecute(con, ins_sql, params = unname(as.list(table[rows, , drop = FALSE])))
        if (progress_bar) update_pb(pb, i)
      }
      a <- RMariaDB::dbExecute(con, build_update_join_sql(table_name_in_base, tmp, cols, keycols))
      RMariaDB::dbExecute(con, paste0("DROP TEMPORARY TABLE ", quote_ident(tmp)))
      if (progress_bar) update_pb(pb, n_iter + 1L)
      a
    }),
    error = function(e) {
      if (!nolog) logging::logerror("Error updating %s: %s", table_name_in_base, conditionMessage(e), logger = LOGGER.MAIN)
      stop(e)
    }
  )
  invisible(affected)
}
