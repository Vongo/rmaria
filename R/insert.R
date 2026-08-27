#' Simplified bulk insert
#'
#' Simple method that inserts the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in \code{db} to insert data into
#' @param preface_queries character vector of queries you want to apply before, typically setting session variables.
#' @param split_threshold integer, number of rows to split the data into smaller groups. Default is 1e5.
#' @param use_file logical; if TRUE, enables the load_data_local_infile flag on the connection (needed for some server configs). Default FALSE.
#' @keywords MariaDB insert
#' @return (invisibly) the number of rows written, which on success equals \code{nrow(table)}.
#'   This differs from \code{insert_table}, which returns the affected count and can report fewer
#'   rows than supplied when \code{ignore=TRUE} skips duplicates. \code{insert_table_local} has no
#'   IGNORE path -- a duplicate key raises rather than being skipped -- so a successful call wrote
#'   every row it was given.
#' @details It's important that the input table and the database table share the same schema (matching names and types). \code{insertq} uses parameterized, chunked, transactional INSERTs; \code{insert_table_local} uses \code{dbWriteTable} with optional load_data_local_infile support (bulk load, no transactional batching or duplicate-key control).
#'
#'   Errors are logged and then rethrown, as in \code{insert_table}. Note there is no
#'   \code{nolog} counterpart here, so the error log cannot be suppressed.
#'
#'   Because there is no transaction, \strong{a failed call may have written a prefix of the
#'   data}: the chunked path commits each batch as it goes, so a failure on a later chunk leaves
#'   the earlier ones in the table. The logged error reports how many rows landed, but that count
#'   is a \strong{lower bound}, not an exact figure: it only counts rows from batches that
#'   completed, and \code{written} is assigned after \code{dbWriteTable} returns, so a batch that
#'   fails part-way through may itself have committed rows on an engine that cannot roll back
#'   (e.g. MyISAM) without those rows being counted. \code{R/modify.R}'s \code{.upsert_row_by_row}
#'   documents the same non-rollback hazard for the upsert path, and counts exactly by writing one
#'   row at a time -- a much larger change than this function makes. A caller deciding whether a
#'   re-run is safe must therefore tolerate rows already present rather than assume the reported
#'   count is exact.
#' @section Warning: \code{use_file=TRUE} inserts via \code{LOAD DATA LOCAL INFILE}, which does
#'   \strong{not} honour \code{STRICT_TRANS_TABLES}. Verified on InnoDB under
#'   \code{STRICT_TRANS_TABLES}: a value too long for its column is silently truncated, and an
#'   invalid value is silently coerced, with \emph{no error or warning reaching R} -- the call
#'   reports success and the reported row count is the full count, exactly as if every value had
#'   been valid. The log-and-rethrow contract documented above does \strong{not} protect this
#'   path: there is nothing to rethrow, because MariaDB itself does not report it as an error.
#'   This is not fixed by this function's error handling; detecting it would need inspecting
#'   \code{SHOW WARNINGS} after every load, which is unimplemented.
#' @seealso pull_data, selectq, insert_table, insertq
#' @export
#' @examples
#' \dontrun{
#'   insert_table_local(iris, "iris")
#'   n <- insert_table_local(iris, "iris", preface_queries="SET session rocksdb_bulk_load=1")
#' }
insert_table_local <- function(table, table_name_in_base, preface_queries=character(0), split_threshold=1e5, use_file=FALSE) {
  creds <- resolve_credentials()
  table <- as.data.frame(table)
  table <- normalize_table_utf8(table)
  con <- NULL
  # Counts rows actually written. It serves two purposes -- the return value on success, and the
  # "how much landed" figure the error path reports -- so the two cannot disagree.
  written <- 0L
  tryCatch({
    con <- .maria_connect(creds$host, creds$port, creds$db, creds$user, creds$pwd, local_infile = use_file)
    if (length(preface_queries) > 0) {
      for (pq in preface_queries) {
        # A preface query (e.g. "SET session rocksdb_bulk_load=1") shares the outer handler with
        # the INSERT itself, so a malformed one used to log as though the INSERT had failed --
        # "Error while inserting data into table t (0 of 3 rows written): <SQL syntax error>" --
        # with nothing to tell a 2am reader it was the preface, not the insert. Name it here and
        # let it propagate; the outer handler still logs and rethrows it.
        tryCatch(
          RMariaDB::dbExecute(con, pq),
          error = function(e) {
            stop(sprintf("preface query failed (%s): %s", pq, conditionMessage(e)), call. = FALSE)
          }
        )
      }
    }
    if (nrow(table) >= split_threshold) {
      start <- 1
      while (start <= nrow(table)) {
        end <- min(nrow(table), start + split_threshold - 1)
        RMariaDB::dbWriteTable(con, table_name_in_base, table[seq(start, end), , drop = FALSE], append = TRUE)
        written <- written + as.integer(end - start + 1)
        start <- end + 1
      }
    } else {
      RMariaDB::dbWriteTable(con, table_name_in_base, table, append = TRUE)
      written <- as.integer(nrow(table))
    }
  }, error = function(e) {
    # Log AND rethrow, matching insert_table(). Swallowing made a failed INSERT
    # indistinguishable from a successful one twice over: no condition reached the caller, and
    # because this tryCatch was the function's last expression, the returned value was
    # logerror()'s -- which is TRUE.
    #
    # `written` is reported because this function is NOT transactional -- insert_table_local does
    # not use dbWithTransaction -- so a failure part-way through the chunked path leaves the
    # earlier chunks committed. But it is a LOWER BOUND, not an exact count: it is only
    # incremented after dbWriteTable() returns, so a batch that fails part-way through may itself
    # have committed rows (on a non-rollback engine such as MyISAM) that are never added to it.
    # A caller deciding whether a re-run is safe must tolerate rows already present rather than
    # trust this number as exact.
    logging::logerror("Error while inserting data into table %s (%s of %s rows written): %s",
                      table_name_in_base, written, nrow(table), conditionMessage(e), logger = LOGGER.MAIN)
    stop(e)
  }, finally = {
    # try(), not a bare call: if dbDisconnect() itself throws (e.g. a bulk-load flush failing at
    # disconnect -- the roxygen's own documented preface_queries="SET session rocksdb_bulk_load=1"
    # flushes here), a finally-block error REPLACES the condition being propagated by error=/stop(e)
    # above. The caller would see "disconnect failed" instead of the insert error that actually
    # matters, after `written` was already counted and logged correctly. Same precedent as
    # .upsert_row_by_row's on.exit(try(dbClearResult(...), silent = TRUE)) in R/modify.R.
    if (!is.null(con)) try(RMariaDB::dbDisconnect(con), silent = TRUE)
  })
  invisible(written)
}

insert_source_full_file <- function(src, host="localhost", port=3306, db, user, password) {
	con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), user=user, password=password, dbname=db, host=host, port=port)
	on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
	# Retrieving the path where MYSQL can read from (if any)
	# Only problem is that you should have the right to write there
	path <- paste0(pull_data(host, port, db, user, password, "SHOW VARIABLES LIKE 'secure_file_priv';")["Value"], "tmp.csv")
	RMariaDB::dbExecute(con, 'set character set "utf8"')
	utils::write.table(src, path, row.names=FALSE, col.names=FALSE, sep='\t')
	query = paste0("LOAD DATA INFILE '", path, "' INTO TABLE uplift_source")
	RMariaDB::dbExecute(con, query)
	file.remove(path)
	RMariaDB::dbDisconnect(con)
}

#' Simplified insert
#'
#' Simple method that inserts the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in \code{db} to insert data into
#' @param ... any other parameter that applies to insert_table
#' @keywords mysql insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, insert_table
#' @export
#' @examples
#' \dontrun{insertq(iris, "iris_name_in_database")}
insertq <- function(table, table_name_in_base, ...) {
  creds <- resolve_credentials()
  insert_table(table = table, table_name_in_base = table_name_in_base,
               host = creds$host, port = creds$port, db = creds$db, user = creds$user, password = creds$pwd, ...)
}

#' Insert
#'
#' Simple method that inserts the input data.frame or data.table into the designated table.
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in \code{db} to insert data into
#' @param chunk_size how many elements should be inserted at a time
#' @param progress_bar nice progress bar to use, it's recommended to disable it in log mode
#' @param ignore if TRUE, uses INSERT IGNORE -- silently skips rows that violate duplicate-key/constraint rules; other errors (connection, missing table) still propagate.
#' @param nolog avoid any writing to the console (when TRUE, errors are not logged either)
#' @param allow.backslash deprecated and ignored; backslashes are now escaped correctly by DBI
#' @return (invisibly) the number of rows affected (with ignore=TRUE, skipped duplicate rows are not counted).
#' @keywords mysql insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, insertq
#' @export
#' @examples
#' \dontrun{data <- insert_table(iris, "iris_name_in_database", host=HOST, db=DB, user=user, password=pwd)}
insert_table <- function(table, table_name_in_base, host="localhost", port=3306, db, user, password, chunk_size=NA, progress_bar=interactive(), ignore=TRUE, nolog=FALSE, allow.backslash=FALSE) {
  table <- as.data.frame(table)
  if (nrow(table) == 0L) {
    if (!nolog) logging::logwarn("You tried to insert an empty table. Leaving.", logger=LOGGER.MAIN)
    return(invisible(0L))
  }
  if (!nolog) logging::loginfo("Inserting data into table %s.", table_name_in_base, logger=LOGGER.MAIN)
  table <- normalize_table_utf8(table, nolog=nolog)
  table[] <- lapply(table, function(col) {
    if (is.factor(col)) col <- as.character(col)
    if (is.numeric(col)) col[!is.finite(col)] <- NA   # NA/NaN/Inf -> NULL
    col
  })
  cols <- colnames(table)
  sql  <- build_insert_sql(table_name_in_base, cols, ignore)
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
      if (!nolog) logging::logerror("Error inserting into %s: %s", table_name_in_base, conditionMessage(e), logger = LOGGER.MAIN)
      stop(e)
    }
  )
  invisible(affected)
}
