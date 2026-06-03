#' Simplified bulk insert
#'
#' Simple method that inserts the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in \code{db} to insert data into
#' @param preface_queries character vector of queries you want to apply before, typically setting session variables.
#' @param split_threshold integer, number of rows to split the data into smaller groups. Default is 1e5.
#' @param use_file logical, if TRUE, uses `load_data_local_infile` flag. Default is FALSE. Careful if you use it, as it does a count(*) on table before and after to check what was integrated.
#' @keywords MariaDB insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types). The difference between insertq and insert_table_local is that \code{insertq} uses homemade INSERTS statements, and \code{insert_table_local} uses `load_data_local_infile` flag.
#' @seealso pull_data, selectq, insert_table, insertq
#' @export
#' @examples
#' \dontrun{
#'   data <- insert_table_local(iris, "iris")
#'   data <- insert_table_local(iris, "iris", preface_queries="SET session rocksdb_bulk_load=1")
#' }
insert_table_local <- function(table, table_name_in_base, preface_queries=character(0), split_threshold=1e5, use_file=FALSE) {
  creds <- resolve_credentials()
  table <- as.data.frame(table)
  con <- NULL
  tryCatch({
    con <- .maria_connect(creds$host, creds$port, creds$db, creds$user, creds$pwd, local_infile = use_file)
    if (length(preface_queries) > 0) {
      for (pq in preface_queries) RMariaDB::dbExecute(con, pq)
    }
    if (nrow(table) >= split_threshold) {
      start <- 1
      while (start <= nrow(table)) {
        end <- min(nrow(table), start + split_threshold - 1)
        RMariaDB::dbWriteTable(con, table_name_in_base, table[seq(start, end), , drop = FALSE], append = TRUE)
        start <- end + 1
      }
    } else {
      RMariaDB::dbWriteTable(con, table_name_in_base, table, append = TRUE)
    }
  }, error = function(e) {
    logging::logerror("Error while inserting data into table %s: %s", table_name_in_base, conditionMessage(e), logger = LOGGER.MAIN)
  }, finally = {
    if (!is.null(con)) RMariaDB::dbDisconnect(con)
  })
}

insert_source_full_file <- function(src, host="localhost", port=3306, db, user, password) {
	con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), user=user, password=password, dbname=db, host=host, port=port)
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
#' \dontrun{data <- insertq(host=HOST, db=DB, user=user, password=pwd, query="select * from table;")}
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
#' @param ignore should we ignore observations that produce errors?
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
