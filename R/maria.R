LOGGER.MAIN <- "com.vongo.rmaria"

# Internal: open one MariaDB connection with utf8mb4. Caller owns disconnect.
.maria_connect <- function(host = "localhost", port = 3306, db, user, password,
                           local_infile = FALSE) {
  con <- RMariaDB::dbConnect(
    RMariaDB::MariaDB(),
    dbname = db, host = host, port = port, user = user, password = password,
    load_data_local_infile = local_infile
  )
  tryCatch(
    RMariaDB::dbExecute(con, "SET NAMES utf8mb4"),
    error = function(e) {
      RMariaDB::dbDisconnect(con)
      stop(sprintf(".maria_connect: 'SET NAMES utf8mb4' failed (host=%s, db=%s): %s",
                   host, db, conditionMessage(e)), call. = FALSE)
    }
  )
  con
}

#' Select
#'
#' Simple wrapper around `pull_data` method, that makes credential use transparent.
#' Requires credentials to be loaded, obviously.
#' @param query query to execute
#' @param ... any argument that can be sent to `pull_data`
#' @keywords mysql select
#' @seealso execq pull_data
#' @export
#' @examples
#' \dontrun{
#' selectq("select * from table limit 10;")}
selectq <- function(query, ...) {
  creds <- resolve_credentials()
  pull_data(host = creds$host, port = creds$port, db = creds$db, user = creds$user, password = creds$pwd, query = query, ...)
}


#' Detailed select
#'
#' Simple method that executes your select query and returns its results in a data.table.
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @param query query to execute
#' @param verbose output current state and warnings
#' @param keep_int64 if TRUE, keeps int64 columns as-is; if FALSE (default), converts to numeric
#' @param retries total number of query attempts including the first; default 1 means no retry
#' @param retry_delay delay in seconds between retry attempts (default: 1)
#' @import magrittr
#' @keywords mysql select
#' @seealso insert_table
#' @export
#' @examples
#' \dontrun{data <- pull_data(host=HOST, db=DB, user=user, password=pwd, query="select * from table;")}
pull_data <- function(host="localhost", port=3306, db, user, password, query, verbose=TRUE, keep_int64=FALSE, retries=1, retry_delay=1) {
	# Input validation
	if (missing(query) || is.null(query) || !is.character(query) || nchar(trimws(query)) == 0) {
		stop("pull_data: 'query' must be a non-empty character string")
	}
	if (missing(db) || is.null(db) || !is.character(db) || nchar(trimws(db)) == 0) {
		stop("pull_data: 'db' must be a non-empty character string")
	}
	if (missing(user) || is.null(user) || !is.character(user)) {
		stop("pull_data: 'user' must be a character string")
	}
	if (missing(password) || is.null(password) || !is.character(password)) {
		stop("pull_data: 'password' must be a character string")
	}

	if (verbose) {
		logging::logfinest("Fetching data with query: \n\t%s.", query, logger=LOGGER.MAIN)
	}

	state <- new.env()
	state$data <- NULL
	state$last_error <- NULL
	attempt <- 0

	while (attempt < retries) {
		attempt <- attempt + 1
		con <- NULL

		result <- tryCatch({
			con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), user=user, password=password, dbname=db, host=host, port=port)
			RMariaDB::dbExecute(con, "SET NAMES utf8mb4")
			state$data <- RMariaDB::dbGetQuery(con, query)
			TRUE
		}, error=function(e) {
			state$last_error <- e
			if (verbose) {
				logging::logwarn("Attempt %d/%d failed for query [%s]: %s", attempt, retries, query, conditionMessage(e), logger=LOGGER.MAIN)
			}
			FALSE
		}, finally={
			if (!is.null(con)) {
				tryCatch(
					RMariaDB::dbDisconnect(con),
					error=function(e) {
						if (verbose) logging::logwarn("Failed to disconnect: %s", conditionMessage(e), logger=LOGGER.MAIN)
					}
				)
			}
		})

		if (isTRUE(result)) {
			break
		}

		if (attempt < retries) {
			Sys.sleep(retry_delay)
		}
	}

	if (is.null(state$data)) {
		error_msg <- if (!is.null(state$last_error)) conditionMessage(state$last_error) else "Unknown error"
		logging::logerror("Error while fetching data with query [%s] after %d attempts:\n[%s]", query, retries, error_msg, logger=LOGGER.MAIN)
		stop(sprintf("pull_data failed after %d attempts: %s", retries, error_msg))
	}

	if (verbose) {
		logging::logfinest("Properly retrieved %s observations.", nrow(state$data), logger=LOGGER.MAIN)
	}

	if (keep_int64==TRUE) {
		purrr::modify_if(state$data, is.list, unlist) |>
			purrr::modify_if(is.raw, as.logical) |>
			data.table::as.data.table()
	} else {
		purrr::modify_if(state$data, is.list, unlist) |>
			purrr::modify_if(is.raw, as.logical) |>
			purrr::modify_if(bit64::is.integer64, as.numeric) |>
			data.table::as.data.table()
	}
}

#' Exec
#'
#' Simple wrapper around `exec_query` method, that makes credential use transparent.
#' Requires credentials to be loaded, obviously.
#' @param query query to execute
#' @param ... any other argument passed to \code{exec_query}
#' @keywords mysql select
#' @seealso exec_query pull_data
#' @export
#' @examples
#' \dontrun{execq("TRUNCATE TABLE foo;")}
execq <- function(query, ...) {
  creds <- resolve_credentials()
  exec_query(host = creds$host, port = creds$port, db = creds$db, user = creds$user, password = creds$pwd, query = query, ...)
}


#' Execute query
#'
#' Simple method that executes your query and doesn't return anything.
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @param query query to execute
#' @keywords mysql delete create statement
#' @seealso insert_table
#' @export
#' @examples
#' \dontrun{exec_query(host=HOST, db=DB, user=USER, password=PWD, query="TRUNCATE TABLE foo;")}
exec_query <- function(host="localhost", port=3306, db, user, password, query) {
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  RMariaDB::dbExecute(con, query)
}

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

#' Truncate table
#'
#' Empties table from all observations, but doesn't delete it.
#' @param table_name_in_base table in \code{db} to insert data into
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @keywords mysql delete
#' @export
#' @examples
#' \dontrun{truncate_table(table="foo", host=HOST, db=DB, user=USER, password=PWD)}
truncate_table <- function(table_name_in_base, host="localhost", port=3306, db, user, password) {
  logging::loginfo("Truncating table %s.", table_name_in_base, logger=LOGGER.MAIN)
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  RMariaDB::dbExecute(con, paste0("TRUNCATE TABLE ", DBI::dbQuoteIdentifier(con, table_name_in_base)))
}

insert_source_full_file <- function(src, host="localhost", port=3306, db, user, password) {
	con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), user=user, password=password, dbname=db, host=host, port=port)
	# Retrieving the path where MYSQL can read from (if any)
	# Only problem is that you should have the right to write there
	path <- paste0(pull_data(host, port, db, user, password, "SHOW VARIABLES LIKE 'secure_file_priv';")["Value"], "tmp.csv")
	print(path)
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

#' Delete query
#'
#' Delete from table rows that match certain criteria
#' @param table_name_in_base table in \code{db} to delete rows from
#' @param where SQL WHERE clause (without the WHERE keyword) selecting rows to delete. Interpolated verbatim into the statement -- the caller is responsible for sanitizing any untrusted input (this fragment is NOT escaped).
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @keywords mysql delete
#' @export
#' @examples
#' \dontrun{delete_from_table(table_name_in_base="foo", where="id in (1, 2, 3)", host=HOST, db=DB, user=USER, password=PWD)}
delete_from_table <- function(table_name_in_base, where, host="localhost", port=3306, db, user, password) {
  if (missing(where) || !is.character(where) || length(where) != 1L || !nzchar(trimws(where))) {
    stop("delete_from_table: 'where' must be a non-empty SQL WHERE clause (use truncate_table to empty a table)")
  }
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  RMariaDB::dbExecute(con,
    paste0("DELETE FROM ", DBI::dbQuoteIdentifier(con, table_name_in_base),
           " WHERE ", where))
}

#' Simplified delete query
#'
#' Delete from table rows that match certain criteria
#' @param table_name_in_base table in \code{db} to delete rows from
#' @param where SQL WHERE clause (without the WHERE keyword) selecting rows to delete. Interpolated verbatim into the statement -- the caller is responsible for sanitizing any untrusted input (this fragment is NOT escaped).
#' @param ... any other parameter passed to \code{delete_from_table}
#' @keywords mysql delete
#' @export
#' @examples
#' \dontrun{deleteq(table_name_in_base="foo", where="id < 10")}
deleteq <- function(table_name_in_base, where, ...) {
  creds <- resolve_credentials()
  delete_from_table(table_name_in_base, where,
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
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in \code{db} to upsert data into
#' @param keycols character vector naming the key column(s) used to identify rows (excluded from the SET/UPDATE clause)
#' @param chunk_size how many rows to send per batched statement (default 10000)
#' @param progress_bar nice progress bar to use, it's recommended to disable it in log mode
#' @param nolog avoid any writing to the console (when TRUE, errors are not logged either)
#' @return (invisibly) MariaDB's affected-row count (inserts count 1, updates count 2 per row).
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
    if (!nolog) logging::logwarn("You tried to insert an empty table. Leaving.", logger=LOGGER.MAIN)
    return(invisible(0L))
  }
  if (missing(keycols) || length(keycols) == 0L) stop("upsert_table: 'keycols' must name the key column(s)")
  unknown <- setdiff(keycols, colnames(table))
  if (length(unknown) > 0L) stop("upsert_table: keycols not found in table: ", paste(unknown, collapse = ", "))
  if (!nolog) logging::loginfo("Upserting %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)
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
#' @details Input rows must be unique on \code{keycols}; duplicate keys within the batch resolve non-deterministically (this is a JOIN-based bulk update, not row-by-row).
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
