init <- function() {
	LOGGER.MAIN <<- "com.vongo.rmaria"
	library(rutils, quietly=TRUE, warn.conflicts=FALSE)
	library(magrittr, quietly=TRUE, warn.conflicts=FALSE)
	library(logging, quietly=TRUE, warn.conflicts=FALSE)
	library(RMariaDB, quietly=TRUE, warn.conflicts=FALSE)
	TRUE
}
init()

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
	target_e <- environment()
	source_environments <- list(
		environment(),
		parent.frame(),
		parent.env(environment()),
		parent.env(parent.env(environment())),
		parent.env(parent.frame(n=1)),
		parent.env(parent.frame(n=2)), # purrr::map
		parent.env(parent.frame(n=3)),
		parent.env(parent.frame(n=4)), # parallel::mclapply
		parent.env(parent.frame(n=5))
	)
	i_env <- 1
	source_e <- source_environments[[i_env]]
	while (i_env<length(source_environments) && !all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		i_env %<>% add(1)
		source_e <- source_environments[[i_env]]
	}
	if (all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		assign("DB", get("DB", envir=source_e), envir=target_e)
		assign("HOST", get("HOST", envir=source_e), envir=target_e)
		assign("PWD", get("PWD", envir=source_e), envir=target_e)
		assign("USER", get("USER", envir=source_e), envir=target_e)
	} else {
		init()
		logging::logerror("Context was not initialized properly. See `?load_env` for more information.", logger=LOGGER.MAIN)
		return(FALSE)
	}
	pull_data(host=HOST, db=DB, user=USER, password=PWD, query=query, ...)
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
#' @param retries number of retry attempts for transient failures (default: 3)
#' @param retry_delay delay in seconds between retry attempts (default: 1)
#' @import magrittr
#' @keywords mysql select
#' @seealso insert_table
#' @export
#' @examples
#' \dontrun{data <- pull_data(host=HOST, db=DB, user=user, password=pwd, query="select * from table;")}
pull_data <- function(host="localhost", port=3306, db, user, password, query, verbose=TRUE, keep_int64=FALSE, retries=1, retry_delay=1) {
	init()

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
			RMariaDB::dbExecute(con, 'set character set "utf8"')
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
#' @param ... any argument that can be sent to `pull_data`
#' @keywords mysql select
#' @seealso exec_query pull_data
#' @export
#' @examples
#' \dontrun{execq('set character set "utf8"')}
execq <- function(query, ...) {
	target_e <- environment()
	source_environments <- list(
		environment(),
		parent.frame(),
		parent.env(environment()),
		parent.env(parent.env(environment())),
		parent.env(parent.frame(n=1)),
		parent.env(parent.frame(n=2)), # purrr::map
		parent.env(parent.frame(n=3)),
		parent.env(parent.frame(n=4)), # parallel::mclapply
		parent.env(parent.frame(n=5))
	)
	i_env <- 1
	source_e <- source_environments[[i_env]]
	while (i_env<length(source_environments) && !all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		i_env %<>% add(1)
		source_e <- source_environments[[i_env]]
	}
	if (all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		assign("DB", get("DB", envir=source_e), envir=target_e)
		assign("HOST", get("HOST", envir=source_e), envir=target_e)
		assign("PWD", get("PWD", envir=source_e), envir=target_e)
		assign("USER", get("USER", envir=source_e), envir=target_e)
	} else {
		init()
		logging::logerror("Context was not initialized properly. See `?load_env` for more information.", logger=LOGGER.MAIN)
		return(FALSE)
	}
	exec_query(host=HOST, db=DB, user=USER, password=PWD, query=query, ...)
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
#' \dontrun{data <- pull_data(host=HOST, db=DB, user=user, password=pwd, query="select * from table;")}
exec_query <- function(host="localhost", port=3306, db, user, password, query) {
	con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), user=user, password=password, dbname=db, host=host, port=port)
	RMariaDB::dbExecute(con, 'set character set "utf8"')
	RMariaDB::dbExecute(con, query)
	RMariaDB::dbDisconnect(con)
}

#' Simplified bulk insert
#'
#' Simple method that inserts the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in {db} to insert data into
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
	target_e <- environment()
	source_environments <- list(
		environment(),
		parent.frame(),
		parent.env(environment()),
		parent.env(parent.env(environment())),
		parent.env(parent.frame(n=1)),
		parent.env(parent.frame(n=2)), # purrr::map
		parent.env(parent.frame(n=3)),
		parent.env(parent.frame(n=4)), # parallel::mclapply
		parent.env(parent.frame(n=5))
	)
	i_env <- 1
	source_e <- source_environments[[i_env]]
	while (i_env<length(source_environments) && !all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		i_env %<>% add(1)
		source_e <- source_environments[[i_env]]
	}
	if (all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		assign("DB", get("DB", envir=source_e), envir=target_e)
		assign("HOST", get("HOST", envir=source_e), envir=target_e)
		assign("PWD", get("PWD", envir=source_e), envir=target_e)
		assign("USER", get("USER", envir=source_e), envir=target_e)
	} else {
		init()
		logging::logerror("Context was not initialized properly. See `?load_env` for more information.", logger=LOGGER.MAIN)
		return(FALSE)
	}
	library(RMariaDB)
	con <- NULL
	tryCatch({
		con <- RMariaDB::dbConnect(
			RMariaDB::MariaDB(),
			host=HOST, db=DB, user=USER, password=PWD, port=3306,
			load_data_local_infile=use_file
		)
		if (length(preface_queries)>0) {
			for (preface_query in preface_queries) {
				RMariaDB::dbExecute(con, preface_query)
			}
		}
		RMariaDB::dbExecute(con, "set character set \"utf8mb4\"")
		# RMariaDB::dbExecute(con, "SET character_set_client = \"utf8mb4\";")
		# RMariaDB::dbExecute(con, "SET character_set_results = \"utf8mb4\";")
		# RMariaDB::dbExecute(con, "SET character_set_connection = \"utf8mb4\";")
		# print(RMariaDB::dbGetQuery(con, "SELECT @@character_set_client;"))
		if (nrow(table)>=split_threshold) {
			start <- 1
			while (start < nrow(table)) {
				end <- min(nrow(table), start+split_threshold-1)
				RMariaDB::dbWriteTable(con, table_name_in_base, table[seq(start, end), names(table), drop=FALSE], append=TRUE)
				start <- end + 1
			}
		} else {
			RMariaDB::dbWriteTable(con, table_name_in_base, table, append=TRUE)
		}
	}, error=function(e) {
		logging::logerror("Error while inserting data into table %s: %s", table_name_in_base, e)
	}, finally={
		if (!is.null(con)) RMariaDB::dbDisconnect(con)
	})
}

#' Truncate table
#'
#' Empties table from all observations, but doesn't delete it.
#' @param table_name_in_base table in {db} to insert data into
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
	init()
	logging::loginfo("Truncating table %s.", table_name_in_base, logger=LOGGER.MAIN)
	query <- paste0("TRUNCATE TABLE `", table_name_in_base, "`;")
	con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), user=user, password=password, dbname=db, host=host, port=port)
	RMariaDB::dbExecute(con, query)
	RMariaDB::dbDisconnect(con)
}

insert_source_full_file <- function(src, host="localhost", port=3306, db, user, password) {
	con <- dbConnect(MariaDB(), user=user, password=password, dbname=db, host=host, port=port)
	# Retrieving the path where MYSQL can read from (if any)
	# Only problem is that you should have the right to write there
	path <- paste0(pull_data(host, port, db, user, password, "SHOW VARIABLES LIKE 'secure_file_priv';")["Value"], "tmp.csv")
	print(path)
	dbExecute(con, 'set character set "utf8"')
	write.table(src, path, row.names=FALSE, col.names=FALSE, sep='\t')
	query = paste0("LOAD DATA INFILE '", path, "' INTO TABLE uplift_source")
	dbExecute(con, query)
	file.remove(path)
	RMariaDB::dbDisconnect(con)
}

#' Simplified insert
#'
#' Simple method that inserts the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in {db} to insert data into
#' @param ... any other parameter that applies to insert_table
#' @keywords mysql insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, insert_table
#' @export
#' @examples
#' \dontrun{data <- insertq(host=HOST, db=DB, user=user, password=pwd, query="select * from table;")}
insertq <- function(table, table_name_in_base, ...) {
	target_e <- environment()
	source_environments <- list(
		environment(),
		parent.frame(),
		parent.env(environment()),
		parent.env(parent.env(environment())),
		parent.env(parent.frame(n=1)),
		parent.env(parent.frame(n=2)), # purrr::map
		parent.env(parent.frame(n=3)),
		parent.env(parent.frame(n=4)), # parallel::mclapply
		parent.env(parent.frame(n=5))
	)
	i_env <- 1
	source_e <- source_environments[[i_env]]
	while (i_env<length(source_environments) && !all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		i_env %<>% add(1)
		source_e <- source_environments[[i_env]]
	}
	if (all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		assign("DB", get("DB", envir=source_e), envir=target_e)
		assign("HOST", get("HOST", envir=source_e), envir=target_e)
		assign("PWD", get("PWD", envir=source_e), envir=target_e)
		assign("USER", get("USER", envir=source_e), envir=target_e)
	} else {
		init()
		logging::logerror("Context was not initialized properly. See `?load_env` for more information.", logger=LOGGER.MAIN)
		return(FALSE)
	}
	insert_table(table=table, table_name_in_base=table_name_in_base, host=HOST, db=DB, user=USER, password=PWD, ...)
}

#' Delete query
#'
#' Delete from table rows that match certain criteria
#' @param table_name_in_base table in \code{db} to insert data into
#' @param where SQL where clause (wtihout the keyword where) specifying which rows should be deleted
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
	con <- RMariaDB::dbConnect(RMariaDB::MariaDB(), user=user, password=password, dbname=db, host=host, port=port)
	RMariaDB::dbExecute(con, 'set character set "utf8"')
	suppressWarnings(RMariaDB::dbExecute(con, paste0("DELETE FROM ", table_name_in_base, " WHERE ", where)))
	RMariaDB::dbDisconnect(con)
}

#' Simplified delete query
#'
#' Delete from table rows that match certain criteria
#' @param table_name_in_base table in \code{db} to insert data into
#' @param where SQL where clause (wtihout the keyword where) specifying which rows should be deleted
#' @keywords mysql delete
#' @export
#' @examples
#' \dontrun{deleteq(table_name_in_base="foo", where="id < 10")}
deleteq <- function(table_name_in_base, where, ...) {
	target_e <- environment()
	source_environments <- list(
		environment(),
		parent.frame(),
		parent.env(environment()),
		parent.env(parent.env(environment())),
		parent.env(parent.frame(n=1)),
		parent.env(parent.frame(n=2)), # purrr::map
		parent.env(parent.frame(n=3)),
		parent.env(parent.frame(n=4)), # parallel::mclapply
		parent.env(parent.frame(n=5))
	)
	i_env <- 1
	source_e <- source_environments[[i_env]]
	while (i_env<length(source_environments) && !all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		i_env %<>% add(1)
		source_e <- source_environments[[i_env]]
	}
	if (all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		assign("DB", get("DB", envir=source_e), envir=target_e)
		assign("HOST", get("HOST", envir=source_e), envir=target_e)
		assign("PWD", get("PWD", envir=source_e), envir=target_e)
		assign("USER", get("USER", envir=source_e), envir=target_e)
	} else {
		init()
		logging::logerror("Context was not initialized properly. See `?load_env` for more information.", logger=LOGGER.MAIN)
		return(FALSE)
	}
	delete_from_table(table_name_in_base, where, host=HOST, db=DB, user=USER, password=PWD, ...)
}


# Escape single quotes
esq <- function(str) {
	gsub("'", "\\\\'", str)
}

# Escape double quotes
edq <- function(str) {
	gsub("\"", "\\\\'", str)
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
#' @param table_name_in_base table in {db} to insert data into
#' @param chunk_size how many elements should be inserted at a time
#' @param progress_bar nice progress bar to use, it's recommended to disable it in log mode
#' @param ignore should we ignore observations that produce errors?
#' @param nolog avoid any writing to the console
#' @keywords mysql insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, insertq
#' @export
#' @examples
#' \dontrun{data <- insert_table(iris, "iris_name_in_database", host=HOST, db=DB, user=user, password=pwd)}
insert_table <- function(table, table_name_in_base, host="localhost", port=3306, db, user, password, chunk_size=NA, progress_bar=TRUE, ignore=TRUE, nolog=FALSE, allow.backslash=FALSE) {
	init()
	if (nrow(table) == 0) {
		if (!nolog) logging::logwarn("You tried to insert an empty table. Leaving.", logger=LOGGER.MAIN)
		return()
	}
	if (!nolog) logging::loginfo("Inserting data into table %s.", table_name_in_base, logger=LOGGER.MAIN)
	if (is.na(chunk_size)) {
		chunk_size <- max(min(10000, nrow(table)/25), 100)
	}
	chunk_size <- min(chunk_size, nrow(table))
	n_iter <- ceiling(nrow(table)/chunk_size)
	has_quotes <- sapply(seq(ncol(table)), function(ic) !(is.numeric(table[,ic]) || is.logical(table[,ic])))
	pb <- if(progress_bar) create_pb(n_iter, bar_style="pc", time_style="cd") else NULL
	for (i in seq(n_iter)) {
		query <- paste0("INSERT ", `if`(ignore, "IGNORE ", ""), "INTO ", table_name_in_base, "(",
			paste0(colnames(table), collapse=","), ") VALUES ")
		vals <- character(0)
		for (j in seq(chunk_size)) {
			k <- (i-1)*chunk_size+j
			if (k <= nrow(table)) {
				vals[j] <- paste0("(",
					paste0(
						sapply(seq(ncol(table)), function(ic) {
							if ((table[k, ic] %>% {is.na(.) || is.nan(.) || (is.numeric(.) && !is.finite(.))})) {
								"Qù@ñÐĲ€T@IS©H€ZMŒZI//@"
							} else {
								if(has_quotes[ic]) {
									table[k, ic] |>
										gsub("'", '\'', x=_) |>
										gsub('"', '\'', x=_) %>%
										{`if`(allow.backslash, gsub("\\\\0", "/0", .), gsub("\\\\", "/", .))} %>%
										{paste0('"', ., '"')}
								} else {
									table[k, ic]
								}
							}
						}), collapse=","
					), ")"
				)
			}
		}
		query <- gsub("Qù@ñÐĲ€T@IS©H€ZMŒZI//@", "NULL", paste0(query, paste0(vals, collapse=',')))
		tryCatch({
				exec_query(host=host, port=port, db=db, user=user, password=password, query=query)
			},
			warn=function(w) {
				if (!nolog) logging::logwarn("Warning while inserting query [%s]: [%s]", query, w, logger=LOGGER.MAIN)
			},
			error=function(e) {
				if (!nolog) logging::logerror("Error while inserting query [%s]: [%s]", query, e, logger=LOGGER.MAIN)
			}
		)
		if (progress_bar) update_pb(pb, i)
	}
}


#' Simplified upsert
#'
#' Simple method that upserts the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to upsert
#' @param table_name_in_base table in {db} to upsert data into
#' @param ... any other parameter that applies to upsert_table
#' @keywords mysql upsert insert update
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, upsert_table, insertq, insert_table
#' @export
#' @examples
#' \dontrun{upsertq(iris, "iris_database_name")}
upsertq <- function(table, table_name_in_base, ...) {
	target_e <- environment()
	source_environments <- list(
		environment(),
		parent.frame(),
		parent.env(environment()),
		parent.env(parent.env(environment())),
		parent.env(parent.frame(n=1)),
		parent.env(parent.frame(n=2)), # purrr::map
		parent.env(parent.frame(n=3)),
		parent.env(parent.frame(n=4)), # parallel::mclapply
		parent.env(parent.frame(n=5))
	)
	i_env <- 1
	source_e <- source_environments[[i_env]]
	while (i_env<length(source_environments) && !all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		i_env %<>% add(1)
		source_e <- source_environments[[i_env]]
	}
	if (all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		assign("DB", get("DB", envir=source_e), envir=target_e)
		assign("HOST", get("HOST", envir=source_e), envir=target_e)
		assign("PWD", get("PWD", envir=source_e), envir=target_e)
		assign("USER", get("USER", envir=source_e), envir=target_e)
	} else {
		init()
		logging::logerror("Context was not initialized properly. See `?load_env` for more information.", logger=LOGGER.MAIN)
		return(FALSE)
	}
	upsert_table(table=table, table_name_in_base=table_name_in_base, host=HOST, db=DB, user=USER, password=PWD, ...)
}


#' Upsert
#'
#' Simple method that inserts the input data.frame or data.table into the designated table, or updates it if the key already exists.
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in {db} to insert data into
#' @param progress_bar nice progress bar to use, it's recommended to disable it in log mode
#' @param nolog avoid any writing to the console
#' @param keycols name of the colums that
#' @keywords mysql insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, insertq
#' @export
#' @examples
#' \dontrun{data <- insert_table(iris, "iris_name_in_database", keycols=c("id"), host=HOST, db=DB, user=user, password=pwd)}
upsert_table <- function(table, table_name_in_base, keycols, host="localhost", port=3306, db, user, password,
	progress_bar=TRUE, nolog=FALSE
) {
	# INSERT INTO `item`
	# (`item_name`, items_in_stock)
	# VALUES( 'A', 27)
	# ON DUPLICATE KEY UPDATE
	# `new_items_count` = `new_items_count` + 27

	init()
	if (nrow(table) == 0) {
		if (!nolog) logging::logwarn("You tried to insert an empty table. Leaving.", logger=LOGGER.MAIN)
		return()
	}
	if (!nolog) logging::loginfo("Upserting %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)

	has_quotes <- sapply(seq(ncol(table)), function(ic) !(is.numeric(table[,ic]) || is.logical(table[,ic])))
	pb <- if(progress_bar) create_pb(nrow(table), bar_style="pc", time_style="cd") else NULL
	for (i in seq(nrow(table))) {
		prefix <- paste0("INSERT INTO ", table_name_in_base, "(", paste0(colnames(table), collapse=","), ") VALUES ")
		values <- paste0("(",
			paste0(
				sapply(seq(ncol(table)), function(ic) {
					if ((table[i, ic] %>% {is.na(.) || is.nan(.) || (is.numeric(.) && !is.finite(.))})) {
						"\"Qù@ñÐĲ€T@IS©H€ZMŒZI//@\""
					} else `if`(has_quotes[ic], paste0("'", gsub("'", '"', table[i, ic]), "'"), table[i, ic])
				}), collapse=","
			), ")"
		)
		suffix <- paste0(
			" ON DUPLICATE KEY UPDATE ",
			which(colnames(table) %ni% keycols) |> sapply(function(ic) {
				if ((table[i, ic] %>% {is.na(.) || is.nan(.) || (is.numeric(.) && !is.finite(.))})) {
					""
				} else {
					paste(
						colnames(table)[ic],
						`if`(has_quotes[ic], paste0("'", gsub("'", '\"', table[i, ic]), "'"), table[i, ic]),
						sep="="
					)
				}
			}) %>% .[map_lgl(., ~nchar(.x)>0)] |> paste(collapse = ",")
		)
		query <- paste0(prefix, gsub("\"Qù@ñÐĲ€T@IS©H€ZMŒZI//@\"", "NULL", values), suffix, ";")
		tryCatch(
			{exec_query(host, port, db, user, password, query)},
			warn=function(w) {
				if (!nolog) logging::logwarn("Warning while upserting query [%s]: [%s]", query, w, logger=LOGGER.MAIN)
			},
			error=function(e) {
				if (!nolog) logging::logerror("Error while upserting query [%s]: [%s]", query, e, logger=LOGGER.MAIN)
			}
		)
		if (progress_bar) update_pb(pb, i)
	}
}

#' Simplified update
#'
#' Simple method that updates the input data.frame or data.table into the designated table in the current DB context.
#' @param table data.frame or data.table to update
#' @param table_name_in_base table in {db} to update data into
#' @param ... any other parameter that applies to update_table
#' @keywords mysql update insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, update_table, insertq, insert_table
#' @export
#' @examples
#' \dontrun{updateq(iris, "iris_database_name")}
updateq <- function(table, table_name_in_base, ...) {
	target_e <- environment()
	source_environments <- list(
		environment(),
		parent.frame(),
		parent.env(environment()),
		parent.env(parent.env(environment())),
		parent.env(parent.frame(n=1)),
		parent.env(parent.frame(n=2)), # purrr::map
		parent.env(parent.frame(n=3)),
		parent.env(parent.frame(n=4)), # parallel::mclapply
		parent.env(parent.frame(n=5))
	)
	i_env <- 1
	source_e <- source_environments[[i_env]]
	while (i_env<length(source_environments) && !all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		i_env %<>% add(1)
		source_e <- source_environments[[i_env]]
	}
	if (all(unlist(lapply(c("DB", "HOST", "PWD", "USER"), exists, envir=source_e)))) {
		assign("DB", get("DB", envir=source_e), envir=target_e)
		assign("HOST", get("HOST", envir=source_e), envir=target_e)
		assign("PWD", get("PWD", envir=source_e), envir=target_e)
		assign("USER", get("USER", envir=source_e), envir=target_e)
	} else {
		init()
		logging::logerror("Context was not initialized properly. See `?load_env` for more information.", logger=LOGGER.MAIN)
		return(FALSE)
	}
	update_table(table=table, table_name_in_base=table_name_in_base, host=HOST, db=DB, user=USER, password=PWD, ...)
}


#' Update
#'
#' Simple method that inserts the input data.frame or data.table into the designated table, or updates it if the key already exists.
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @param table data.frame or data.table to insert
#' @param table_name_in_base table in {db} to insert data into
#' @param progress_bar nice progress bar to use, it's recommended to disable it in log mode
#' @param nolog avoid any writing to the console
#' @param keycols name of the colums that
#' @keywords mysql insert
#' @details It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
#' @seealso pull_data, selectq, insertq
#' @export
#' @examples
#' \dontrun{data <- insert_table(iris, "iris_name_in_database", keycols=c("id"), host=HOST, db=DB, user=user, password=pwd)}
update_table <- function(table, table_name_in_base, keycols, host="localhost", port=3306, db, user, password,
	progress_bar=interactive(), nolog=FALSE
) {
	# UPDATE `item` (`item_name`, items_in_stock)
	# SET `new_items_count` = `new_items_count` + 27
	# WHERE `id`=42;

	init()
	if (nrow(table) == 0) {
		if (!nolog) logging::logwarn("You tried to update with empty data. Leaving.", logger=LOGGER.MAIN)
		return()
	}
	if (!nolog) logging::loginfo("updating %s rows data into table %s.", nrow(table), table_name_in_base, logger=LOGGER.MAIN)

	has_quotes <- table |> purrr::map_lgl(~!(is.numeric(.x)||is.logical(.x)))
	pb <- if(progress_bar) create_pb(nrow(table), bar_style="pc", time_style="cd") else NULL
	for (i in seq(nrow(table))) {
		prefix <- paste0("UPDATE ", table_name_in_base, " SET ")
		suffix <- paste(
			which(colnames(table) %ni% keycols) |> sapply(function(ic) {
				value <- table[i, ic] |> unlist()
				if (is.na(value) || is.nan(value) || (is.numeric(value) && !is.finite(value))) {
					""
				} else {
					paste(
						colnames(table)[ic],
						`if`(has_quotes[ic], paste0("'", gsub("'", '\"', value), "'"), value),
						sep="="
					)
				}
			}) %>% .[map_lgl(., ~nchar(.x)>0)] |> paste(collapse = ","),
			"where",
			which(colnames(table) %in% keycols) |> sapply(function(ic) {
				value <- table[i, ic] |> unlist()
				if (is.na(value) || is.nan(value) || (is.numeric(value) && !is.finite(value))) {
					""
				} else {
					paste(
						colnames(table)[ic],
						`if`(has_quotes[ic], paste0("'", gsub("'", '\"', value), "'"), value),
						sep="="
					)
				}
			}) %>% .[map_lgl(., ~nchar(.x)>0)] |> paste(collapse = " and ")
		)
		if (grepl("^[ ]*where", suffix) | grepl("where[ ]*$", suffix)) {
			if (!nolog) logging::logfinest("Skipping incomplete row with index [%s]", i, logger=LOGGER.MAIN)
			next
		}
		query <- paste0(prefix, suffix, ";")
		tryCatch({
				exec_query(host, port, db, user, password, query)
			},
			warn=function(w) {
				if (!nolog) logging::logwarn("Warning while updating query [%s]: [%s]", query, w, logger=LOGGER.MAIN)
			},
			error=function(e) {
				if (!nolog) logging::logerror("Error while updating query [%s]: [%s]", query, e, logger=LOGGER.MAIN)
			}
		)
		if (progress_bar) update_pb(pb, i)
	}
}
