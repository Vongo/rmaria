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
#' @param on_nul behavior when a text column contains embedded NUL bytes (UTF-16 data): "decode" (default) transparently re-fetches and decodes to UTF-8, "strip" removes NUL bytes, "error" stops with an actionable message. Auto-recovery wraps the query as a derived table, so it may not apply to SELECT * joins with duplicate column names, multi-statement, or line-commented queries; those surface the actionable error instead.
#' @keywords mysql select
#' @seealso insert_table
#' @export
#' @examples
#' \dontrun{data <- pull_data(host=HOST, db=DB, user=user, password=pwd, query="select * from table;")}
pull_data <- function(host="localhost", port=3306, db, user, password, query, verbose=TRUE, keep_int64=FALSE, retries=1, retry_delay=1, on_nul=c("decode", "error", "strip")) {
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

	on_nul <- match.arg(on_nul)

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
			state$data <- dbGetQuery_nul_safe(con, query, on_nul=on_nul, verbose=verbose)
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

		if (inherits(state$last_error, "rmaria_embedded_nul")) {
			break   # deterministic: retrying will not help
		}

		if (attempt < retries) {
			Sys.sleep(retry_delay)
		}
	}

	if (is.null(state$data)) {
		error_msg <- if (!is.null(state$last_error)) conditionMessage(state$last_error) else "Unknown error"
		logging::logerror("Error while fetching data with query [%s] after %d attempts:\n[%s]", query, attempt, error_msg, logger=LOGGER.MAIN)
		stop(sprintf("pull_data failed after %d attempts: %s", attempt, error_msg))
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
#' Simple method that executes your query and returns (invisibly) the number of rows affected.
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @param query query to execute
#' @return (invisibly) the number of rows affected.
#' @keywords mysql delete create statement
#' @seealso insert_table
#' @export
#' @examples
#' \dontrun{exec_query(host=HOST, db=DB, user=USER, password=PWD, query="TRUNCATE TABLE foo;")}
exec_query <- function(host="localhost", port=3306, db, user, password, query) {
  if (missing(query) || is.null(query) || !is.character(query) || nchar(trimws(query)) == 0) {
    stop("exec_query: 'query' must be a non-empty character string")
  }
  if (missing(db) || is.null(db) || !is.character(db) || nchar(trimws(db)) == 0) {
    stop("exec_query: 'db' must be a non-empty character string")
  }
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  tryCatch(
    RMariaDB::dbExecute(con, query),
    error = function(e) {
      logging::logerror("exec_query failed on db=%s: %s", db, conditionMessage(e), logger = LOGGER.MAIN)
      stop(e)
    }
  )
}
