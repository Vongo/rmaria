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

#' Truncate table
#'
#' Empties table from all observations, but doesn't delete it.
#' @param table_name_in_base table in \code{db} to truncate
#' @param host host
#' @param port port
#' @param db default database name
#' @param user user
#' @param password password
#' @keywords mysql delete
#' @export
#' @examples
#' \dontrun{truncate_table(table_name_in_base="foo", host=HOST, db=DB, user=USER, password=PWD)}
truncate_table <- function(table_name_in_base, host="localhost", port=3306, db, user, password) {
  logging::loginfo("Truncating table %s.", table_name_in_base, logger=LOGGER.MAIN)
  con <- .maria_connect(host, port, db, user, password)
  on.exit(RMariaDB::dbDisconnect(con), add = TRUE)
  RMariaDB::dbExecute(con, paste0("TRUNCATE TABLE ", DBI::dbQuoteIdentifier(con, table_name_in_base)))
}
