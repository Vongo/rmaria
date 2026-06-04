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
