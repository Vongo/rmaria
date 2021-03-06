% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/maria.R
\name{upsert_table}
\alias{upsert_table}
\title{Upsert}
\usage{
upsert_table(
  table,
  table_name_in_base,
  keycols,
  host = "localhost",
  port = 3306,
  db,
  user,
  password,
  progress_bar = TRUE,
  nolog = FALSE
)
}
\arguments{
\item{table}{data.frame or data.table to insert}

\item{table_name_in_base}{table in {db} to insert data into}

\item{keycols}{name of the colums that}

\item{host}{host}

\item{port}{port}

\item{db}{default database name}

\item{user}{user}

\item{password}{password}

\item{progress_bar}{nice progress bar to use, it's recommended to disable it in log mode}

\item{nolog}{avoid any writing to the console}
}
\description{
Simple method that inserts the input data.frame or data.table into the designated table, or updates it if the key already exists.
}
\details{
It's important to be aware that both input table and table in database should have the same schema (matching names, matching types).
}
\examples{
\dontrun{data <- insert_table(iris, "iris_name_in_database", keycols=c("id"), host=HOST, db=DB, user=user, password=pwd)}
}
\seealso{
pull_data, selectq, insertq
}
\keyword{insert}
\keyword{mysql}
