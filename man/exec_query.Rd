% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/maria.R
\name{exec_query}
\alias{exec_query}
\title{Execute query}
\usage{
exec_query(host = "localhost", port = 3306, db, user, password, query)
}
\arguments{
\item{host}{host}

\item{port}{port}

\item{db}{default database name}

\item{user}{user}

\item{password}{password}

\item{query}{query to execute}
}
\description{
Simple method that executes your query and doesn't return anything.
}
\examples{
\dontrun{data <- pull_data(host=HOST, db=DB, user=user, password=pwd, query="select * from table;")}
}
\seealso{
insert_table
}
\keyword{create}
\keyword{delete}
\keyword{mysql}
\keyword{statement}
