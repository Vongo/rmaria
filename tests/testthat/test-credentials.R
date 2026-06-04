test_that("resolve_credentials finds creds in the wrapper's own environment", {
  wrapper <- function() { DB <- "d2"; HOST <- "h2"; USER <- "u2"; PWD <- "p2"; resolve_credentials() }
  cr <- wrapper()
  expect_equal(cr$db, "d2"); expect_equal(cr$host, "h2")
  expect_equal(cr$user, "u2"); expect_equal(cr$pwd, "p2")
})

test_that("resolve_credentials finds creds set in the wrapper's caller", {
  wrapper <- function() resolve_credentials()
  runner  <- function() { DB <- "d1"; HOST <- "h1"; USER <- "u1"; PWD <- "p1"; wrapper() }
  cr <- runner()
  expect_equal(cr$db, "d1"); expect_equal(cr$host, "h1")
  expect_equal(cr$user, "u1"); expect_equal(cr$pwd, "p1")
})

test_that("resolve_credentials finds creds across an lapply frame", {
  wrapper <- function() resolve_credentials()
  runner  <- function() { DB <- "dm"; HOST <- "hm"; USER <- "um"; PWD <- "pm"; lapply(1, function(i) wrapper())[[1]] }
  cr <- runner()
  expect_equal(cr$db, "dm")
})

test_that("resolve_credentials stops with a clear message when creds are absent", {
  rm(list = intersect(c("DB","HOST","USER","PWD"), ls(envir = globalenv())), envir = globalenv())
  wrapper <- function() resolve_credentials()
  expect_error(wrapper(), "credentials not found")
})

test_that("resolve_credentials resolves PORT when set, defaults to 3306", {
  wrapper <- function() resolve_credentials()
  with_port <- function() { DB<-"d";HOST<-"h";USER<-"u";PWD<-"p";PORT<-33306L; wrapper() }
  no_port  <- function() { DB<-"d";HOST<-"h";USER<-"u";PWD<-"p"; wrapper() }
  expect_equal(with_port()$port, 33306L)
  expect_equal(no_port()$port, 3306L)
})

test_that("selectq resolves creds (incl. port) from the caller and runs (integration)", {
  skip_if_no_db(); e <- db_env()
  DB <- e$db; HOST <- e$host; USER <- e$user; PWD <- e$pwd; PORT <- e$port
  got <- selectq("SELECT 1 AS one")
  expect_equal(got$one[1], 1)
})

test_that("selectq stops (not FALSE) when credentials are absent", {
  rm(list = intersect(c("DB","HOST","USER","PWD"), ls(envir = globalenv())), envir = globalenv())
  expect_error(selectq("SELECT 1"), "credentials not found")
})

test_that("resolve_credentials coerces a valid string PORT", {
  wrapper <- function() resolve_credentials()
  f <- function() { DB<-"d"; HOST<-"h"; USER<-"u"; PWD<-"p"; PORT<-"33306"; wrapper() }
  expect_equal(f()$port, 33306L)
})

test_that("resolve_credentials warns and defaults to 3306 for a non-numeric PORT", {
  wrapper <- function() resolve_credentials()
  f <- function() { DB<-"d"; HOST<-"h"; USER<-"u"; PWD<-"p"; PORT<-"bad"; wrapper() }
  expect_warning(cr <- f(), "not a valid integer")
  expect_equal(cr$port, 3306L)
})
