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
