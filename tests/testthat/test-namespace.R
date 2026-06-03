test_that("update_table/upsert_table do not rely on an attached purrr", {
  # modify_if must be reachable via the package namespace, not the search path.
  expect_true(requireNamespace("purrr", quietly = TRUE))
  expect_true(exists("modify_if", envir = asNamespace("purrr")))
})

test_that("DESCRIPTION declares purrr and bit64", {
  imports <- read.dcf(system.file("DESCRIPTION", package = "rmaria"), fields = "Imports")[1, 1]
  expect_match(imports, "purrr", fixed = TRUE)
  expect_match(imports, "bit64", fixed = TRUE)
})

test_that("no bare (unqualified) modify_if or map_lgl remains in R/", {
  src_files <- list.files("../../R", pattern = "\\.R$", full.names = TRUE)
  testthat::skip_if(length(src_files) == 0L, "source not present (R CMD check) -- skipping source lint")
  src  <- unlist(lapply(src_files, readLines, warn = FALSE))
  bare <- grepl("(^|[^:[:alnum:]_.])(modify_if|map_lgl)\\s*\\(", src) & !grepl("purrr::(modify_if|map_lgl)", src)
  expect_equal(which(bare), integer(0))
})
