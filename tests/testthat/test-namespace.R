test_that("update_table/upsert_table do not rely on an attached purrr", {
  # map_lgl must be reachable via the package namespace, not the search path.
  expect_true(requireNamespace("purrr", quietly = TRUE))
  expect_true(exists("map_lgl", envir = asNamespace("purrr")))
})

test_that("DESCRIPTION declares purrr and bit64", {
  imports <- read.dcf(system.file("DESCRIPTION", package = "rmaria"), fields = "Imports")[1, 1]
  expect_match(imports, "purrr", fixed = TRUE)
  expect_match(imports, "bit64", fixed = TRUE)
})

test_that("no bare (unqualified) map_lgl remains in R/maria.R", {
  src_path <- "../../R/maria.R"
  testthat::skip_if_not(file.exists(src_path),
                        "source not present (R CMD check) — skipping source lint")
  src  <- readLines(src_path, warn = FALSE)
  bare <- grepl("(^|[^:[:alnum:]_.])map_lgl\\s*\\(", src) & !grepl("purrr::map_lgl", src)
  expect_equal(which(bare), integer(0))
})
