test_that("quote_ident wraps in backticks and doubles internal backticks", {
  expect_equal(quote_ident("tbl"), "`tbl`")
  expect_equal(quote_ident("we`ird"), "`we``ird`")
  expect_equal(quote_ident(c("a", "b")), c("`a`", "`b`"))
})

test_that("build_insert_sql emits one placeholder tuple", {
  expect_equal(
    build_insert_sql("t", c("a", "b"), ignore = TRUE),
    "INSERT IGNORE INTO `t` (`a`,`b`) VALUES (?,?)"
  )
  expect_equal(
    build_insert_sql("t", c("a"), ignore = FALSE),
    "INSERT INTO `t` (`a`) VALUES (?)"
  )
})

test_that("build_upsert_sql sets COALESCE for non-key cols only", {
  expect_equal(
    build_upsert_sql("t", c("id", "a", "b"), keycols = "id"),
    "INSERT INTO `t` (`id`,`a`,`b`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `a`=COALESCE(VALUES(`a`),`a`),`b`=COALESCE(VALUES(`b`),`b`)"
  )
})

test_that("build_upsert_sql with only key cols falls back to INSERT IGNORE", {
  expect_equal(
    build_upsert_sql("t", c("id"), keycols = "id"),
    "INSERT IGNORE INTO `t` (`id`) VALUES (?)"
  )
})

test_that("build_update_join_sql joins on keys and COALESCEs non-keys", {
  expect_equal(
    build_update_join_sql("t", "tmp", c("id1", "id2", "v"), keycols = c("id1", "id2")),
    "UPDATE `t` t JOIN `tmp` s ON t.`id1`=s.`id1` AND t.`id2`=s.`id2` SET t.`v`=COALESCE(s.`v`,t.`v`)"
  )
})

test_that("build_update_join_sql handles a single key (no AND)", {
  expect_equal(
    build_update_join_sql("t", "tmp", c("id", "v"), keycols = "id"),
    "UPDATE `t` t JOIN `tmp` s ON t.`id`=s.`id` SET t.`v`=COALESCE(s.`v`,t.`v`)"
  )
})

test_that("builders reject empty cols / keycols / no-update", {
  expect_error(build_insert_sql("t", character(0)), "non-empty")
  expect_error(build_upsert_sql("t", character(0), keycols = "id"), "non-empty")
  expect_error(build_upsert_sql("t", c("id", "a"), keycols = character(0)), "non-empty")
  expect_error(build_update_join_sql("t", "tmp", c("id", "v"), keycols = character(0)), "non-empty")
  expect_error(build_update_join_sql("t", "tmp", c("id"), keycols = "id"), "no non-key")
})
