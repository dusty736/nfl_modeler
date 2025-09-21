test_that("safe_tbl() handles missing tables without throwing", {
  skip_if_not(fn_exists("safe_tbl"))
  
  # Expect: calling with a definitely-missing table does NOT error.
  expect_error({
    out <- safe_tbl("__chatgpt_absent_table__")
    # Accept either NULL (preferred) or a harmless sentinel object; just ensure no error.
    expect_true(is.null(out) || inherits(out, "tbl_dbi") || inherits(out, "tbl_sql") || inherits(out, "tbl"))
  }, NA)
})

test_that("tbl_exists() returns a logical and never throws", {
  skip_if_not(fn_exists("tbl_exists"))
  
  expect_error({
    res <- tbl_exists("__chatgpt_absent_table__")
    expect_true(is.logical(res) && length(res) == 1)
  }, NA)
})

test_that("ensure_dir() creates directories idempotently", {
  skip_if_not(fn_exists("ensure_dir"))
  
  d <- tmp_path("nested", "dir", "here")
  # First create
  expect_error(ensure_dir(d), NA)
  expect_true(dir.exists(d))
  # Call again (idempotent)
  expect_error(ensure_dir(d), NA)
  expect_true(dir.exists(d))
})

test_that("type coercers (if present) respect NA semantics", {
  # These are optional — only run if you have them.
  if (fn_exists("coerce_int")) {
    x <- c("1","2",NA,"3")
    expect_equal(coerce_int(x), as.integer(c(1,2,NA,3)))
  } else {
    skip("coerce_int() not present")
  }
  if (fn_exists("coerce_date")) {
    x <- c("2024-09-01","not-a-date",NA)
    out <- coerce_date(x)
    expect_s3_class(out, "Date")
    expect_true(is.na(out[2]) && is.na(out[3]))
  } else {
    skip("coerce_date() not present")
  }
})
