test_that("path helpers (if any) resolve under repo or temp root and never escape", {
  # All optional — each block guarded by presence.
  if (fn_exists("path_raw")) {
    p <- path_raw("somefile.parquet")
    expect_true(grepl("raw", p, fixed = TRUE))
    expect_false(grepl("\\.\\.", p))  # no parent escapes
  } else {
    skip("path_raw() not present")
  }
  
  if (fn_exists("path_processed")) {
    p <- path_processed("part", "data.parquet")
    expect_true(grepl("processed", p, fixed = TRUE))
    expect_false(grepl("\\.\\.", p))
  } else {
    skip("path_processed() not present")
  }
  
  if (fn_exists("path_staging")) {
    p <- path_staging("x.parquet")
    expect_true(grepl("staging", p, fixed = TRUE))
    expect_false(grepl("\\.\\.", p))
  } else {
    skip("path_staging() not present")
  }
  
  if (fn_exists("path_for_database")) {
    p <- path_for_database("y.parquet")
    expect_true(grepl("for_database", p, fixed = TRUE))
    expect_false(grepl("\\.\\.", p))
  } else {
    skip("path_for_database() not present")
  }
})
