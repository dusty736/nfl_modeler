test_that("write_parquet_safely() is idempotent and safe", {
  skip_if_not(fn_exists("write_parquet_safely"))
  
  df <- tibble::tibble(
    game_id = c("2024090801","2024090802","2024090803"),
    week = c(1L,1L,1L),
    value = c(0.1, 2.5, NA_real_)
  )
  
  p <- tmp_path("io", "safe.parquet")
  dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)
  
  # First write
  expect_error(write_parquet_safely(df, p), NA)
  expect_true(file.exists(p))
  
  # Read back and compare schema & data
  df1 <- arrow::read_parquet(p)
  expect_equal(names(df1), names(df))
  expect_equal(dplyr::arrange(df1, game_id), dplyr::arrange(df, game_id))
  
  # Second write (should not fail or corrupt)
  expect_error(write_parquet_safely(df, p), NA)
  df2 <- arrow::read_parquet(p)
  expect_equal(dplyr::arrange(df2, game_id), dplyr::arrange(df, game_id))
})

test_that("safe_read_parquet() returns a tibble with expected columns", {
  skip_if_not(fn_exists("safe_read_parquet"))
  
  # Create a small parquet we control
  p <- tmp_path("io", "readme.parquet")
  dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)
  df <- tibble::tibble(a = 1:3, b = c("x","y","z"))
  arrow::write_parquet(df, p)
  
  # Should read successfully
  expect_error({
    out <- safe_read_parquet(p)
    expect_true(is.data.frame(out) || tibble::is_tibble(out))
    expect_equal(colnames(out), c("a","b"))
    expect_equal(nrow(out), 3L)
  }, NA)
  
  # Missing file should not hard-crash (your design may return NULL or error with message)
  expect_error({
    out2 <- safe_read_parquet(tmp_path("io", "does_not_exist.parquet"))
    # Accept either NULL or empty tibble if that's your convention; just ensure no throw above
    expect_true(is.null(out2) || is.data.frame(out2))
  }, NA)
})
