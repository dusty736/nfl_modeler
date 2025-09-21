# tests/testthat/test-step2-team-metadata-process.R
# Basic structural checks for team metadata output — no fancy args, version-safe.

suppressPackageStartupMessages({
  library(testthat)
  library(arrow)
  library(here)
  library(dplyr)
})

processed_path <- here::here("data", "processed", "team_metadata.parquet")
raw_path       <- here::here("data", "raw", "team_metadata.parquet")

expected_cols <- c(
  "team_abbr", "team_name", "team_id", "team_nick",
  "team_conf", "team_division",
  "team_color", "team_color2", "team_color3", "team_color4",
  "team_logo_wikipedia", "team_logo_espn", "team_wordmark",
  "team_conference_logo", "team_league_logo", "team_logo_squared"
)

valid_divisions <- c(
  "AFC East","AFC North","AFC South","AFC West",
  "NFC East","NFC North","NFC South","NFC West"
)

# --- 1) Existence & readability ----------------------------------------------
test_that("team metadata processed parquet exists and is readable", {
  expect_true(file.exists(processed_path))
  df <- arrow::read_parquet(processed_path)
  expect_true(is.data.frame(df))
  if (nrow(df) == 0) fail("Processed file is empty.")
})

# --- 2) Columns & dimensions --------------------------------------------------
test_that("team metadata has expected columns and dimensions", {
  df <- arrow::read_parquet(processed_path)
  
  # Exact set of columns (order not enforced)
  expect_setequal(names(df), expected_cols)
  
  # Types: all character columns
  all_char <- vapply(df, is.character, logical(1))
  expect_true(all(all_char))
  
  # Dimensions: 35 × 16
  expect_equal(ncol(df), length(expected_cols))
  expect_equal(nrow(df), 35L)
})

# --- 3) Duplicates & keys -----------------------------------------------------
test_that("no duplicate rows and team_abbr is unique", {
  df <- arrow::read_parquet(processed_path)
  expect_equal(anyDuplicated(df), 0L)
  expect_equal(sum(duplicated(df$team_abbr)), 0L)
})

# --- 4) Content sanity --------------------------------------------------------
test_that("values pass basic sanity checks", {
  df <- arrow::read_parquet(processed_path)
  
  # Required non-missing columns
  must_have <- c("team_abbr","team_name","team_conf","team_division","team_id")
  for (col in must_have) {
    expect_false(any(is.na(df[[col]])))
    expect_true(all(trimws(df[[col]]) == df[[col]]))
  }
  
  # team_abbr: 2–3 uppercase letters
  expect_true(all(grepl("^[A-Z]{2,3}$", df$team_abbr)))
  
  # Conferences
  expect_true(all(df$team_conf %in% c("AFC","NFC")))
  
  # Divisions
  expect_true(all(df$team_division %in% valid_divisions))
  
  # team_id: four digits as character
  expect_true(all(grepl("^\\d{4}$", df$team_id)))
  
  # Colour columns: hex codes or NA
  hex_cols <- c("team_color","team_color2","team_color3","team_color4")
  for (col in hex_cols) {
    ok <- is.na(df[[col]]) | grepl("^#[0-9A-Fa-f]{6}$", df[[col]])
    expect_true(all(ok))
  }
  
  # URL columns: must start with http/https and not NA
  url_cols <- c("team_logo_wikipedia","team_logo_espn","team_wordmark",
                "team_conference_logo","team_league_logo","team_logo_squared")
  for (col in url_cols) {
    expect_false(any(is.na(df[[col]])))
    expect_true(all(grepl("^https?://", df[[col]])))
  }
})

# --- 5) Process is idempotent: processed == distinct(raw) ---------------------
test_that("processed data equals distinct(raw)", {
  if (!file.exists(raw_path)) skip("Raw parquet missing; skipping distinct equality check.")
  raw <- arrow::read_parquet(raw_path) %>% dplyr::distinct()
  out <- arrow::read_parquet(processed_path)
  
  # Align columns and sort by a stable key
  raw <- raw[, names(out)]
  raw <- dplyr::arrange(raw, team_abbr)
  out <- dplyr::arrange(out, team_abbr)
  
  expect_setequal(names(out), names(raw))
  expect_equal(out, raw, check.attributes = FALSE)
})

