# tests/testthat/test-step4-update-prod.R
# Purpose: SIMPLE, non-flaky checks for Step 4 "update prod".
# Hugh Grant voice: a neat haircut for your deployment script — nothing baroque.

suppressPackageStartupMessages({
  library(testthat)
  library(here)
  library(stringr)
})

# --- Candidate file paths ------------------------------------------------------
script_candidates <- c(
  here("etl", "R", "step4_update_db", "step4_update_prod.R")
)

func_candidates <- c(
  here("etl", "R", "step4_update_db", "step4_update_prod_functions.R"),
  here("etl", "R", "step4_update_db", "upsert_stage_to_prod.R")  # you pasted this name in the message
)

script_path <- script_candidates[file.exists(script_candidates)][1]
func_path   <- func_candidates[file.exists(func_candidates)][1]

test_that("step4 update prod script exists in an expected location", {
  msg <- paste("Checked:", paste(script_candidates, collapse = " | "))
  expect_true(!is.na(script_path) && nzchar(script_path), info = msg)
})

test_that("step4 update prod functions file exists in an expected location", {
  msg <- paste("Checked:", paste(func_candidates, collapse = " | "))
  expect_true(!is.na(func_path) && nzchar(func_path), info = msg)
})

skip_if(is.na(script_path), "Step4 prod script not found; skipping content checks.")
skip_if(is.na(func_path),   "Step4 prod functions file not found; skipping content checks.")

# --- Parse-only sanity (no execution) -----------------------------------------
test_that("both files are syntactically valid (parsed, not executed)", {
  expect_error(parse(file = script_path), NA)
  expect_error(parse(file = func_path), NA)
})

# --- Script: sources the expected helper files --------------------------------
test_that("script sources the expected helper files", {
  s <- readLines(script_path, warn = FALSE)
  txt <- paste(s, collapse = "\n")
  expect_true(str_detect(txt, "step4_update_prod_functions\\.R"))
  expect_true(str_detect(txt, "step3_parquet_to_postgres_functions\\.R"))
  expect_true(str_detect(txt, "step3_database_file_prep_functions\\.R"))
  expect_true(str_detect(txt, "utils\\.R"))
})

# --- Script: defines key_map with a few must-have entries ---------------------
test_that("script defines key_map with expected table->key mappings (spot checks)", {
  txt <- paste(readLines(script_path, warn = FALSE), collapse = "\n")
  # Spot-check several entries — exact keys matter to your upsert contracts.
  expect_true(str_detect(txt, 'games_tbl\\s*=\\s*c\\("game_id"\\)'))
  expect_true(str_detect(txt, 'weekly_results_tbl\\s*=\\s*c\\("game_id","team_id"\\)'))
  expect_true(str_detect(txt, 'rosters_tbl\\s*=\\s*c\\("season","week","team_id","player_id"\\)'))
  expect_true(str_detect(txt, 'contracts_qb_tbl\\s*=\\s*c\\("gsis_id","team","year_signed"\\)'))
  expect_true(str_detect(txt, 'team_weekly_tbl\\s*=\\s*c\\("season","season_type","week","team","stat_name","stat_type"\\)'))
  expect_true(str_detect(txt, 'player_weekly_tbl\\s*=\\s*c\\("season","season_type","week","player_id","stat_name","stat_type"\\)'))
  # Rankings MV input presence
  expect_true(str_detect(txt, 'team_weekly_rankings_tbl\\s*=\\s*c\\("season",\\s*"week",\\s*"team",\\s*"stat_name"\\)'))
})

# --- Script: refreshes MVs politely ------------------------------------------
test_that("script refreshes the four player weekly materialized views", {
  s <- readLines(script_path, warn = FALSE)
  txt <- paste(s, collapse = "\n")
  # The four MVs named explicitly
  expect_true(str_detect(txt, 'prod\\.player_weekly_qb_mv'))
  expect_true(str_detect(txt, 'prod\\.player_weekly_rb_mv'))
  expect_true(str_detect(txt, 'prod\\.player_weekly_wr_mv'))
  expect_true(str_detect(txt, 'prod\\.player_weekly_te_mv'))
  # Uses REFRESH MATERIALIZED VIEW (CONCURRENTLY optional/fallback)
  expect_true(str_detect(txt, "REFRESH MATERIALIZED VIEW"))
})

# --- Functions file: expected function definitions exist ----------------------
test_that("functions file defines required helpers and upsert runners", {
  ftxt <- paste(readLines(func_path, warn = FALSE), collapse = "\n")
  must_have <- c(
    "get_cols\\s*<-\\s*function",
    "unique_index_colsets\\s*<-\\s*function",
    "has_matching_unique_index\\s*<-\\s*function",
    "qcsv\\s*<-\\s*function",
    "tuple_expr\\s*<-\\s*function",
    "upsert_table\\s*<-\\s*function",
    "upsert_all\\s*<-\\s*function",
    "refresh_mv\\s*<-\\s*function"
  )
  for (rx in must_have) expect_true(str_detect(ftxt, rx), info = paste("Missing:", rx))
})

# --- Functions file: upsert_table builds the expected SQL branches ------------
test_that("upsert_table contains ON CONFLICT and fallback DELETE+INSERT paths", {
  ftxt <- paste(readLines(func_path, warn = FALSE), collapse = "\n")
  expect_true(str_detect(ftxt, "ON CONFLICT \\("))          # happy path with unique index
  expect_true(str_detect(ftxt, "DO UPDATE"))                # update branch
  expect_true(str_detect(ftxt, "DO NOTHING;"))              # no-nonkey branch
  # With this more forgiving pair:
  expect_true(
    str_detect(ftxt, "(?s)DELETE\\s+FROM\\s+.*?USING\\s*\\("),
    info = "Expected fallback DELETE ... USING ( ... ) block"
  )
  
  # And keep the insert check as-is (or make it flexible too, if you like):
  # expect_true(str_detect(ftxt, "INSERT INTO .* SELECT"))
  expect_true(
    str_detect(ftxt, "(?s)INSERT\\s+INTO\\s+.*?SELECT\\s"),
    info = "Expected fallback INSERT ... SELECT ... block"
  )
  expect_true(str_detect(ftxt, "INSERT INTO .* SELECT"))    # fallback insert
  expect_true(str_detect(ftxt, "ANALYZE "))                 # analyze afterwards
})

# --- Functions file: tuple_expr/qcsv appear to use dbQuoteIdentifier ----------
test_that("qcsv and tuple_expr quote identifiers via DBI", {
  ftxt <- paste(readLines(func_path, warn = FALSE), collapse = "\n")
  expect_true(str_detect(ftxt, "dbQuoteIdentifier\\("))
})
