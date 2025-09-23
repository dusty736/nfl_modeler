# tests/testthat/test-step4-download-staging.R
# Purpose: SIMPLE, non-flaky checks for the Step 4 staging downloader script.
# Hugh Grant voice: find the script, give it a polite once-over, and go home.

suppressPackageStartupMessages({
  library(testthat)
  library(here)
  library(stringr)
})

# Try all likely locations/names (you said there are 4 scripts; we spot-check the two download ones)
candidate_paths <- c(
  here::here("etl", "R", "download_staging_nfl_data.R"),
  here::here("etl", "R", "step4_update_db", "step4_weekly_update_data_download.R"),
  here::here("etl", "R", "step4_update_db", "step4_daily_update_data_download.R")
)

existing <- candidate_paths[file.exists(candidate_paths)]
script_path <- if (length(existing) > 0) existing[[1]] else NA_character_

test_that("a Step 4 download script exists in an expected location", {
  msg <- paste0(
    "Checked:\n- ", paste(candidate_paths, collapse = "\n- ")
  )
  expect_true(length(existing) > 0, info = msg)
})

# If we didn't find it, bow out gracefully from the rest.
skip_if_not(length(existing) > 0, "No Step 4 download script found in expected locations.")

test_that("download script is syntactically valid (parsed, not executed)", {
  expect_error(parse(file = script_path), NA)
})

test_that("script loads expected core packages", {
  lines <- readLines(script_path, warn = FALSE)
  expect_true(any(str_detect(lines, "library\\(nflreadr\\)")))
  expect_true(any(str_detect(lines, "library\\(tidyverse\\)")))
  expect_true(any(str_detect(lines, "library\\(arrow\\)")))
  expect_true(any(str_detect(lines, "library\\(here\\)")))
})

test_that("script defines seasons and weeks in the usual way", {
  txt <- paste(readLines(script_path, warn = FALSE), collapse = "\n")
  expect_true(str_detect(txt, "seasons\\s*<-\\s*lubridate::year\\(Sys\\.Date\\(\\)\\)"))
  # Be tolerant of spacing and min-week logic.
  expect_true(str_detect(
    txt,
    "weeks\\s*<-\\s*max\\s*\\(\\s*nflreadr::get_current_week\\(\\)\\s*-\\s*2\\s*,\\s*1\\s*\\)\\s*:\\s*nflreadr::get_current_week\\(\\)"
  ))
})

test_that("script sources expected helper function files (spot checks)", {
  lines <- readLines(script_path, warn = FALSE)
  expect_true(any(str_detect(lines, "step2_pbp_process_functions\\.R")))
  expect_true(any(str_detect(lines, "step2_rosters_process_functions\\.R")))
  expect_true(any(str_detect(lines, "step2_depth_charts_process_functions\\.R")))
  expect_true(any(str_detect(lines, "step3_long_player_format_functions\\.R")))
  expect_true(any(str_detect(lines, "step3_long_team_format_functions\\.R")))
  expect_true(any(str_detect(lines, "step3_team_rankings_long_functions\\.R")))
})

test_that("script writes a core set of staging parquet outputs (by filename, literal OR here())", {
  txt <- paste(readLines(script_path, warn = FALSE), collapse = "\n")
  
  # Helper: pass if we see either a literal "data/staging/<file>"
  # OR a here("data","staging","<file>") call in write_parquet(…)
  # Helper: pass if we see either a literal "data/staging/<file>"
  # OR any here("data","staging","<file>") nearby (namespaced or not, multiline ok).
  saw_output <- function(file) {
    txt <- paste(readLines(script_path, warn = FALSE), collapse = "\n")
    
    # 1) Literal path in the script
    literal_ok <- stringr::str_detect(txt, stringr::fixed(file.path("data/staging", file)))
    
    # 2) A here("data","staging","<file>") anywhere
    here_any <- stringr::str_detect(
      txt,
      paste0('here\\s*\\(\\s*"data"\\s*,\\s*"staging"\\s*,\\s*"', stringr::fixed(file), '"\\s*\\)')
    )
    
    # 3) here(...) specifically as the second arg to write_parquet (namespaced ok, multiline ok)
    write_here <- stringr::str_detect(
      txt,
      paste0(
        '(?s)(?:arrow::)?write_parquet\\s*\\(',
        '.*?',  # first arg
        ',\\s*',
        'here\\s*\\(\\s*"data"\\s*,\\s*"staging"\\s*,\\s*"', stringr::fixed(file), '"\\s*\\)',
        '.*?\\)'
      )
    )
    
    literal_ok || here_any || write_here
  }
  
  expected_files <- c(
    "pbp.parquet",
    "pbp_games.parquet",
    "team_strength_tbl.parquet",
    "rosters.parquet",
    "roster_summary.parquet",
    "roster_position_summary.parquet",
    "weekly_stats_qb.parquet",
    "weekly_stats_rb.parquet",
    "weekly_stats_wr.parquet",
    "weekly_stats_te.parquet",
    "off_team_stats_week.parquet",
    "def_player_stats_weekly.parquet",
    "def_team_stats_week.parquet",
    "st_player_stats_weekly.parquet",
    "id_map.parquet",
    "contracts_position_cap_pct.parquet",
    "contracts_qb.parquet",
    "games.parquet",
    "weekly_results.parquet",
    "player_weekly_tbl.parquet",
    "team_weekly_tbl.parquet",
    "team_weekly_rankings_tbl.parquet"
  )
  
  for (f in expected_files) {
    expect_true(saw_output(f), info = paste("Expected write_parquet for:", file.path("data/staging", f)))
  }
})

test_that("script uses nflreadr loaders we rely on (spot checks)", {
  txt <- paste(readLines(script_path, warn = FALSE), collapse = "\n")
  for (fn in c(
    "load_pbp\\(", "load_rosters\\(", "load_depth_charts\\(", "load_nextgen_stats\\(",
    "load_snap_counts\\(", "load_espn_qbr\\(", "load_player_stats\\(", "load_schedules\\("
  )) {
    expect_true(str_detect(txt, fn), info = paste("Expected:", fn))
  }
})

test_that("script ends with a success message (optional nicety)", {
  lines <- readLines(script_path, warn = FALSE)
  # Accept either exact message or any message() call mentioning 'staging'
  ok_exact <- any(str_detect(lines, 'message\\("All staging data saved to /data/staging"\\)'))
  ok_fuzzy <- any(str_detect(paste(lines, collapse = "\n"), "message\\(.*staging.*\\)"))
  expect_true(ok_exact || ok_fuzzy)
})
