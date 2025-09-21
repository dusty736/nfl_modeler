# tests/testthat/test-step2-def-player-stats-process.R

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(here)
  library(arrow)
  library(purrr)
})

testthat::local_edition(3)
options(lifecycle_verbosity = "quiet")

# -----------------------------------------------------------------------------
# Source functions under test (robust pathing)
# -----------------------------------------------------------------------------
fpath <- here::here("etl", "R", "step2_process", "step2_def_player_stats_process_functions.R")
if (!file.exists(fpath)) stop("Cannot find functions file: ", fpath)
source(fpath)

raw_path <- here::here("data", "raw", "def_player_stats.parquet")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
# Mirror the aliasing done in the calling script so the function sees expected names.
normalize_raw_for_process <- function(df) {
  if (!"def_tackles" %in% names(df) && "def_tackles_solo" %in% names(df)) {
    df$def_tackles <- df$def_tackles_solo
  }
  if (!"def_fumble_recovery_own" %in% names(df) && "fumble_recovery_own" %in% names(df)) {
    df$def_fumble_recovery_own <- df$fumble_recovery_own
  }
  if (!"def_fumble_recovery_yards_own" %in% names(df) && "fumble_recovery_yards_own" %in% names(df)) {
    df$def_fumble_recovery_yards_own <- df$fumble_recovery_yards_own
  }
  if (!"def_fumble_recovery_opp" %in% names(df) && "fumble_recovery_opp" %in% names(df)) {
    df$def_fumble_recovery_opp <- df$fumble_recovery_opp
  }
  if (!"def_fumble_recovery_yards_opp" %in% names(df) && "fumble_recovery_yards_opp" %in% names(df)) {
    df$def_fumble_recovery_yards_opp <- df$fumble_recovery_yards_opp
  }
  if (!"def_safety" %in% names(df) && "def_safeties" %in% names(df)) {
    df$def_safety <- df$def_safeties
  }
  if (!"def_penalty" %in% names(df) && "penalties" %in% names(df)) {
    df$def_penalty <- df$penalties
  }
  if (!"def_penalty_yards" %in% names(df) && "penalty_yards" %in% names(df)) {
    df$def_penalty_yards <- df$penalty_yards
  }
  df
}

# Pick a few numeric "stat" columns by regex that actually exist
pick_stat_cols <- function(df) {
  cand <- grep("(tack|sack|int|ff|pd|qb_?hit|press|tfl|solo|assist|qb_pressure|tackle)",
               names(df), ignore.case = TRUE, value = TRUE)
  dplyr::select(df, tidyselect::any_of(cand)) |>
    dplyr::select(where(is.numeric)) |>
    names()
}

# -----------------------------------------------------------------------------
# process_defensive_player_stats()
# -----------------------------------------------------------------------------
test_that("process_defensive_player_stats: smoke + schema + sane types", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 500) |>
    normalize_raw_for_process()
  
  cleaned <- process_defensive_player_stats(raw)
  
  expect_s3_class(cleaned, "tbl_df")
  expect_true(ncol(cleaned) >= 4)
  
  for (c in c("player_name", "team")) {
    if (c %in% names(cleaned)) expect_type(cleaned[[c]], "character")
  }
  for (c in c("season", "week")) {
    if (c %in% names(cleaned)) expect_true(is.integer(cleaned[[c]]) || is.numeric(cleaned[[c]]))
  }
  
  stat_cols <- pick_stat_cols(cleaned)
  if (length(stat_cols)) {
    mins <- cleaned |>
      dplyr::summarise(dplyr::across(dplyr::all_of(stat_cols), ~ suppressWarnings(min(.x, na.rm = TRUE))))
    mins_vec <- as.numeric(mins[1, ])
    expect_true(all(is.na(mins_vec) | mins_vec >= 0))
  }
  
  # Zero-row behaviour (schema preserved)
  empty_in <- normalize_raw_for_process(raw)[0, ]
  empty_out <- process_defensive_player_stats(empty_in)
  expect_identical(names(empty_out), names(cleaned))
  expect_identical(nrow(empty_out), 0L)
})

test_that("process_defensive_player_stats: missing critical column throws a helpful error", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 50) |>
    normalize_raw_for_process()
  
  victim <- intersect(c("season", "week", "player_id", "team"), names(raw))
  skip_if(length(victim) == 0, "No likely-required columns found to drop")
  
  bad <- dplyr::select(raw, -dplyr::all_of(victim[1]))
  expect_error(process_defensive_player_stats(bad), regexp = victim[1])
})

# -----------------------------------------------------------------------------
# summarize_defensive_player_stats_by_season()
# -----------------------------------------------------------------------------
test_that("summarize_defensive_player_stats_by_season: grouping shape and sums", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 1000) |>
    normalize_raw_for_process()
  
  cleaned <- process_defensive_player_stats(raw)
  season_df <- summarize_defensive_player_stats_by_season(cleaned)
  
  expect_s3_class(season_df, "tbl_df")
  
  if (all(c("player_id", "season") %in% names(cleaned)) &&
      all(c("player_id", "season") %in% names(season_df))) {
    expect_equal(
      nrow(dplyr::distinct(season_df, player_id, season)),
      nrow(dplyr::distinct(cleaned, player_id, season))
    )
    
    stat <- intersect(pick_stat_cols(cleaned),
                      names(dplyr::select(season_df, where(is.numeric)))) |>
      purrr::pluck(1, .default = NA_character_)
    skip_if(is.na(stat), "No common numeric stat to validate")
    
    sample_groups <- dplyr::distinct(cleaned, player_id, season) |>
      dplyr::slice_head(n = 5)
    check_tbl <- sample_groups |>
      dplyr::rowwise() |>
      dplyr::mutate(
        weekly_sum = sum(cleaned[[stat]][cleaned$player_id == .data$player_id &
                                           cleaned$season == .data$season], na.rm = TRUE),
        season_val = {
          v <- season_df[[stat]][season_df$player_id == .data$player_id &
                                   season_df$season == .data$season]
          if (length(v)) v[[1]] else NA_real_
        }
      ) |>
      dplyr::ungroup()
    
    expect_equal(check_tbl$season_val, check_tbl$weekly_sum, tolerance = 1e-8)
  }
})

test_that("summarize_defensive_player_stats_by_season: zero-row input ok", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 50) |>
    normalize_raw_for_process()
  cleaned <- process_defensive_player_stats(raw)
  empty <- cleaned[0, ]
  out <- summarize_defensive_player_stats_by_season(empty)
  expect_s3_class(out, "tbl_df")
  expect_identical(nrow(out), 0L)
})

# -----------------------------------------------------------------------------
# summarize_defensive_stats_by_team_season()
# -----------------------------------------------------------------------------
test_that("summarize_defensive_stats_by_team_season: grouping shape and sums", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 1000) |>
    normalize_raw_for_process()
  cleaned <- process_defensive_player_stats(raw)
  team_season <- summarize_defensive_stats_by_team_season(cleaned)
  
  expect_s3_class(team_season, "tbl_df")
  
  if (all(c("team", "season") %in% names(cleaned)) &&
      all(c("team", "season") %in% names(team_season))) {
    expect_equal(
      nrow(dplyr::distinct(team_season, team, season)),
      nrow(dplyr::distinct(cleaned, team, season))
    )
    
    stat <- intersect(pick_stat_cols(cleaned),
                      names(dplyr::select(team_season, where(is.numeric)))) |>
      purrr::pluck(1, .default = NA_character_)
    skip_if(is.na(stat), "No common numeric stat to validate")
    
    sample_groups <- dplyr::distinct(cleaned, team, season) |>
      dplyr::slice_head(n = 5)
    check_tbl <- sample_groups |>
      dplyr::rowwise() |>
      dplyr::mutate(
        weekly_sum = sum(cleaned[[stat]][cleaned$team == .data$team &
                                           cleaned$season == .data$season], na.rm = TRUE),
        agg_val = {
          v <- team_season[[stat]][team_season$team == .data$team &
                                     team_season$season == .data$season]
          if (length(v)) v[[1]] else NA_real_
        }
      ) |>
      dplyr::ungroup()
    
    expect_equal(check_tbl$agg_val, check_tbl$weekly_sum, tolerance = 1e-8)
  }
})

test_that("summarize_defensive_stats_by_team_season: zero-row input ok", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 50) |>
    normalize_raw_for_process()
  cleaned <- process_defensive_player_stats(raw)
  empty <- cleaned[0, ]
  out <- summarize_defensive_stats_by_team_season(empty)
  expect_s3_class(out, "tbl_df")
  expect_identical(nrow(out), 0L)
})

# -----------------------------------------------------------------------------
# summarize_defensive_stats_by_team_weekly()
# -----------------------------------------------------------------------------
test_that("summarize_defensive_stats_by_team_weekly: grouping shape and sums", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 1000) |>
    normalize_raw_for_process()
  cleaned <- process_defensive_player_stats(raw)
  team_weekly <- summarize_defensive_stats_by_team_weekly(cleaned)
  
  expect_s3_class(team_weekly, "tbl_df")
  
  if (all(c("team", "season", "week") %in% names(cleaned)) &&
      all(c("team", "season", "week") %in% names(team_weekly))) {
    expect_equal(
      nrow(dplyr::distinct(team_weekly, team, season, week)),
      nrow(dplyr::distinct(cleaned, team, season, week))
    )
    
    stat <- intersect(pick_stat_cols(cleaned),
                      names(dplyr::select(team_weekly, where(is.numeric)))) |>
      purrr::pluck(1, .default = NA_character_)
    skip_if(is.na(stat), "No common numeric stat to validate")
    
    sample_groups <- dplyr::distinct(cleaned, team, season, week) |>
      dplyr::slice_head(n = 5)
    check_tbl <- sample_groups |>
      dplyr::rowwise() |>
      dplyr::mutate(
        weekly_sum = sum(cleaned[[stat]][cleaned$team == .data$team &
                                           cleaned$season == .data$season &
                                           cleaned$week == .data$week], na.rm = TRUE),
        agg_val = {
          v <- team_weekly[[stat]][team_weekly$team == .data$team &
                                     team_weekly$season == .data$season &
                                     team_weekly$week == .data$week]
          if (length(v)) v[[1]] else NA_real_
        }
      ) |>
      dplyr::ungroup()
    
    expect_equal(check_tbl$agg_val, check_tbl$weekly_sum, tolerance = 1e-8)
  }
})

test_that("summarize_defensive_stats_by_team_weekly: zero-row input ok", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 50) |>
    normalize_raw_for_process()
  cleaned <- process_defensive_player_stats(raw)
  empty <- cleaned[0, ]
  out <- summarize_defensive_stats_by_team_weekly(empty)
  expect_s3_class(out, "tbl_df")
  expect_identical(nrow(out), 0L)
})

# -----------------------------------------------------------------------------
# summarize_defensive_stats_by_player()
# -----------------------------------------------------------------------------
test_that("summarize_defensive_stats_by_player: grouping shape and sums", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 1000) |>
    normalize_raw_for_process()
  cleaned <- process_defensive_player_stats(raw)
  career <- summarize_defensive_stats_by_player(cleaned)
  
  expect_s3_class(career, "tbl_df")
  
  if ("player_id" %in% names(cleaned) && "player_id" %in% names(career)) {
    expect_equal(
      nrow(dplyr::distinct(career, player_id)),
      nrow(dplyr::distinct(cleaned, player_id))
    )
    
    stat <- intersect(pick_stat_cols(cleaned),
                      names(dplyr::select(career, where(is.numeric)))) |>
      purrr::pluck(1, .default = NA_character_)
    skip_if(is.na(stat), "No common numeric stat to validate")
    
    sample_players <- dplyr::distinct(cleaned, player_id) |>
      dplyr::slice_head(n = 5)
    check_tbl <- sample_players |>
      dplyr::rowwise() |>
      dplyr::mutate(
        weekly_sum = sum(cleaned[[stat]][cleaned$player_id == .data$player_id], na.rm = TRUE),
        agg_val = {
          v <- career[[stat]][career$player_id == .data$player_id]
          if (length(v)) v[[1]] else NA_real_
        }
      ) |>
      dplyr::ungroup()
    
    expect_equal(check_tbl$agg_val, check_tbl$weekly_sum, tolerance = 1e-8)
  }
})

test_that("summarize_defensive_stats_by_player: zero-row input ok", {
  skip_if_not(file.exists(raw_path), "raw def_player_stats.parquet not found")
  raw <- arrow::read_parquet(raw_path) |>
    dplyr::slice_head(n = 50) |>
    normalize_raw_for_process()
  cleaned <- process_defensive_player_stats(raw)
  empty <- cleaned[0, ]
  out <- summarize_defensive_stats_by_player(empty)
  expect_s3_class(out, "tbl_df")
  expect_identical(nrow(out), 0L)
})
