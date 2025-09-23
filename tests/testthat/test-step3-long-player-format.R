# tests/testthat/test-step3-long-player-format.R
# Basic, robust tests for step3_long_player_format functions.
# Calm down, future-me: nothing fancy, just making sure it doesn't fall over.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(arrow)
  library(here)
  library(withr)
})

# Load the functions under test
source(here::here("etl", "R", "step3_sql", "step3_long_player_format_functions.R"))

# ------------------------------------------------------------------------------
# pivot_player_stats_long -------------------------------------------------------
# ------------------------------------------------------------------------------
test_that("pivot_player_stats_long pivots wide → long and flags cumulative", {
  td <- local_tempdir()
  fp <- file.path(td, "weekly_wr.parquet")
  
  wide <- tibble::tibble(
    player_id     = "p1",
    full_name     = "Alice A",
    position      = "WR",
    season        = 2024L,
    season_type   = "REG",
    week          = 1L,
    recent_team   = "SEA",
    opponent_team = "SF",
    receptions                 = 5,
    cumulative_receptions      = 12,
    receiving_yards            = 67
  )
  
  arrow::write_parquet(wide, fp)
  
  out <- pivot_player_stats_long(fp)
  
  expect_true(all(c(
    "player_id","name","position","season","season_type","week",
    "team","opponent","stat_type","stat_name","value") %in% names(out)))
  
  # should produce one row per stat column (3 stats)
  expect_equal(nrow(out), 3L)
  
  # cumulative_ stripped and typed correctly
  cum_row <- out %>% filter(stat_name == "receptions", stat_type == "cumulative")
  base_row <- out %>% filter(stat_name == "receptions", stat_type == "base")
  yards_row <- out %>% filter(stat_name == "receiving_yards", stat_type == "base")
  
  expect_equal(nrow(cum_row), 1)
  expect_equal(nrow(base_row), 1)
  expect_equal(nrow(yards_row), 1)
  expect_equal(cum_row$value, 12)
  expect_equal(base_row$value, 5)
  expect_equal(yards_row$value, 67)
  
  # rename of id columns
  expect_setequal(unique(out$team), "SEA")
  expect_setequal(unique(out$opponent), "SF")
})

test_that("pivot_player_stats_long errors clearly on missing id columns", {
  td <- local_tempdir()
  fp <- file.path(td, "bad.parquet")
  
  bad <- tibble::tibble(
    player_id   = "p1",
    full_name   = "Alice A",
    position    = "WR",
    season      = 2024L,
    season_type = "REG",
    week        = 1L,
    # missing recent_team / opponent_team
    receptions  = 1
  )
  arrow::write_parquet(bad, fp)
  
  expect_error(
    pivot_player_stats_long(fp),
    regexp = "Missing required columns"
  )
})

# ------------------------------------------------------------------------------
# pivot_ngs_player_stats_long ---------------------------------------------------
# ------------------------------------------------------------------------------
test_that("pivot_ngs_player_stats_long prefixes ng_ and joins opponent", {
  td <- local_tempdir()
  fp <- file.path(td, "ngs.parquet")
  
  ngs_wide <- tibble::tibble(
    player_gsis_id  = "pQB",
    full_name       = "Quincy B",
    player_position = "QB",
    team_abbr       = "SEA",
    season          = 2024L,
    season_type     = "REG",
    week            = 2L,
    pass_yards               = 250,
    cumulative_pass_yards    = 400
  )
  arrow::write_parquet(ngs_wide, fp)
  
  # Opponent info by player-week
  opp <- tibble::tibble(
    season = 2024L, season_type = "REG", week = 2L,
    player_id = "pQB", opponent = "SF"
  )
  
  out <- pivot_ngs_player_stats_long(fp, opponent_df = opp)
  
  expect_true(all(c(
    "player_id","name","position","season","season_type","week",
    "team","opponent","stat_type","stat_name","value") %in% names(out)))
  
  # Both stats present, prefixed with ng_
  expect_setequal(unique(out$stat_name), c("ng_pass_yards"))
  expect_setequal(sort(unique(out$stat_type)), c("base","cumulative"))
  expect_setequal(unique(out$opponent), "SF")
  
  # values split correctly
  base_val <- out %>% filter(stat_type == "base", stat_name == "ng_pass_yards") %>% pull(value)
  cum_val  <- out %>% filter(stat_type == "cumulative", stat_name == "ng_pass_yards") %>% pull(value)
  expect_equal(base_val, 250)
  expect_equal(cum_val, 400)
})

# ------------------------------------------------------------------------------
#
