# tests/testthat/test-step2-nextgen-stats-functions.R
# --- COMPLETE FILE (rectangular fixture + assertions) ---

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(here)
})

testthat::local_edition(3)

# ------------------------------------------------------------------
# Source functions under test
# ------------------------------------------------------------------
fpath <- here::here("etl", "R", "step2_process", "step2_nextgen_stats_process_functions.R")
stopifnot(file.exists(fpath))
source(fpath)

# ------------------------------------------------------------------
# Rectangular fixture covering REG + POST, week 0, and NAs for max fields
# ------------------------------------------------------------------
raw_ngs <- tibble::tribble(
  ~season, ~season_type, ~week, ~player_gsis_id, ~player_display_name, ~player_position, ~team_abbr,
  ~avg_time_to_throw, ~avg_completed_air_yards, ~avg_intended_air_yards, ~avg_air_yards_differential,
  ~aggressiveness, ~max_completed_air_distance, ~avg_air_yards_to_sticks,
  ~attempts, ~pass_yards, ~pass_touchdowns, ~interceptions, ~passer_rating,
  ~completions, ~completion_percentage, ~expected_completion_percentage, ~completion_percentage_above_expectation,
  ~avg_air_distance, ~max_air_distance,
  
  # A, REG, week 0 (dropped later)
  2023L, "REG",  0L, "A", "Alpha A", "QB", "SEA",
  2.8, 6.0, 7.0, -1.0,
  15, NA, 1.0,
  0, 0, 0, 0, 0,
  0, 0, 0, 0.0,
  NA, NA,
  
  # A, REG, week 1
  2023L, "REG",  1L, "A", "Alpha A", "QB", "SEA",
  2.7, 6.5, 7.2, -0.7,
  16, NA, 1.1,
  30, 220, 2, 1, 0,
  18, 60, 58, 2.0,
  6.8, NA,
  
  # A, REG, week 2
  2023L, "REG",  2L, "A", "Alpha A", "QB", "SEA",
  2.9, 6.2, 7.1, -0.9,
  14, NA, 1.2,
  28, 200, 1, 0, 0,
  17, 61, 59, 2.0,
  6.6, NA,
  
  # A, POST, week 1
  2023L, "POST", 1L, "A", "Alpha A", "QB", "SEA",
  2.8, 6.0, 7.0, -1.0,
  13, 36, 1.0,
  25, 180, 1, 0, 0,
  16, 64, 60, 4.0,
  6.9, 45,
  
  # B, REG, week 1
  2023L, "REG",  1L, "B", "Bravo B", "QB", "KC",
  2.5, 5.0, 6.3, -1.3,
  12, 38, 0.8,
  20, 150, 1, 1, 0,
  12, 55, 54, 1.0,
  6.1, 40
)

# After process(), expected columns
proc_cols <- c(
  "season","season_type","week","player_gsis_id","full_name","player_position","team_abbr",
  "avg_time_to_throw","avg_completed_air_yards","avg_intended_air_yards","avg_air_yards_differential",
  "aggressiveness","max_completed_air_distance","avg_air_yards_to_sticks",
  "attempts","pass_yards","pass_touchdowns","interceptions","passer_rating",
  "completions","completion_percentage","expected_completion_percentage","completion_percentage_above_expectation",
  "avg_air_distance","max_air_distance"
)

# ------------------------------------------------------------------
# process_nextgen_stats()
# ------------------------------------------------------------------
test_that("process_nextgen_stats: drops week 0, selects/renames expected columns", {
  got <- process_nextgen_stats(raw_ngs)
  expect_identical(names(got), proc_cols)
  expect_true(all(got$week > 0))
  expect_true(all(got$full_name %in% c("Alpha A","Bravo B")))
})

# ------------------------------------------------------------------
# aggregate_nextgen_by_season()  (REG only)
# ------------------------------------------------------------------
test_that("aggregate_nextgen_by_season: REG-only totals, per-game avgs, and max -Inf -> NA", {
  clean <- process_nextgen_stats(raw_ngs)
  by_season <- aggregate_nextgen_by_season(clean)
  
  # A REG: weeks 1-2 only
  a <- by_season %>% filter(player_gsis_id=="A", season==2023)
  expect_equal(nrow(a), 1L)
  expect_equal(a$games_played, 2)
  expect_equal(a$attempts, 30 + 28)
  expect_equal(a$completions, 18 + 17)
  expect_equal(a$pass_yards, 220 + 200)
  expect_equal(a$pass_touchdowns, 2 + 1)
  expect_equal(a$interceptions, 1 + 0)
  
  # Per-game avgs
  expect_equal(a$avg_attempts, round((30+28)/2, 1))
  expect_equal(a$avg_completions, round((18+17)/2, 1))
  expect_equal(a$avg_pass_yards, round((220+200)/2, 1))
  expect_equal(a$avg_pass_touchdowns, round((2+1)/2, 2))
  expect_equal(a$avg_interceptions, round((1+0)/2, 2))
  
  # max_* should be NA (all NA in REG rows for A)
  expect_true(is.na(a$max_completed_air_distance))
  expect_true(is.na(a$max_air_distance))
  
  # B REG: single week
  b <- by_season %>% filter(player_gsis_id=="B", season==2023)
  expect_equal(b$games_played, 1)
  expect_equal(b$attempts, 20)
  expect_equal(b$max_completed_air_distance, 38)
  expect_equal(b$max_air_distance, 40)
})

# ------------------------------------------------------------------
# aggregate_nextgen_postseason()  (POST per season)
# ------------------------------------------------------------------
test_that("aggregate_nextgen_postseason: groups by player-season, uses n_distinct(week)", {
  clean <- process_nextgen_stats(raw_ngs)
  post <- aggregate_nextgen_postseason(clean)
  
  # Only A has POST in fixture, 2023 week 1
  a <- post %>% filter(player_gsis_id=="A", season==2023)
  expect_equal(nrow(a), 1L)
  expect_equal(a$games_played, 1)       # n_distinct(week)
  expect_equal(a$attempts, 25)
  expect_equal(a$pass_touchdowns, 1)
  expect_equal(a$interceptions, 0)
  expect_equal(a$max_completed_air_distance, 36)
  expect_equal(a$max_air_distance, 45)
})

# ------------------------------------------------------------------
# aggregate_nextgen_by_career()  (all seasons/types)
# ------------------------------------------------------------------
test_that("aggregate_nextgen_by_career: career totals and per-game avgs across REG+POST", {
  clean <- process_nextgen_stats(raw_ngs)
  car <- aggregate_nextgen_by_career(clean)
  
  # A: REG (2) + POST (1) = 3 distinct (season,week)
  a <- car %>% filter(player_gsis_id=="A")
  expect_equal(a$games_played, 3)
  expect_equal(a$attempts, 30+28+25)
  expect_equal(a$completions, 18+17+16)
  expect_equal(a$pass_yards, 220+200+180)
  expect_equal(a$pass_touchdowns, 2+1+1)
  expect_equal(a$interceptions, 1+0+0)
  
  expect_equal(a$avg_attempts, round((30+28+25)/3, 1))
  expect_equal(a$avg_completions, round((18+17+16)/3, 1))
  expect_equal(a$avg_pass_yards, round((220+200+180)/3, 1))
  
  # max_* across all A rows includes POST values
  expect_equal(a$max_completed_air_distance, 36)
  expect_equal(a$max_air_distance, 45)
})

# ------------------------------------------------------------------
# compute_cumulative_nextgen_stats()
# ------------------------------------------------------------------
test_that("compute_cumulative_nextgen_stats: cumulative sums increase and order is correct", {
  clean <- process_nextgen_stats(raw_ngs) %>%
    filter(player_gsis_id=="A", season==2023, season_type=="REG") %>%
    arrange(week)
  
  cum <- compute_cumulative_nextgen_stats(clean)
  
  expect_equal(cum$cumulative_attempts, c(30, 30+28))
  expect_equal(cum$cumulative_completions, c(18, 18+17))
  expect_equal(cum$cumulative_pass_yards, c(220, 220+200))
  expect_true(all(cum$week == sort(cum$week)))
})
