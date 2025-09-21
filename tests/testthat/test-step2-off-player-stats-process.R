# tests/testthat/test-step2-off-player-stats-process.R
# Basic, robust tests for offensive player processing (QB/RB/WR/TE + team agg).

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
})

testthat::local_edition(3)

# --- Load functions under test ------------------------------------------------
suppressMessages({
  source(here::here("etl", "R", "step2_process", "step2_off_player_stats_process_functions.R"))
})

# --- Minimal toy data covering QB/RB/WR/TE across two weeks -------------------
toy_off <- tibble::tibble(
  # identifiers
  season = c(2024,2024,2024, 2024,                       2024,2024,   2024,          2024,2024, 2024),
  week   = c(1,   2,   1,     1,                          1,   2,      1,             1,   1,    1),
  season_type = c("REG","REG","REG","REG",                "REG","REG","REG",          "REG","POST","REG"),
  player_id = c("qb1","qb1","qb2","qb3",                  "rb1","rb1","rb2",          "wr1","wr1","te1"),
  player_display_name = c("QB One","QB One","QB Two","QB Three",
                          "RB One","RB One","RB Two",     "WR One","WR One P","TE One"),
  position_group = c("QB","QB","QB","QB",                 "RB","RB","RB",             "WR","WR","TE"),
  position       = c("QB","QB","QB","QB",                 "RB","RB","RB",             "WR","WR","TE"),
  recent_team    = c("SEA","SEA","SF","SEA",              "SEA","SEA","SF",           "SEA","SEA","SEA"),
  opponent_team  = c("SF","LAR","SEA","DAL",              "SF","LAR","SEA",           "SF","SF","SF"),
  
  # Passing (mostly for QBs)
  completions = c(18,15,NA,NA,  0,0,0, 0,0,0),
  attempts    = c(30,28,NA,NA,  0,0,0, 0,0,0),
  passing_yards = c(220,200,NA,NA,  0,0,0, 0,0,0),
  passing_tds   = c(2,1,NA,NA,  0,0,0, 0,0,0),
  interceptions = c(1,0,NA,NA,  0,0,0, 0,0,0),
  sacks         = c(2,1,NA,NA,  0,0,0, 0,0,0),
  sack_yards    = c(10,7,NA,NA,  0,0,0, 0,0,0),
  sack_fumbles       = c(0,0,NA,NA,  0,0,0, 0,0,0),
  sack_fumbles_lost  = c(0,0,NA,NA,  0,0,0, 0,0,0),
  passing_air_yards          = c(150,120,NA,NA,  0,0,0, 0,0,0),
  passing_yards_after_catch  = c(70,80,NA,NA,   0,0,0, 0,0,0),
  passing_first_downs        = c(12,10,NA,NA,  0,0,0, 0,0,0),
  passing_epa                = c(1.5,0.8,NA,NA, 0,0,0, 0,0,0),
  passing_2pt_conversions    = c(0,0,NA,NA,    0,0,0, 0,0,0),
  pacr   = c(0.9,0.8,NA,NA,  NA,NA,NA,  NA,NA,NA),
  dakota = c(0.2,0.1,NA,NA,  NA,NA,NA,  NA,NA,NA),
  
  # Rushing
  carries        = c(4,3,2,NA,  10,8,5,  1,1,1),
  rushing_yards  = c(20,10,5,NA, 50,40,15, 5,4,3),
  rushing_tds    = c(0,1,0,NA,  1,0,0,  0,0,0),
  rushing_fumbles      = c(0,0,0,NA, 0,0,0, 0,0,0),
  rushing_fumbles_lost = c(0,0,0,NA, 0,0,0, 0,0,0),
  rushing_first_downs  = c(1,1,0,NA, 2,1,0, 0,0,0),
  rushing_epa          = c(0.3,0.2,0.1,NA, 0.5,0.3,0.1, 0.05,0.04,0.03),
  rushing_2pt_conversions = c(0,0,0,NA, 0,0,0, 0,0,0),
  
  # Receiving
  targets    = c(NA,NA,NA,NA,  3,2,1,  8,6,4),
  receptions = c(NA,NA,NA,NA,  2,1,1,  5,4,3),
  receiving_yards = c(NA,NA,NA,NA,  15,12,9, 60,40,25),
  receiving_tds   = c(NA,NA,NA,NA,  0,0,0,   1,0,0),
  receiving_fumbles      = c(NA,NA,NA,NA, 0,0,0, 0,0,0),
  receiving_fumbles_lost = c(NA,NA,NA,NA, 0,0,0, 0,0,0),
  receiving_air_yards         = c(NA,NA,NA,NA, 5,4,3, 70,45,15),
  receiving_yards_after_catch = c(NA,NA,NA,NA, 8,7,5, 20,12,9),
  receiving_first_downs       = c(NA,NA,NA,NA, 1,1,0, 3,2,1),
  receiving_epa               = c(NA,NA,NA,NA, 0.2,0.1,0.05, 0.6,0.3,0.2),
  receiving_2pt_conversions   = c(NA,NA,NA,NA, 0,0,0, 0,0,0),
  
  # Advanced + Fantasy (for RB/WR/TE)
  racr          = c(NA,NA,NA,NA, 0.5,0.6,0.4, 0.7,0.65,0.5),
  target_share  = c(NA,NA,NA,NA, 0.10,0.08,0.05, 0.20,0.18,0.12),
  air_yards_share = c(NA,NA,NA,NA, 0.05,0.04,0.03, 0.25,0.22,0.10),
  wopr          = c(NA,NA,NA,NA, 0.20,0.25,0.15, 0.45,0.40,0.28),
  
  fantasy_points     = c(18,17,3,NA, 12,10,6,  15,12,8),
  fantasy_points_ppr = c(18,17,3,NA, 14,11,7,  20,16,11)
)

# --- QB: weekly processing ----------------------------------------------------
test_that("process_qb_stats filters to QBs and computes cumulative correctly", {
  qb <- process_qb_stats(toy_off)
  
  # Only QBs (position retained; position_group is not)
  expect_true(all(qb$position == "QB"))
  # No rows where both attempts and carries were NA (qb3 should drop)
  expect_false(any(is.na(qb$attempts) & is.na(qb$carries)))
  
  # Cumulative checks for qb1
  a <- qb %>% filter(player_id == "qb1") %>% arrange(week)
  expect_equal(a$cumulative_attempts, c(30, 30 + 28))
  expect_equal(a$cumulative_passing_yards, c(220, 220 + 200))
  
  expect_false(dplyr::is_grouped_df(qb))
  expect_true(all(c("cumulative_completions","cumulative_attempts","cumulative_passing_yards") %in% names(qb)))
})

# --- QB: season aggregation ---------------------------------------------------
test_that("aggregate_qb_season_stats sums volumes, averages rates, drops cumulative", {
  qb_week <- process_qb_stats(toy_off)
  qb_season <- aggregate_qb_season_stats(qb_week)
  
  expect_true(all(qb_season %>% count(season, player_id) %>% pull(n) == 1))
  expect_false(any(startsWith(names(qb_season), "cumulative_")))
  
  s <- qb_season %>% filter(player_id == "qb1", season == 2024) %>% slice(1)
  expect_equal(s$attempts, 30 + 28)
  expect_equal(s$completions, 18 + 15)
  expect_equal(s$passing_yards, 220 + 200)
  expect_equal(s$pacr, mean(c(0.9, 0.8)))
  expect_equal(s$dakota, mean(c(0.2, 0.1)))
  expect_equal(s$games_played, 2)
})

# --- QB: career aggregation ---------------------------------------------------
test_that("aggregate_qb_career_stats totals across seasons and counts games as implemented", {
  qb_week <- process_qb_stats(toy_off)
  qb_career <- aggregate_qb_career_stats(qb_week)
  
  expect_true(all(qb_career %>% count(player_id) %>% pull(n) == 1))
  
  c1 <- qb_career %>% filter(player_id == "qb1") %>% slice(1)
  expect_equal(c1$attempts, 30 + 28)
  expect_equal(c1$games_played, dplyr::n_distinct(qb_week %>% filter(player_id=="qb1") %>% transmute(season, week)))
})

# --- RB: weekly processing ----------------------------------------------------
test_that("process_rb_stats filters to RBs and computes cumulative correctly", {
  rb <- process_rb_stats(toy_off)
  expect_true(all(rb$position == "RB"))
  expect_false(any(is.na(rb$carries) & is.na(rb$targets)))
  
  r1 <- rb %>% filter(player_id == "rb1") %>% arrange(week)
  expect_equal(r1$cumulative_carries, c(10, 10 + 8))
  expect_equal(r1$cumulative_receiving_yards, c(15, 15 + 12))
  expect_false(dplyr::is_grouped_df(rb))
})

# --- RB: season aggregation ---------------------------------------------------
test_that("aggregate_rb_season_stats sums and averages as written", {
  rb_week <- process_rb_stats(toy_off)
  rb_season <- aggregate_rb_season_stats(rb_week)
  
  expect_true(all(rb_season %>% count(season, player_id) %>% pull(n) == 1))
  
  s <- rb_season %>% filter(player_id == "rb1", season == 2024) %>% slice(1)
  expect_equal(s$carries, 10 + 8)
  expect_equal(s$receiving_yards, 15 + 12)
  expect_equal(s$fantasy_points, 12 + 10)
  expect_equal(s$racr, mean(c(0.5, 0.6)))
  expect_equal(s$wopr, mean(c(0.20, 0.25)))
  expect_equal(s$games_played, 2)
})

# --- RB: career aggregation ---------------------------------------------------
test_that("aggregate_rb_career_stats totals across seasons", {
  rb_week <- process_rb_stats(toy_off)
  rb_career <- aggregate_rb_career_stats(rb_week)
  
  c1 <- rb_career %>% filter(player_id == "rb1") %>% slice(1)
  expect_equal(c1$carries, 18)
  expect_equal(c1$games_played, dplyr::n_distinct(rb_week %>% filter(player_id=="rb1") %>% transmute(season, week)))
})

# --- WR/TE: weekly processing -------------------------------------------------
test_that("process_receiver_stats works for WR and TE and errors for other group", {
  wr <- process_receiver_stats(toy_off, "WR")
  te <- process_receiver_stats(toy_off, "TE")
  
  expect_true(all(wr$position == "WR"))
  expect_true(all(te$position == "TE"))
  
  # Cumulative columns exist (sample a few)
  expect_true(all(c("cumulative_targets",
                    "cumulative_receptions",
                    "cumulative_receiving_yards",
                    "cumulative_fantasy_points_ppr") %in% names(wr)))
  
  # Monotone cumulative per player-season
  wr_mono <- wr %>%
    group_by(season, player_id) %>%
    summarize(ok = all(diff(cumulative_receptions) >= 0), .groups = "drop")
  expect_true(all(wr_mono$ok))
  
  # Last cumulative equals group total receptions
  wr_tot <- wr %>%
    group_by(season, player_id) %>%
    summarize(last_cum = dplyr::last(cumulative_receptions),
              sum_rec = sum(receptions, na.rm = TRUE), .groups = "drop")
  expect_true(all(wr_tot$last_cum == wr_tot$sum_rec))
  
  expect_error(process_receiver_stats(toy_off, "QB"))
})

# --- WR/TE: season aggregation -----------------------------------------------
test_that("aggregate_receiver_season_stats sums volumes and averages rates", {
  wr_week <- process_receiver_stats(toy_off, "WR")
  wr_season <- aggregate_receiver_season_stats(wr_week)
  
  s <- wr_season %>% filter(player_id=="wr1", season==2024) %>% slice(1)
  expect_equal(s$receptions, 5 + 4)      # REG wk1 + POST wk1 summed
  expect_equal(s$receiving_yards, 60 + 40)
  expect_equal(s$fantasy_points, 15 + 12)
  expect_equal(s$racr, mean(c(0.7, 0.65)))
  
  # games_played counts distinct week numbers only per current implementation
  expect_equal(s$games_played, 1)
})

# --- WR/TE: career aggregation ------------------------------------------------
test_that("aggregate_receiver_career_stats counts games using (season, season_type, week)", {
  wr_week <- process_receiver_stats(toy_off, "WR")
  wr_career <- aggregate_receiver_career_stats(wr_week)
  
  c <- wr_career %>% filter(player_id=="wr1") %>% slice(1)
  expect_equal(c$games_played, 2)  # REG wk1 and POST wk1
  expect_equal(c$receiving_yards, 60 + 40)
})

# --- Team aggregates ----------------------------------------------------------
test_that("aggregate_offense_team_week_stats and _season_stats sum per team and period", {
  tw <- aggregate_offense_team_week_stats(toy_off)
  manual_wk1_sea <- toy_off %>% filter(recent_team=="SEA", week==1) %>% summarize(v=sum(passing_yards, na.rm=TRUE)) %>% pull(v)
  expect_equal(tw %>% filter(recent_team=="SEA", week==1) %>% pull(passing_yards), manual_wk1_sea)
  expect_true(all(tw$games_played == 1))
  
  ts <- aggregate_offense_team_season_stats(toy_off)
  manual_season_sea <- toy_off %>% filter(recent_team=="SEA") %>% summarize(v=sum(passing_yards, na.rm=TRUE)) %>% pull(v)
  expect_equal(ts %>% filter(recent_team=="SEA", season==2024) %>% pull(passing_yards), manual_season_sea)
  
  sea_weeks <- toy_off %>% filter(recent_team=="SEA") %>% distinct(week) %>% nrow()
  expect_equal(ts %>% filter(recent_team=="SEA", season==2024) %>% pull(games_played), sea_weeks)
})

