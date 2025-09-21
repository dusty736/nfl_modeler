# tests/testthat/test-step2-schedule-process.R
# Tests for schedule processing. Basic, tidy, and just a smidge charming.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(lubridate)
})

testthat::local_edition(3)

# --- Load functions under test ------------------------------------------------
suppressMessages({
  source(here::here("etl", "R", "step2_process", "step2_schedule_process_functions.R"))
})

# --- Tiny toy schedule --------------------------------------------------------
toy_schedule <- tibble::tibble(
  game_id   = c("G1", "G2", "G3", "G4"),
  season    = c(2024L, 2024L, 2024L, 2024L),
  week      = c(1L, 1L, 1L, 1L),
  game_type = c("REG", "REG", "POST", "WC"),
  gameday   = as.Date(c("2024-09-08", "2024-09-08", "2025-01-05", "2025-01-05")),
  gametime  = c("16:25", "18:05", "20:15", "21:00"),
  weekday   = c("Sun","Sun","Sun","Sun"),
  
  home_team = c("sea","ne","sf","sea"),
  away_team = c("sf","nyj","dal","lar"),
  home_score = c(21L, 24L, 17L, 27L),
  away_score = c(17L, 24L, 20L, 23L),
  overtime   = c(FALSE, TRUE, FALSE, FALSE),
  
  # Betting lines
  spread_line = c(-3, 0, 2.5, -1.5),
  total_line  = c(43.5, 41, 42, 47),
  away_moneyline = c(+140L, +100L, +120L, +110L),
  home_moneyline = c(-160L, +100L, -140L, -120L),
  away_spread_odds = c(-110L, -110L, -110L, -110L),
  home_spread_odds = c(-110L, -110L, -110L, -110L),
  under_odds = c(-110L, -110L, -110L, -110L),
  over_odds  = c(-110L, -110L, -110L, -110L),
  
  # Venue
  stadium = c("Lumen","Gillette","Levi's","Lumen"),
  stadium_id = c("ST1","ST2","ST3","ST1"),
  roof = c("outdoors","outdoors","outdoors","outdoors"),
  surface = c("artificial","artificial","grass","artificial"),
  temp = c(65L, 70L, 60L, 50L),
  wind = c(5L, 10L, 7L, 12L),
  
  referee = c("Ref A","Ref B","Ref C","Ref D"),
  
  # People
  home_qb_id = c("QB_SEA","QB_NE","QB_SF","QB_SEA"),
  away_qb_id = c("QB_SF","QB_NYJ","QB_DAL","QB_LAR"),
  home_qb_name = c("A Seahawk","A Patriot","A Niner","A Seahawk"),
  away_qb_name = c("A Niner","A Jet","A Cowboy","A Ram"),
  home_coach = c("Coach SEA","Coach NE","Coach SF","Coach SEA"),
  away_coach = c("Coach SF","Coach NYJ","Coach DAL","Coach LAR"),
  
  div_game = c(FALSE, TRUE, FALSE, TRUE),
  
  # External ids
  old_game_id = c("old1","old2","old3","old4"),
  gsis = c("gs1","gs2","gs3","gs4"),
  nfl_detail_id = c("n1","n2","n3","n4"),
  pfr = c("p1","p2","p3","p4"),
  pff = c("pf1","pf2","pf3","pf4"),
  espn = c("e1","e2","e3","e4"),
  ftn  = c("f1","f2","f3","f4")
)

# ------------------------------------------------------------------------------
# clean_schedule_data
# ------------------------------------------------------------------------------
test_that("clean_schedule_data: types, uppercase teams, result & favored team", {
  cln <- clean_schedule_data(toy_schedule)
  
  # POSIXct kickoff in UTC
  expect_s3_class(cln$kickoff, "POSIXct")
  expect_identical(attr(cln$kickoff, "tzone"), "UTC")
  
  # Teams uppercased
  expect_true(all(cln$home_team == toupper(toy_schedule$home_team)))
  expect_true(all(cln$away_team == toupper(toy_schedule$away_team)))
  
  # Results
  g1 <- cln %>% filter(game_id=="G1")
  g2 <- cln %>% filter(game_id=="G2")
  expect_equal(g1$result, "HOME")
  expect_equal(g2$result, "TIE")
  
  # Favored team: negative -> home; positive -> away; zero -> EVEN
  expect_equal((cln %>% filter(game_id=="G1"))$favored_team, "SEA")
  expect_equal((cln %>% filter(game_id=="G3"))$favored_team, "DAL")
  expect_equal((cln %>% filter(game_id=="G2"))$favored_team, "EVEN")
  
  # Overtime logical
  expect_type(cln$overtime, "logical")
})

# ------------------------------------------------------------------------------
# parse_kickoff_time
# ------------------------------------------------------------------------------
test_that("parse_kickoff_time: parses date + time as UTC POSIXct", {
  ts <- parse_kickoff_time(as.Date("2024-09-08"), "16:25")
  expect_s3_class(ts, "POSIXct")
  expect_identical(format(ts, tz = "UTC"), "2024-09-08 16:25:00")
})

# ------------------------------------------------------------------------------
# compute_result
# ------------------------------------------------------------------------------
test_that("compute_result: HOME/AWAY/TIE and NA handling", {
  expect_equal(compute_result(21,17), "HOME")
  expect_equal(compute_result(17,21), "AWAY")
  expect_equal(compute_result(24,24), "TIE")
  expect_true(is.na(compute_result(NA, 3)))
  expect_true(is.na(compute_result(7, NA)))
})

# ------------------------------------------------------------------------------
# compute_favored_team
# ------------------------------------------------------------------------------
test_that("compute_favored_team: sign of spread selects team; zero -> EVEN; NA -> NA", {
  expect_equal(compute_favored_team("SEA","SF",-2.5), "SEA")
  expect_equal(compute_favored_team("SEA","SF", 3.0), "SF")
  expect_equal(compute_favored_team("SEA","SF", 0), "EVEN")
  expect_true(is.na(compute_favored_team("SEA","SF", NA_real_)))
})

# ------------------------------------------------------------------------------
# get_weekly_season_table
# ------------------------------------------------------------------------------
test_that("get_weekly_season_table: produces team-game rows and week labels", {
  wk <- get_weekly_season_table(toy_schedule)
  
  # Two rows per REG game (home+away), so 2*2 = 4 REG rows in Week 1
  reg_rows <- wk %>% filter(season==2024, season_type=="REG", week==1)
  expect_equal(nrow(reg_rows), 4)
  
  # Wins entering Week 1 are zero (team ids are lowercased in raw schedule)
  sea_reg <- reg_rows %>% filter(team_id=="sea")
  expect_equal(sea_reg$wins_entering, 0)
  
  # POST week labeling uses Wild Card for week==1
  post_rows <- wk %>% filter(season_type=="POST", week==1)
  expect_true(nrow(post_rows) > 0)
  expect_true(all(post_rows$week_label == "Wild Card"))
})

# ------------------------------------------------------------------------------
# summarize_season_team_results
# ------------------------------------------------------------------------------
test_that("summarize_season_team_results: REG totals + playoff appearance & round", {
  season_sum <- summarize_season_team_results(toy_schedule)
  
  # SEA: 1 REG win, made playoffs via WC game, round WC
  sea <- season_sum %>% filter(season==2024, team_id=="sea")
  expect_equal(sea$wins, 1)
  expect_equal(sea$losses, 0)
  expect_equal(sea$ties, 0)
  expect_equal(sea$points_scored, 21)
  expect_equal(sea$points_allowed, 17)
  expect_true(sea$made_playoffs)
  expect_equal(sea$postseason_round, "WC")
  
  # NE & NYJ: tie merchants, no playoffs
  ne  <- season_sum %>% filter(season==2024, team_id=="ne")
  nyj <- season_sum %>% filter(season==2024, team_id=="nyj")
  expect_equal(ne$ties, 1)
  expect_equal(nyj$ties, 1)
  expect_false(ne$made_playoffs)
  expect_false(nyj$made_playoffs)
  expect_equal(ne$postseason_round, "None")
  
  # Note: Teams appearing ONLY in postseason (e.g., "lar" here) are not in the REG summary.
  # If you want to assert on LAR, add a REG game for them in toy data first. We’re not wizards.
})
