# tests/testthat/test-step2-pbp-process.R
# Basic, robust tests for play-by-play processing. Minimal glamour, maximum manners.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(tidyr)
})

testthat::local_edition(3)

# --- Load functions under test ------------------------------------------------
suppressMessages({
  source(here::here("etl", "R", "step2_process", "step2_pbp_process_functions.R"))
})

# --- Tiny, hand-crafted PBP toy set ------------------------------------------
# Gist:
# - One game (G1) with SEA (home) vs SF (away)
# - SEA has two drives: Drive 1 scores a TD, Drive 2 is a three-and-out ending in a punt
# - SF has a token drive (one run) so defense cumuls get a little exercise
toy_pbp_raw <- tibble::tibble(
  # identifiers + game state
  game_id = c("G1","G1","G1", "G1","G1","G1","G1","G1", "G1"),
  play_id = c(10L,20L,30L,  40L,50L,60L,70L,80L, 90L),
  qtr     = c(1L,1L,1L,      2L,2L,2L,2L,2L,  3L),
  down    = c(1L,1L,1L,      1L,3L,4L,1L,1L,  1L),
  ydstogo = c(10L,10L,10L,   10L,5L,10L,10L,10L, 10L),
  yardline_100 = c(75L,63L,35L,   85L,90L,92L,   75L,75L,  70L),
  goal_to_go = c(FALSE,FALSE,FALSE, FALSE,FALSE,FALSE, FALSE,FALSE, FALSE),
  posteam   = c("SEA","SEA","SEA", "SEA","SEA","SEA", "SF","SF","SF"),
  defteam   = c("SF","SF","SF",    "SF","SF","SF",    "SEA","SEA","SEA"),
  home_team = "SEA",
  away_team = "SF",
  
  quarter_seconds_remaining = c(900, 800, 700, 900, 850, 820, 900, 890, 880),
  half_seconds_remaining    = c(1800,1700,1600, 900, 850, 820, 1800,1790,1780),
  game_seconds_remaining    = c(3600,3500,3400, 2700,2650,2620, 1800,1790,1780),
  time      = c("15:00","13:20","11:40","15:00","14:10","13:40","30:00","29:50","29:40"),
  play_clock= c("40","40","40","40","40","40","40","40","40"),
  
  # scoreboard
  home_score = 0L, away_score = 0L,
  posteam_score = c(0L,0L,7L, 7L,7L,7L, 0L,0L,0L),
  defteam_score = c(0L,0L,0L, 0L,0L,0L, 0L,0L,0L),
  total_home_score = 0L, total_away_score = 0L,
  score_differential     = c(0L,0L,0L, 7L,7L,7L, 0L,0L,0L),
  score_differential_post= c(0L,0L,7L, 7L,7L,7L, 0L,0L,0L),
  
  # outcomes + modeling fields
  yards_gained = c(12L, 8L, 15L,  -10L, -2L, 0L, 5L, 0L, 0L),
  epa = c(0.3, 0.05, 3.0,  -0.5, -0.2, 0.0, 0.1, 0.0, 0.0),
  wpa = c(0.01, 0.005, 0.10,  -0.02, -0.01, 0.0, 0.02, 0.0, 0.0),
  wp  = c(0.50,0.51,0.60, 0.62,0.60,0.58, 0.45,0.45,0.45),
  home_wp_post = NA_real_, away_wp_post = NA_real_,
  vegas_wp = NA_real_, vegas_home_wp = NA_real_,
  
  play_type = c("pass","run","pass", "pass","run","punt", "run","pass","pass"),
  desc = "blah",
  success = c(TRUE, TRUE, TRUE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE),
  penalty = FALSE,
  timeout = FALSE,
  aborted_play = FALSE,
  play_deleted = FALSE,
  
  air_yards = c(10, NA, 15, 6, NA, NA, NA, 12, 10),
  yards_after_catch = c(2, NA, 0, 0, NA, NA, NA, 3, 1),
  pass_length = c("short", NA, "short", "short", NA, NA, NA, "deep", "short"),
  pass_location = c("left", NA, "middle","right", NA, NA, NA, "left","right"),
  rush_attempt = c(0L,1L,0L, 0L,1L,0L, 1L,0L,0L),
  pass_attempt = c(1L,0L,1L, 1L,0L,0L, 0L,1L,1L),
  sack = c(0L,0L,0L, 1L,0L,0L, 0L,0L,0L),
  touchdown = c(0L,0L,1L, 0L,0L,0L, 0L,0L,0L),
  interception = 0L,
  
  xpass = c(0.55,0.20,0.60, 0.65,0.10,0.00, 0.20,0.55,0.60),
  pass_oe = c(0.20,0.00,0.30, 0.10,0.00,0.00, 0.00,0.10,0.10),
  
  series = c(1L,2L,3L,  1L,2L,3L,  1L,2L,3L),
  series_result = c("First Down","First Down","Touchdown", "First Down","First Down","Punt",
                    "First Down","First Down","First Down"),
  drive = c(1L,1L,1L, 2L,2L,2L, 1L,1L,1L),
  drive_play_count = c(1L,2L,3L, 1L,2L,3L, 1L,2L,3L),
  drive_ended_with_score = c(0L,0L,1L, 0L,0L,0L, 0L,0L,0L),
  drive_time_of_possession = c("01:30", NA, NA, "00:50", NA, NA, NA, NA, NA)
) %>%
  # add two junk rows: one no_play and one NA play_type (must be dropped)
  bind_rows(
    tibble(
      game_id="G1", play_id=5L, qtr=1L, down=1L, ydstogo=10L, yardline_100=70L,
      goal_to_go=FALSE, posteam="SEA", defteam="SF", home_team="SEA", away_team="SF",
      quarter_seconds_remaining=850, half_seconds_remaining=1700, game_seconds_remaining=3500,
      time="14:10", play_clock="40", home_score=0L, away_score=0L,
      posteam_score=0L, defteam_score=0L, total_home_score=0L, total_away_score=0L,
      score_differential=0L, score_differential_post=0L,
      yards_gained=0L, epa=0, wpa=0, wp=0.5, home_wp_post=NA_real_, away_wp_post=NA_real_,
      vegas_wp=NA_real_, vegas_home_wp=NA_real_,
      play_type="no_play", desc="penalty no play", success=FALSE, penalty=TRUE, timeout=FALSE,
      aborted_play=FALSE, play_deleted=FALSE, air_yards=NA_real_, yards_after_catch=NA_real_,
      pass_length=NA_character_, pass_location=NA_character_,
      rush_attempt=0L, pass_attempt=0L, sack=0L, touchdown=0L, interception=0L,
      xpass=0.3, pass_oe=0,
      series=1L, series_result="Penalty", drive=1L, drive_play_count=1L, drive_ended_with_score=0L,
      drive_time_of_possession=NA_character_
    ),
    tibble(
      game_id="G1", play_id=6L, qtr=1L, down=1L, ydstogo=10L, yardline_100=70L,
      goal_to_go=FALSE, posteam="SEA", defteam="SF", home_team="SEA", away_team="SF",
      quarter_seconds_remaining=840, half_seconds_remaining=1680, game_seconds_remaining=3480,
      time="14:00", play_clock="40", home_score=0L, away_score=0L,
      posteam_score=0L, defteam_score=0L, total_home_score=0L, total_away_score=0L,
      score_differential=0L, score_differential_post=0L,
      yards_gained=0L, epa=0, wpa=0, wp=0.5, home_wp_post=NA_real_, away_wp_post=NA_real_,
      vegas_wp=NA_real_, vegas_home_wp=NA_real_,
      play_type=NA_character_, desc="missing", success=FALSE, penalty=FALSE, timeout=FALSE,
      aborted_play=FALSE, play_deleted=FALSE, air_yards=NA_real_, yards_after_catch=NA_real_,
      pass_length=NA_character_, pass_location=NA_character_,
      rush_attempt=0L, pass_attempt=0L, sack=0L, touchdown=0L, interception=0L,
      xpass=0.3, pass_oe=0,
      series=1L, series_result="Unknown", drive=1L, drive_play_count=1L, drive_ended_with_score=0L,
      drive_time_of_possession=NA_character_
    )
  )

# ------------------------------------------------------------------------------
# clean_pbp_data
# ------------------------------------------------------------------------------
test_that("clean_pbp_data: filters junk rows and standardises types", {
  cln <- clean_pbp_data(toy_pbp_raw)
  
  # no 'no_play' or NA play_type
  expect_false(any(is.na(cln$play_type)))
  expect_false(any(cln$play_type == "no_play"))
  
  # types
  expect_type(cln$game_id, "character")
  expect_type(cln$play_id, "integer")
  expect_type(cln$qtr, "integer")
  expect_type(cln$goal_to_go, "logical")
  expect_type(cln$epa, "double")
  expect_type(cln$pass_attempt, "logical")
  expect_type(cln$rush_attempt, "logical")
  expect_type(cln$sack, "logical")
  expect_type(cln$touchdown, "logical")
  
  # first SEA play: pass attempt TRUE, rush FALSE
  sea1 <- cln %>% filter(game_id=="G1", posteam=="SEA") %>% arrange(play_id) %>% slice(1)
  expect_true(sea1$pass_attempt)
  expect_false(sea1$rush_attempt)
})

# ------------------------------------------------------------------------------
# add_team_cumulative_stats
# ------------------------------------------------------------------------------
test_that("add_team_cumulative_stats: cum tallies grow as expected", {
  cln <- clean_pbp_data(toy_pbp_raw)
  off <- add_team_cumulative_stats(cln)
  
  sea <- off %>% filter(posteam=="SEA") %>% arrange(play_id)
  # SEA has 6 plays after cleaning (TD drive of 3 + three-and-out of 3)
  expect_equal(max(sea$cum_play_offense), 6)
  # After TD, cum TDs == 1; by end still 1
  expect_equal(sea %>% filter(play_id==30) %>% pull(cum_td_offense), 1)
  expect_equal(sea %>% slice_tail(n=1) %>% pull(cum_td_offense), 1)
  # Pass attempts cum (plays 10,30,40 are passes)
  expect_equal(sea %>% slice_tail(n=1) %>% pull(cum_pass_attempts), 3)
  # Ratio is computed from cum counts
  last <- sea %>% slice_tail(n=1)
  expect_equal(
    last$run_pass_ratio,
    last$cum_rush_attempts / (last$cum_pass_attempts + 1e-5)
  )
})

# ------------------------------------------------------------------------------
# add_defense_cumulative_stats
# ------------------------------------------------------------------------------
test_that("add_defense_cumulative_stats: defense tallies reflect allowed TDs", {
  cln <- clean_pbp_data(toy_pbp_raw)
  def <- add_defense_cumulative_stats(cln)
  
  sf_def <- def %>% filter(defteam=="SF") %>% arrange(play_id)
  # By SEA's TD play, SF has allowed 1 TD
  expect_equal(sf_def %>% filter(play_id==30) %>% pull(cum_td_allowed), 1)
})

# ------------------------------------------------------------------------------
# add_situational_features
# ------------------------------------------------------------------------------
test_that("add_situational_features: distances, zones, leverage, and flags", {
  cln <- clean_pbp_data(toy_pbp_raw)
  # Nudge one play to be late & close and under 2:00 in the quarter
  cln2 <- cln %>%
    mutate(
      qtr = ifelse(play_id==30, 4L, qtr),
      game_seconds_remaining = ifelse(play_id==30, 500, game_seconds_remaining),
      quarter_seconds_remaining = ifelse(play_id==30, 100, quarter_seconds_remaining)
    )
  feat <- add_situational_features(cln2)
  
  p10 <- feat %>% filter(play_id==10) %>% slice(1)      # 1st play: 1&10 @ own 25
  p30 <- feat %>% filter(play_id==30) %>% slice(1)      # TD play moved to Q4 with <=2:00
  
  # distance buckets
  expect_equal(p10$distance_cat, factor("long", levels=c("short","medium","long","very_long")))
  
  # field zones
  expect_equal((feat %>% filter(play_id==40) %>% pull(field_zone))[[1]],
               factor("backed_up", levels=c("backed_up","midfield","red_zone")))
  expect_equal((feat %>% filter(play_id==30) %>% pull(field_zone))[[1]],
               factor("midfield", levels=c("backed_up","midfield","red_zone")))
  # FG math at opp 35 => 52 yards, in range
  expect_equal((feat %>% filter(play_id==30) %>% pull(estimated_fg_distance))[[1]], 52)
  expect_true((feat %>% filter(play_id==30) %>% pull(in_fg_range))[[1]])
  
  # downs
  expect_false(p10$is_third_down)
  expect_false(p10$is_fourth_down)
  
  # possession_home and score state
  expect_true(p10$possession_home)
  expect_equal(p10$score_state, factor("tied", levels=c("trailing","tied","leading")))
  
  # dropback + explosive
  p40 <- feat %>% filter(play_id==40) %>% slice(1)  # pass + sack
  expect_true(p40$dropback)
  expect_true(p30$explosive_play)  # 15-yard pass
  
  # points from score delta on TD play (0 -> 7)
  expect_equal(p30$posteam_points, 7)
  expect_equal(p30$defteam_points, 0)
  
  # two-minute + late & close
  expect_true(p30$two_min_warning_half)
  expect_true(p30$late_and_close)
})

# ------------------------------------------------------------------------------
# derive_team_rate_features
# ------------------------------------------------------------------------------
test_that("derive_team_rate_features: stable divisions over cumulative tallies", {
  cln <- clean_pbp_data(toy_pbp_raw)
  off <- add_team_cumulative_stats(cln)
  def <- add_defense_cumulative_stats(off)
  rates <- derive_team_rate_features(def)
  
  sea_p3 <- rates %>% filter(posteam=="SEA", play_id==30) %>% slice(1)
  expect_equal(
    sea_p3$off_epa_per_play,
    sea_p3$cum_epa_offense / sea_p3$cum_play_offense
  )
  expect_equal(
    sea_p3$off_pass_rate,
    sea_p3$cum_pass_attempts / sea_p3$cum_play_offense
  )
})

# ------------------------------------------------------------------------------
# summarise_team_game_features
# ------------------------------------------------------------------------------
test_that("summarise_team_game_features: drives, quality, 3-and-outs, and early-downs", {
  cln <- clean_pbp_data(toy_pbp_raw)
  game <- summarise_team_game_features(cln, qd_yardline = 40L, qd_min_plays = 4L, qd_min_yards = 20L)
  
  sea <- game %>% filter(game_id=="G1", posteam=="SEA") %>% slice(1)
  
  # Drives and quality
  expect_equal(sea$drives, 2)
  expect_equal(sea$quality_drives, 1)
  expect_equal(sea$quality_drive_rate, 0.5)
  
  # Scoring + efficiency
  expect_equal(sea$points_per_drive, 7/2)
  expect_equal(sea$plays_total, 6)
  expect_equal(sea$epa_total, 2.65, tolerance = 1e-9)
  expect_equal(sea$epa_per_play, 2.65/6, tolerance = 1e-9)
  expect_equal(sea$success_rate, 3/6, tolerance = 1e-9)
  expect_equal(sea$explosive_rate, 1/6, tolerance = 1e-9)
  expect_equal(sea$pass_rate, 3/6, tolerance = 1e-9)
  expect_equal(sea$rush_rate, 2/6, tolerance = 1e-9)
  
  # Per-drive events
  expect_equal(sea$sacks_per_drive, 1/2)
  expect_equal(sea$interceptions_per_drive, 0)
  expect_equal(sea$td_rate_per_drive, 1/2)
  
  # Field position + time
  expect_equal(sea$avg_start_yardline_100, (75+85)/2)
  expect_equal(sea$avg_drive_depth_into_opp, (40+0)/2)
  expect_equal(sea$avg_drive_plays, 3)
  expect_equal(sea$avg_drive_time_seconds, (90+50)/2)
  
  # Red zone + 3-and-out bundle
  expect_equal(sea$red_zone_trips, 0)
  expect_equal(sea$red_zone_trip_rate, 0)
  expect_equal(sea$three_and_outs, 1)
  expect_equal(sea$three_and_out_rate, 0.5)
  expect_equal(sea$short_turnovers_leq3, 0)
  expect_equal(sea$three_and_out_or_short_turnover, 1)
  expect_equal(sea$three_and_out_or_short_turnover_rate, 0.5)
  
  # WPA total
  expect_equal(sea$wpa_total, 0.085, tolerance = 1e-9)
  
  # Early-down metrics (downs 1–2 only)
  expect_equal(sea$early_plays, 4)
  expect_equal(sea$early_epa_total, 2.85, tolerance = 1e-9)
  expect_equal(sea$early_epa_per_play, 2.85/4, tolerance = 1e-9)
  expect_equal(sea$early_success_rate, 3/4, tolerance = 1e-9)
  expect_equal(sea$pass_oe_mean, 0.6/6, tolerance = 1e-9)
})
