# tests/testthat/test-step2-rosters-process.R
# Basic, robust tests for roster processing. Minimal fuss, maximum charm.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(lubridate)
})

testthat::local_edition(3)

# --- Load functions under test ------------------------------------------------
suppressMessages({
  source(here::here("etl", "R", "step2_process", "step2_rosters_process_functions.R"))
})

# --- Tiny toy roster ----------------------------------------------------------
toy_rosters_raw <- tibble::tibble(
  season       = c(2024L, 2024L, 2024L, 2023L, 2023L),
  gsis_id      = c("A1",  "A2",  NA,     "B1",  "B2"),
  full_name    = c("Alpha One","Alpha Two","Ghost Player","Beta One","Beta Two"),
  first_name   = c("Alpha","Alpha","Ghost","Beta","Beta"),
  last_name    = c("One","Two","Player","One","Two"),
  position     = c("QB","WR","RB","TE", NA_character_),
  team         = c("SEA","SEA","SEA","SF","SF"),
  status       = c("ACT","ACT","ACT","ACT","ACT"),
  birth_date   = as.Date(c("1998-05-10","2000-11-20", NA, "1995-09-15", NA)),
  height       = c(73L, 72L, 71L, 76L, 74L),
  weight       = c(210L,195L,205L,240L, NA_integer_),
  college      = c("UW","WSU","UO","Cal","Stanford"),
  years_exp    = c(3, 2, 1, 5, NA_real_),
  rookie_year  = c(2021L, 2022L, 2024L, 2018L, 2023L),
  entry_year   = c(2021L, 2022L, 2024L, 2018L, 2023L),
  headshot_url = c("u/a1","u/a2","u/ghost","u/b1","u/b2"),
  esb_id       = c("ESB_A1","ESB_A2","ESB_GHOST","ESB_B1","ESB_B2")
)

# ------------------------------------------------------------------------------
# process_rosters
# ------------------------------------------------------------------------------
test_that("process_rosters: filters NA gsis_id, computes age, keeps core fields, and sorts", {
  proc <- process_rosters(toy_rosters_raw)
  
  # NA gsis_id row dropped
  expect_false(any(is.na(proc$player_id)))
  expect_equal(nrow(proc), 4)
  
  # Columns present
  expect_true(all(c(
    "season","player_id","full_name","first_name","last_name","position","team",
    "status","age","height","weight","college","years_exp","rookie_year",
    "entry_year","headshot_url","esb_id"
  ) %in% names(proc)))
  
  # Types
  expect_type(proc$height, "integer")
  expect_type(proc$weight, "integer")
  expect_type(proc$age, "double")
  
  # player_id mirrors gsis_id
  expect_true(all(proc$player_id %in% toy_rosters_raw$gsis_id))
  
  # Age as of Sept 1 season-year: A1=26 (1998-05-10 @ 2024-09-01), A2=23, B1=27, B2=NA
  a1_age <- proc %>% filter(player_id=="A1", season==2024) %>% pull(age)
  a2_age <- proc %>% filter(player_id=="A2", season==2024) %>% pull(age)
  b1_age <- proc %>% filter(player_id=="B1", season==2023) %>% pull(age)
  b2_age <- proc %>% filter(player_id=="B2", season==2023) %>% pull(age)
  expect_equal(a1_age, 26)
  expect_equal(a2_age, 23)
  expect_equal(b1_age, 27)
  expect_true(is.na(b2_age))
  
  # Sorted by season then player_id
  expect_equal(
    proc %>% arrange(season, player_id) %>% pull(player_id),
    proc %>% pull(player_id)
  )
})

# ------------------------------------------------------------------------------
# summarize_rosters_by_team_season
# ------------------------------------------------------------------------------
test_that("summarize_rosters_by_team_season: distinct counts and NA-robust averages", {
  proc <- process_rosters(toy_rosters_raw)
  team_season <- summarize_rosters_by_team_season(proc)
  
  # Rows per team-season
  expect_true(all(c("SEA","SF") %in% team_season$team))
  expect_true(all(c(2023L,2024L) %in% team_season$season))
  
  # SEA 2024: A1 + A2
  sea24 <- team_season %>% filter(season==2024, team=="SEA")
  expect_equal(sea24$n_players, 2)
  expect_equal(sea24$avg_age, (26 + 23)/2)
  expect_equal(sea24$avg_height, (73 + 72)/2)
  expect_equal(sea24$avg_weight, (210 + 195)/2)
  expect_equal(sea24$avg_exp, (3 + 2)/2)
  
  # SF 2023: B1 + B2 (B2 weight NA, years_exp NA -> ignored in means)
  sf23 <- team_season %>% filter(season==2023, team=="SF")
  expect_equal(sf23$n_players, 2)
  expect_equal(sf23$avg_age, mean(c(27, NA), na.rm = TRUE))
  expect_equal(sf23$avg_height, mean(c(76, 74), na.rm = TRUE))
  expect_equal(sf23$avg_weight, mean(c(240, NA), na.rm = TRUE))
  expect_equal(sf23$avg_exp, mean(c(5, NA), na.rm = TRUE))
})

# ------------------------------------------------------------------------------
# summarize_rosters_by_team_position
# ------------------------------------------------------------------------------
test_that("summarize_rosters_by_team_position: excludes NA positions and groups correctly", {
  proc <- process_rosters(toy_rosters_raw)
  by_pos <- summarize_rosters_by_team_position(proc)
  
  # No NA positions
  expect_false(any(is.na(by_pos$position)))
  
  # SEA 2024 QB group (A1 only)
  sea_qb <- by_pos %>% filter(season==2024, team=="SEA", position=="QB")
  expect_equal(sea_qb$n_players, 1)
  expect_equal(sea_qb$avg_age, 26)
  expect_equal(sea_qb$avg_height, 73)
  expect_equal(sea_qb$avg_weight, 210)
  
  # SEA 2024 WR group (A2 only)
  sea_wr <- by_pos %>% filter(season==2024, team=="SEA", position=="WR")
  expect_equal(sea_wr$n_players, 1)
  expect_equal(sea_wr$avg_age, 23)
  
  # SF 2023 TE group (B1 only); B2 is NA position -> excluded
  sf_te <- by_pos %>% filter(season==2023, team=="SF", position=="TE")
  expect_equal(sf_te$n_players, 1)
  expect_equal(sf_te$avg_age, 27)
  expect_equal(sf_te$avg_weight, 240)
})
