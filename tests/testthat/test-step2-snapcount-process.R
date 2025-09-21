# tests/testthat/test-step2-snapcount-process.R
# Basic, robust tests for snapcount processing. No jazz hands.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
})

testthat::local_edition(3)

# --- Load functions under test ------------------------------------------------
suppressMessages({
  source(here::here("etl", "R", "step2_process", "step2_snapcount_process_functions.R"))
})

# --- Tiny toy weekly snapcounts ----------------------------------------------
# Columns expected by the functions:
# game_id, season, game_type, week, player, pfr_player_id, position, team, opponent,
# offense_snaps, offense_pct, defense_snaps, defense_pct, st_snaps, st_pct
toy_snaps <- tibble::tibble(
  # A Player: SEA 2024 (2 real games + 1 zero-snap row), SF 2023 (1 game)
  game_id        = c("S24W1SEA","S24W2SEA","S24W3SEA","S23W1SF",
                     "S24W1SEA_DEF","S24W2SEA_DEF","S23W1SEA_DEF"),
  season         = c(2024L, 2024L, 2024L, 2023L, 2024L, 2024L, 2023L),
  game_type      = c("REG","REG","REG","REG","REG","REG","REG"),
  week           = c(1L, 2L, 3L, 1L, 1L, 2L, 1L),
  player         = c("A Player","A Player","A Player","A Player",
                     "B Defender","B Defender","B Defender"),
  pfr_player_id  = c("P1","P1","P1","P1","P2","P2","P2"),
  position       = c("WR","WR","WR","WR","LB","LB","LB"),
  team           = c("SEA","SEA","SEA","SF","SEA","SEA","SEA"),
  opponent       = c("SF","LAR","ARI","SEA","SF","LAR","SF"),
  
  offense_snaps  = c(50, 40, NA, 20, 0, 0, 0),
  offense_pct    = c(0.80, 0.70, NA, 0.30, NA, NA, NA),
  defense_snaps  = c(0, 0, NA, 0, 60, 30, 10),
  defense_pct    = c(NA, NA, NA, NA, 0.90, 0.45, 0.15),
  st_snaps       = c(10, 5, 0, 0, 5, 0, 0),
  st_pct         = c(0.20, 0.10, NA, NA, 0.50, NA, NA)
)

# ------------------------------------------------------------------------------
# summarize_snapcounts_season
# ------------------------------------------------------------------------------
test_that("summarize_snapcounts_season: sums, means (with zeros), and game counts per season/team/player", {
  ssn <- summarize_snapcounts_season(toy_snaps)
  
  # Expect key columns exist
  expect_true(all(c(
    "season","team","player","pfr_player_id","position","games_played",
    "offense_games","defense_games","st_games",
    "offense_snaps","defense_snaps","st_snaps",
    "offense_pct_mean","defense_pct_mean","st_pct_mean"
  ) %in% names(ssn)))
  
  # A Player — SEA 2024:
  # games_played counts only rows with any snaps > 0 (rows 1 & 2) -> 2
  # offense_snaps = 50 + 40 = 90
  # st_snaps = 10 + 5 = 15
  # pct means are over ALL rows in the group after coalescing NA -> 0 (rows 1,2,3):
  #   offense_pct_mean = mean(0.80, 0.70, 0.00) = 0.5
  #   st_pct_mean      = mean(0.20, 0.10, 0.00) = 0.1
  a_sea_24 <- ssn %>% filter(season == 2024, team == "SEA", player == "A Player")
  expect_equal(a_sea_24$games_played, 2)
  expect_equal(a_sea_24$offense_games, 2)
  expect_equal(a_sea_24$defense_games, 0)
  expect_equal(a_sea_24$st_games, 2)
  expect_equal(a_sea_24$offense_snaps, 90)
  expect_equal(a_sea_24$defense_snaps, 0)
  expect_equal(a_sea_24$st_snaps, 15)
  expect_equal(a_sea_24$offense_pct_mean, 0.5)
  expect_equal(a_sea_24$st_pct_mean, 0.1)
  
  # A Player — SF 2023:
  # single row: offense_snaps=20, offense_pct=0.3
  a_sf_23 <- ssn %>% filter(season == 2023, team == "SF", player == "A Player")
  expect_equal(a_sf_23$games_played, 1)
  expect_equal(a_sf_23$offense_snaps, 20)
  expect_equal(a_sf_23$offense_pct_mean, 0.3)
  
  # B Defender — SEA 2024:
  # defense_snaps = 60 + 30 = 90
  # st_snaps = 5 + 0 = 5
  # defense_pct_mean = mean(0.90, 0.45) = 0.675
  # st_pct_mean = mean(0.50, 0.00) = 0.25
  b_sea_24 <- ssn %>% filter(season == 2024, team == "SEA", player == "B Defender")
  expect_equal(b_sea_24$games_played, 2)
  expect_equal(b_sea_24$defense_games, 2)
  expect_equal(b_sea_24$st_games, 1)
  expect_equal(b_sea_24$defense_snaps, 90)
  expect_equal(b_sea_24$st_snaps, 5)
  expect_equal(b_sea_24$defense_pct_mean, 0.675)
  expect_equal(b_sea_24$st_pct_mean, 0.25)
  
  # Sorted by season, team, player (we won’t be precious — just smoke-test it)
  expect_equal(
    ssn %>% arrange(season, team, player) %>% pull(player),
    ssn %>% pull(player)
  )
  
  # Rounding to 3 decimals for numerics (spot-check)
  expect_equal(b_sea_24$defense_pct_mean, round(0.675, 3))
})

# ------------------------------------------------------------------------------
# summarize_snapcounts_career
# ------------------------------------------------------------------------------
test_that("summarize_snapcounts_career: career sums, means (with zeros), spans, and distincts", {
  car <- summarize_snapcounts_career(toy_snaps)
  
  # A Player career:
  # games_played = 3 (two in 2024 with snaps + one in 2023)
  # offense_snaps = 50+40+20 = 110
  # st_snaps = 10+5+0 = 15
  # offense_pct_mean = mean(0.80, 0.70, 0.00, 0.30) = 0.45
  # st_pct_mean      = mean(0.20, 0.10, 0.00, 0.00) = 0.075
  a_car <- car %>% filter(player == "A Player", pfr_player_id == "P1")
  expect_equal(a_car$first_season, 2023)
  expect_equal(a_car$last_season, 2024)
  expect_equal(a_car$seasons_played, 2)
  expect_equal(a_car$teams_played_for, 2)
  expect_equal(a_car$games_played, 3)
  expect_equal(a_car$offense_games, 3)
  expect_equal(a_car$defense_games, 0)
  expect_equal(a_car$st_games, 2)
  expect_equal(a_car$offense_snaps, 110)
  expect_equal(a_car$st_snaps, 15)
  expect_equal(a_car$offense_pct_mean, 0.45)
  expect_equal(a_car$st_pct_mean, 0.075)
  
  # B Defender career:
  # defense_snaps = 60+30+10 = 100
  # defense_pct_mean = mean(0.90, 0.45, 0.15) = 0.50
  # st_pct_mean = mean(0.50, 0.00, 0.00) = 0.1666667 -> rounded 0.167
  b_car <- car %>% filter(player == "B Defender", pfr_player_id == "P2")
  expect_equal(b_car$first_season, 2023)
  expect_equal(b_car$last_season, 2024)
  expect_equal(b_car$seasons_played, 2)
  expect_equal(b_car$teams_played_for, 1)
  expect_equal(b_car$defense_snaps, 100)
  expect_equal(b_car$defense_pct_mean, 0.5)
  expect_equal(b_car$st_pct_mean, 0.167)
  
  # Sorted by player then first_season (again, a polite nudge)
  expect_equal(
    car %>% arrange(player, first_season) %>% pull(player),
    car %>% pull(player)
  )
})
