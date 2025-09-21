# tests/testthat/test-step2-paricipation-process.R
# Basic, robust tests for participation processing (offense/defense + summaries).

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
})

testthat::local_edition(3)

# --- Load functions under test ------------------------------------------------
suppressMessages({
  source(here::here("etl", "R", "step2_process", "step2_participation_process_functions.R"))
})

# --- Minimal toy raw participation covering two games, both sides -------------
toy_participation <- tibble::tibble(
  # Game + play identifiers
  nflverse_game_id      = c("2024_01_SEA_SF","2024_01_SEA_SF","2024_01_SEA_SF",
                            "2024_02_SEA_LAR","2024_02_SEA_LAR"),
  play_id               = c(1L, 2L, 3L, 1L, 2L),
  
  # Possession (offense) team
  possession_team       = c("SEA","SEA","SF","SEA","LAR"),
  
  # Offense fields
  offense_formation     = c("SHOTGUN", "", "SINGLEBACK", "PISTOL", "SHOTGUN"),
  offense_personnel     = c("11", "", "12", "20", NA_character_),
  n_offense             = c(11L, 11L, 11L, 11L, 11L),
  ngs_air_yards         = c(7.5, NA, 18.0, 3.0, NA),
  time_to_throw         = c(2.6, NA, 2.2, 2.0, NA),  # NA => run, numeric => pass
  was_pressure          = c(TRUE, FALSE, FALSE, FALSE, FALSE),
  route                 = c("SLANT", "", "GO", "FLAT", NA_character_),
  
  # Defense fields
  defense_personnel     = c("NICKEL","BASE","NICKEL","DIME","BASE"),
  defenders_in_box      = c(6L, 7L, 8L, 6L, 7L),
  number_of_pass_rushers= c(5L, 4L, 6L, 3L, 4L),
  defense_man_zone_type = c("MAN_COVERAGE","ZONE_COVERAGE","MAN_COVERAGE","ZONE_COVERAGE","ZONE_COVERAGE"),
  defense_coverage_type = c("COVER_1","COVER_3","COVER_0","COVER_2","COVER_4")
)

# ------------------------------------------------------------------------------
# Offense: per-play processing
# ------------------------------------------------------------------------------
test_that("process_participation_offense_by_play: basic fields, types, and fallbacks", {
  off_pbp <- process_participation_offense_by_play(toy_participation)
  
  # Required columns exist
  expect_true(all(c("game_id","play_id","team","season","week",
                    "offense_formation","offense_personnel","play_type",
                    "pressures_allowed","cumulative_pass","cumulative_run") %in% names(off_pbp)))
  
  # Types + derived values
  expect_type(off_pbp$game_id, "character")
  expect_type(off_pbp$season, "integer")
  expect_type(off_pbp$week, "integer")
  expect_type(off_pbp$play_id, "integer")
  
  # Season/week parsed from game_id "2024_01_*"
  expect_equal(unique(off_pbp$season), 2024L)
  expect_true(all(unique(off_pbp$week) %in% c(1L, 2L)))
  
  # Formation/personnel fallbacks -> "OTHER" when empty/NA
  sea_g1 <- off_pbp %>% dplyr::filter(game_id=="2024_01_SEA_SF", team=="SEA") %>% arrange(play_id)
  expect_equal(sea_g1$offense_formation, c("SHOTGUN","OTHER"))
  expect_equal(sea_g1$offense_personnel, c("11","OTHER"))
  
  # play_type from time_to_throw
  expect_equal(sea_g1$play_type, c("pass","run"))
  
  # pressures_allowed is a cumsum within (game_id, team)
  expect_equal(sea_g1$pressures_allowed, c(1,1))
  
  # Cumulative route/formation counters are non-decreasing
  cum_cols <- c("cumulative_slant","cumulative_go","cumulative_flat",
                "cumulative_shotgun","cumulative_other","cumulative_pistol")
  expect_true(all(cum_cols %in% names(off_pbp)))
  
  mono_ok <- off_pbp %>%
    group_by(game_id, team) %>%
    summarize(
      ok = {
        cols <- across(all_of(cum_cols))
        all(unlist(lapply(cols, function(x) all(diff(x) >= 0))))
      },
      .groups = "drop"
    )
  expect_true(all(mono_ok$ok))
})

test_that("process_participation_offense_by_play: cumulative equals totals at last play", {
  off_pbp <- process_participation_offense_by_play(toy_participation)
  
  # SEA, G1: 1 pass, 1 run; 1 SLANT; 1 SHOTGUN; 1 OTHER
  sea_g1_last <- off_pbp %>% filter(game_id=="2024_01_SEA_SF", team=="SEA") %>% arrange(play_id) %>% slice_tail(n=1)
  expect_equal(sea_g1_last$cumulative_pass, 1)
  expect_equal(sea_g1_last$cumulative_run, 1)
  expect_equal(sea_g1_last$cumulative_slant, 1)
  expect_equal(sea_g1_last$cumulative_shotgun, 1)
  expect_equal(sea_g1_last$cumulative_other, 1)
})

# ------------------------------------------------------------------------------
# Offense: game- and formation-level summaries
# ------------------------------------------------------------------------------
test_that("summarize_offense_by_team_game: counts, pressures, and cumulative to-date", {
  off_pbp <- process_participation_offense_by_play(toy_participation)
  off_game <- summarize_offense_by_team_game(off_pbp)
  
  # SEA G1
  sea_g1 <- off_game %>% filter(game_id=="2024_01_SEA_SF", team=="SEA")
  expect_equal(sea_g1$n_plays, 2)
  expect_equal(sea_g1$n_pass, 1)
  expect_equal(sea_g1$n_run, 1)
  expect_equal(sea_g1$n_shotgun, 1)
  expect_equal(sea_g1$n_other_formations, 1)
  expect_equal(sea_g1$n_slant, 1)
  expect_equal(sea_g1$pressures_allowed, 1)
  expect_equal(sea_g1$avg_time_to_throw, 2.6)
  
  # SEA G2 (one pass, PISTOL)
  sea_g2 <- off_game %>% filter(game_id=="2024_02_SEA_LAR", team=="SEA")
  expect_equal(sea_g2$n_plays, 1)
  expect_equal(sea_g2$n_pass, 1)
  expect_equal(sea_g2$n_pistol, 1)
  
  # Cumulative to-date across season/team
  sea_rows <- off_game %>% filter(team=="SEA") %>% arrange(week)
  expect_equal(sea_rows$cumulative_plays, c(2, 3))
  expect_equal(sea_rows$cumulative_pass, c(1, 2))
  expect_equal(sea_rows$cumulative_run,  c(1, 1))
  expect_equal(sea_rows$cumulative_pressures_allowed, c(1, 1))
  expect_equal(sea_rows$avg_time_to_throw_to_date, c(2.6, (2.6 + 2.0)/2))
})

test_that("summarize_offense_by_team_game_formation: per-formation counts and stats", {
  off_pbp <- process_participation_offense_by_play(toy_participation)
  off_form <- summarize_offense_by_team_game_formation(off_pbp)
  
  # SEA G1 SHOTGUN (1 play, pass)
  row <- off_form %>% filter(game_id=="2024_01_SEA_SF", team=="SEA", offense_formation=="SHOTGUN")
  expect_equal(row$n_plays, 1)
  expect_equal(row$n_pass, 1)
  expect_equal(row$n_run, 0)
  expect_equal(row$pressures_allowed, 1)
  
  # SEA G1 OTHER (run)
  row2 <- off_form %>% filter(game_id=="2024_01_SEA_SF", team=="SEA", offense_formation=="OTHER")
  expect_equal(row2$n_plays, 1)
  expect_equal(row2$n_run, 1)
})

test_that("summarize_offense_by_team_season: aggregates games and averages game-level TTT", {
  off_pbp <- process_participation_offense_by_play(toy_participation)
  off_game <- summarize_offense_by_team_game(off_pbp)
  off_season <- summarize_offense_by_team_season(off_game)
  
  sea <- off_season %>% filter(season==2024, team=="SEA")
  expect_equal(sea$n_plays, 3)
  expect_equal(sea$n_pass, 2)
  expect_equal(sea$n_run, 1)
  expect_equal(sea$n_shotgun, 1)
  expect_equal(sea$n_pistol, 1)
  expect_equal(sea$n_other_formations, 1)
  expect_equal(sea$pressures_allowed, 1)        # 1 in G1 + 0 in G2
  expect_equal(sea$avg_time_to_throw, (2.6 + 2.0)/2)
})

# ------------------------------------------------------------------------------
# Defense: per-play processing
# ------------------------------------------------------------------------------
test_that("process_participation_defense_by_play: defense team assignment, bins, and cumulatives", {
  def_pbp <- process_participation_defense_by_play(toy_participation)
  
  # Required columns
  expect_true(all(c("game_id","play_id","defense_team","season","week",
                    "play_type","rush_bin","box_bin",
                    "cumulative_pass","cumulative_run",
                    "cumulative_blitz","cumulative_standard_box") %in% names(def_pbp)))
  
  # Defense team is the non-possession team for that game
  # G1: plays 1-2 possession SEA -> defense SF; play 3 possession SF -> defense SEA
  g1_df <- def_pbp %>% filter(game_id=="2024_01_SEA_SF") %>% arrange(play_id)
  expect_equal(g1_df$defense_team, c("SF","SF","SEA"))
  
  # play_type from time_to_throw
  expect_equal(g1_df$play_type, c("pass","run","pass"))
  
  # Rush + box bins
  expect_equal(g1_df$rush_bin[1], "blitz")        # 5
  expect_equal(g1_df$box_bin[1], "light")         # 6
  expect_equal(g1_df$rush_bin[2], "standard")     # 4
  expect_equal(g1_df$box_bin[2], "standard")      # 7
  expect_equal(g1_df$rush_bin[3], "heavy_blitz")  # 6
  expect_equal(g1_df$box_bin[3], "stacked")       # 8
  
  # Cumulative monotone within (game_id, defense_team)
  cum_cols <- c("cumulative_pass","cumulative_run",
                "cumulative_blitz","cumulative_standard_rush","cumulative_heavy_blitz",
                "cumulative_light_box","cumulative_standard_box","cumulative_stacked_box",
                "cumulative_cover_1","cumulative_cover_3")
  have <- intersect(cum_cols, names(def_pbp))
  mono_ok <- def_pbp %>%
    group_by(game_id, defense_team) %>%
    summarize(
      ok = {
        cols <- across(all_of(have))
        all(unlist(lapply(cols, function(x) all(diff(x) >= 0))))
      },
      .groups = "drop"
    )
  expect_true(all(mono_ok$ok))
})

# ------------------------------------------------------------------------------
# Defense: game- and season-level summaries
# ------------------------------------------------------------------------------
test_that("summarize_defense_by_team_game: counts and to-date cumulatives", {
  def_pbp <- process_participation_defense_by_play(toy_participation)
  def_game <- summarize_defense_by_team_game(def_pbp)
  
  # G1 SF defending vs SEA (2 plays: pass+run)
  sf_g1 <- def_game %>% filter(game_id=="2024_01_SEA_SF", defense_team=="SF")
  expect_equal(sf_g1$n_plays, 2)
  expect_equal(sf_g1$n_pass, 1)
  expect_equal(sf_g1$n_run, 1)
  expect_equal(sf_g1$n_blitz, 1)            # play1
  expect_equal(sf_g1$n_standard_rush, 1)    # play2
  expect_equal(sf_g1$n_light_box, 1)        # play1
  expect_equal(sf_g1$n_standard_box, 1)     # play2
  expect_equal(sf_g1$n_man, 1)
  expect_equal(sf_g1$n_zone, 1)
  expect_equal(sf_g1$n_cover_1, 1)
  expect_equal(sf_g1$n_cover_3, 1)
  expect_equal(sf_g1$n_pressures, 1)
  expect_equal(sf_g1$avg_time_to_throw, 2.6)
  
  # Cumulative to-date equals game counts (only one game for SF)
  expect_equal(sf_g1$cumulative_plays, sf_g1$n_plays)
  expect_equal(sf_g1$cumulative_pass,  sf_g1$n_pass)
  expect_equal(sf_g1$cumulative_run,   sf_g1$n_run)
})

test_that("summarize_defense_by_team_season: aggregates plays and averages TTT", {
  def_pbp <- process_participation_defense_by_play(toy_participation)
  def_season <- summarize_defense_by_team_season(def_pbp)
  
  # SEA defense appears twice: G1 play3 (vs SF pass) + G2 play2 (vs LAR run)
  sea_def <- def_season %>% filter(season==2024, defense_team=="SEA")
  expect_equal(sea_def$n_plays, 2)
  expect_equal(sea_def$n_pass, 1)
  expect_equal(sea_def$n_run, 1)
  expect_equal(sea_def$avg_time_to_throw, 2.2)   # mean(2.2, NA)
})

  