################################################################################
# step2_team_strength_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)
library(digest)
library(jsonlite)

source(here("etl", "R", "step2_process", "step2_team_strength_process_functions.R"))

################################################################################
# Load data
################################################################################
games <- arrow::read_parquet(here("data", "processed", "games.parquet"))

games <- games %>%
  dplyr::mutate(
    game_type   = toupper(game_type),
    season_type = dplyr::if_else(game_type == "REG", "REG", "POST", missing = "POST"),
    game_type = dplyr::if_else(game_type == "REG", "REG", "POST", missing = "POST"),
    home_team = dplyr::recode(home_team, OAK = "LV", STL = "LA", SD = "LAC", .default = home_team),
    away_team = dplyr::recode(away_team, OAK = "LV", STL = "LA", SD = "LAC", .default = away_team)
  )

pbp <- arrow::read_parquet(here("data", "processed", "pbp_cleaned.parquet")) %>% 
  left_join(., games %>% dplyr::select(game_id, season, week), by='game_id')

################################################################################
# Process data
################################################################################

ratings <- build_team_strength_v01(
  pbp   = pbp,
  games = games,        # optional but recommended for bye-week rows
  w_min = 0.25,
  H     = 4,
  beta  = 0.7,
  min_eff_plays = 20,
  keep_components = FALSE
)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(ratings %>% dplyr::select(-params_version, -run_timestamp) %>% distinct(), "data/for_database/team_strength_tbl.parquet")


