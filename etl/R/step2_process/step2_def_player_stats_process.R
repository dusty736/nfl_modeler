################################################################################
# step2_def_player_stats_process.R
################################################################################

library(arrow)
library(here)
library(tidyverse)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_def_player_stats_process_functions.R"))

################################################################################
# Load raw data
################################################################################
def_player_stats_raw <- arrow::read_parquet(here("data", "raw", "def_player_stats.parquet")) %>% 
  distinct() %>% 
  mutate(def_tackles = def_tackles_solo,
         def_fumble_recovery_own = fumble_recovery_own,
         def_fumble_recovery_yards_own = fumble_recovery_yards_own,
         def_fumble_recovery_opp = fumble_recovery_opp,
         def_fumble_recovery_yards_opp = fumble_recovery_yards_opp,
         def_safety = def_safeties,
         def_penalty = penalties,
         def_penalty_yards = penalty_yards)

################################################################################
# Clean and normalize
################################################################################
def_player_stats_cleaned <- process_defensive_player_stats(def_player_stats_raw)
def_player_stats_season <- summarize_defensive_player_stats_by_season(def_player_stats_cleaned)
def_team_stats_season <- summarize_defensive_stats_by_team_season(def_player_stats_cleaned)
def_team_stats_weekly <- summarize_defensive_stats_by_team_weekly(def_player_stats_cleaned)
def_player_stats_career <- summarize_defensive_stats_by_player(def_player_stats_cleaned)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(def_player_stats_cleaned %>% distinct(), "data/processed/def_player_stats_weekly.parquet")
arrow::write_parquet(def_player_stats_season %>% distinct(), "data/processed/def_player_stats_season.parquet")
arrow::write_parquet(def_player_stats_career %>% distinct(), "data/processed/def_player_stats_career.parquet")
arrow::write_parquet(def_team_stats_season %>% distinct(), "data/processed/def_team_stats_season.parquet")
arrow::write_parquet(def_team_stats_weekly %>% distinct(), "data/processed/def_team_stats_week.parquet")
