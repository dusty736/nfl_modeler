################################################################################
# step2_def_player_stats_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_def_player_stats_process_functions.R"))

################################################################################
# Load raw data
################################################################################
def_player_stats_raw <- arrow::read_parquet(here("data", "raw", "def_player_stats.parquet"))

################################################################################
# Clean and normalize
################################################################################
def_player_stats_cleaned <- process_defensive_player_stats(def_player_stats_raw)
def_player_stats_season <- summarize_defensive_player_stats_by_season(def_player_stats_cleaned)
def_team_stats_season <- summarize_defensive_stats_by_team_season(def_player_stats_cleaned)
def_player_stats_career <- summarize_defensive_stats_by_player(def_player_stats_cleaned)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(def_player_stats_cleaned, "data/processed/def_player_stats_weekly.parquet")
arrow::write_parquet(def_player_stats_season, "data/processed/def_player_stats_season.parquet")
arrow::write_parquet(def_player_stats_career, "data/processed/def_player_stats_career.parquet")
arrow::write_parquet(def_team_stats_season, "data/processed/def_team_stats_season.parquet")
