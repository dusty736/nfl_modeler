################################################################################
# step2_st_player_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_st_player_stats_process_functions.R"))

################################################################################
# Load raw data
################################################################################
st_stats_raw <- arrow::read_parquet(here("data", "raw", "st_player_stats.parquet"))

################################################################################
# Clean and normalize
################################################################################
st_stats_cleaned <- process_special_teams_stats(st_stats_raw)
st_stats_games <- add_cumulative_special_teams_stats(st_stats_cleaned)
st_stats_season <- summarize_special_teams_by_season(st_stats_cleaned)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(st_stats_games, "data/processed/st_player_stats_weekly.parquet")
arrow::write_parquet(st_stats_season, "data/processed/st_player_stats_season.parquet")
