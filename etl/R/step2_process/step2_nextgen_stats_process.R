################################################################################
# step2_nextgen_stats_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_nextgen_stats_process_functions.R"))

################################################################################
# Load raw data
################################################################################
nextgen_stats_raw <- arrow::read_parquet(here("data", "raw", "nextgen_stats.parquet"))

################################################################################
# Clean and normalize
################################################################################
nextgen_stats_cleaned <- process_nextgen_stats(nextgen_stats_raw)

# Get stats by player - season
nextgen_stats_player_season <- aggregate_nextgen_by_season(nextgen_stats_cleaned)

# Get stats by player
nextgen_stats_player_career <- aggregate_nextgen_by_career(nextgen_stats_cleaned)

# Get stats by player - season type
nextgen_stats_player_postseason <- aggregate_nextgen_postseason(nextgen_stats_cleaned)

# Get running player stats by season
nextgen_stats_player_season_cumulative <- compute_cumulative_nextgen_stats(nextgen_stats_cleaned)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(nextgen_stats_player_season, "data/processed/nextgen_stats_player_season.parquet")
arrow::write_parquet(nextgen_stats_player_career, "data/processed/nextgen_stats_player_career.parquet")
arrow::write_parquet(nextgen_stats_player_postseason, "data/processed/nextgen_stats_player_postseason.parquet")
arrow::write_parquet(nextgen_stats_player_season_cumulative, "data/processed/nextgen_stats_player_weekly.parquet")
