################################################################################
# step2_participation_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_participation_process_functions.R"))

################################################################################
# Load raw data
################################################################################
participation_raw <- arrow::read_parquet(here("data", "raw", "participation.parquet"))

################################################################################
# Clean and normalize
################################################################################

# Offense
participation_pbp_offense <- process_participation_offense_by_play(participation_raw)
participation_game_offense <- summarize_offense_by_team_game(participation_pbp_offense)
participation_game_formation_offense <- summarize_offense_by_team_game_formation(participation_pbp_offense)
participation_season_offense <- summarize_offense_by_team_season(participation_game_offense)

# Defense
participation_pbp_defense <- process_participation_defense_by_play(participation_raw)
participation_game_defense <- summarize_defense_by_team_game(participation_pbp_defense)
participation_season_defense <- summarize_defense_by_team_season(participation_pbp_defense)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(participation_pbp_offense %>% distinct(), "data/processed/participation_offense_pbp.parquet")
arrow::write_parquet(participation_game_offense %>% distinct(), "data/processed/participation_offense_game.parquet")
arrow::write_parquet(participation_game_formation_offense %>% distinct(), "data/processed/participation_offense_formation_game.parquet")
arrow::write_parquet(participation_season_offense %>% distinct(), "data/processed/participation_offense_season.parquet")
arrow::write_parquet(participation_pbp_defense %>% distinct(), "data/processed/participation_defense_pbp.parquet")
arrow::write_parquet(participation_game_defense %>% distinct(), "data/processed/participation_defense_game.parquet")
arrow::write_parquet(participation_season_defense %>% distinct(), "data/processed/participation_defense_season.parquet")

