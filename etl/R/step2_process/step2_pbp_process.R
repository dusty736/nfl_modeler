################################################################################
# step2_pbp_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_pbp_process_functions.R"))

################################################################################
# Load raw data
################################################################################
pbp_raw <- arrow::read_parquet(here("data", "raw", "pbp.parquet"))

################################################################################
# Clean and normalize
################################################################################
pbp_clean <- pbp_raw |>
  clean_pbp_data() |>
  add_team_cumulative_stats() |>
  add_defense_cumulative_stats()

pbp_feats <- pbp_clean |>
  clean_pbp_data() |>
  add_team_cumulative_stats() |>
  add_defense_cumulative_stats() |>
  add_situational_features() |>
  derive_team_rate_features() %>% distinct()

game_team_feats <- pbp_clean %>%
  clean_pbp_data() %>%
  summarise_team_game_features(
    qd_yardline  = 40,  # opp 40
    qd_min_plays = 4,
    qd_min_yards = 20
  ) %>% distinct()

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(
  pbp_feats,
  here("data", "processed", "pbp_cleaned.parquet")
)

arrow::write_parquet(
  game_team_feats,
  here("data", "processed", "pbp_cleaned_games.parquet")
)
