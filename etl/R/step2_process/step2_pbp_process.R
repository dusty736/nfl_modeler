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

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(
  pbp_clean,
  here("data", "processed", "pbp_cleaned.parquet")
)
