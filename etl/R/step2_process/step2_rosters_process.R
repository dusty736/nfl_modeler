################################################################################
# step2_rosters_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_rosters_process_functions.R"))

################################################################################
# Load raw data
################################################################################
roster_raw <- arrow::read_parquet(here("data", "raw", "rosters.parquet"))

################################################################################
# Clean and normalize
################################################################################
roster_clean <- process_rosters(roster_raw)
roster_summary <- summarize_rosters_by_team_season(roster_clean)
roster_position_summary <- summarize_rosters_by_team_position(roster_clean)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(roster_clean, "data/processed/rosters.parquet")
arrow::write_parquet(roster_summary, "data/processed/roster_summary.parquet")
arrow::write_parquet(roster_position_summary, "data/processed/roster_position_summary.parquet")
