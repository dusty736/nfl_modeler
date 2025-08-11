################################################################################
# step2_contracts_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_snapcount_process_functions.R"))

################################################################################
# Load raw data
################################################################################
snapcount_raw <- arrow::read_parquet(here("data", "raw", "player_snapcount.parquet"))

################################################################################
# Clean and normalize
################################################################################
snapcount_weekly <- snapcount_raw
snapcount_season <- summarize_snapcounts_season(snapcount_raw)
snapcount_career <- summarize_snapcounts_career(snapcount_raw)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(snapcount_weekly, "data/processed/snapcount_weekly.parquet")
arrow::write_parquet(snapcount_season, "data/processed/snapcount_season.parquet")
arrow::write_parquet(snapcount_career, "data/processed/snapcount_career.parquet")

