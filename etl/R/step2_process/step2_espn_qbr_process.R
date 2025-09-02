################################################################################
# step2_contracts_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_espn_qbr_process_functions.R"))

################################################################################
# Load raw data
################################################################################
espn_qbr_raw <- arrow::read_parquet(here("data", "raw", "espn_qbr.parquet"))

################################################################################
# Clean and normalize
################################################################################

espn_qbr_season <- espn_qbr_raw
espn_qbr_career <- espn_qbr_career_by_season_type(espn_qbr_raw)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(espn_qbr_season %>% distinct(), "data/processed/espn_qbr_season.parquet")
arrow::write_parquet(espn_qbr_career %>% distinct(), "data/processed/espn_qbr_career.parquet")
