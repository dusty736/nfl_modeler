################################################################################
# step2_team_medadata_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_team_metadata_process_functions.R"))

################################################################################
# Load raw data
################################################################################
team_metadata_raw <- arrow::read_parquet(here("data", "raw", "team_metadata.parquet"))

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(team_metadata_raw %>% distinct(), "data/processed/team_metadata.parquet")


