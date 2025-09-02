################################################################################
# step2_schedule_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_schedule_process_functions.R"))

################################################################################
# Read raw file
################################################################################
schedule_raw <- arrow::read_parquet(here("data", "raw", "schedule.parquet"))

################################################################################
# Clean and normalize
################################################################################
schedule_clean <- clean_schedule_data(schedule_raw)
weekly_results <- get_weekly_season_table(schedule_raw)
season_results <- summarize_season_team_results(schedule_raw)

################################################################################
# Write to processed/
################################################################################
arrow::write_parquet(
  schedule_clean %>% distinct(),
  here("data", "processed", "games.parquet")
)
arrow::write_parquet(
  weekly_results %>% distinct(),
  here("data", "processed", "weekly_results.parquet")
)
arrow::write_parquet(
  season_results %>% distinct(),
  here("data", "processed", "season_results.parquet")
)
