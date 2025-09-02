################################################################################
# step2_injuries_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_injuries_process_functions.R"))

################################################################################
# Load raw data
################################################################################
injuries_raw <- arrow::read_parquet(here("data", "raw", "injuries.parquet"))

################################################################################
# Clean and normalize
################################################################################
injuries_cleaned <- process_injuries(injuries_raw)

# Process injuries by position
injuries_position <- position_injury_summary(injuries_cleaned)

# Process team injuries by week
injuries_week_team <- team_injury_summary(injuries_position)

# process team injuries by season
injuries_season_team <- season_injury_summary(injuries_week_team)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(injuries_cleaned %>% distinct(), "data/processed/injuries_weekly.parquet")
arrow::write_parquet(injuries_position %>% distinct(), "data/processed/injuries_position_weekly.parquet")
arrow::write_parquet(injuries_week_team %>% distinct(), "data/processed/injuries_team_weekly.parquet")
arrow::write_parquet(injuries_season_team %>% distinct(), "data/processed/injuries_team_season.parquet")


