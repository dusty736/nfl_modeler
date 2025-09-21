################################################################################
# step3_long_player_format.R
################################################################################

library(arrow)
library(here)
library(dplyr)
library(tidyverse)
library(here)

source(here("etl", "R", "step3_sql", "step3_team_rankings_long_functions.R"))

team_stats_long <- arrow::read_parquet(here("data", "for_database", "team_weekly_tbl.parquet"))

################################################################################
# step3_long_player_format.R
################################################################################

team_ranking_columns <- c(
  # Offense (totals/rates)
  'passing_yards', 'passing_tds', 'interceptions', 'sacks',
  'passing_first_downs', 'passing_epa', 'drives',
  'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles',
  'rushing_first_downs', 'rushing_epa', 'points_scored',

  # Special teams
  'fg_pct',
  
  # Defensive
  'def_tackles', 'def_tackles_for_loss', 'def_fumbles_forced',
  'def_sacks', 'def_qb_hits', 'def_interceptions', 'def_fumbles',
  'def_penalty', 'points_allowed',
  'def_passing_yards_allowed',
  'def_passing_tds_allowed',
  'def_passing_first_downs_allowed',
  'def_pass_epa_allowed',
  'def_drives_allowed',
  'def_carries_allowed',
  'def_rushing_yards_allowed',
  'def_rushing_tds_allowed',
  'def_rushing_first_downs_allowed',
  'def_rushing_epa_allowed'
)

team_rankings_long <- rank_team_stats_weekly(team_stats_long, team_ranking_columns)

################################################################################
# Save
################################################################################

arrow::write_parquet(team_rankings_long, "data/for_database/team_weekly_rankings_tbl.parquet")


