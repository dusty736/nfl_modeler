################################################################################
# step2_off_player_stats_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_off_player_stats_process_functions.R"))

################################################################################
# Load raw data
################################################################################
off_player_stats_raw <- arrow::read_parquet(here("data", "raw", "off_player_stats.parquet"))

################################################################################
# Clean and normalize
################################################################################
# QB
weekly_qb_stats <- process_qb_stats(off_player_stats_raw)
season_qb_stats <- aggregate_qb_season_stats(weekly_qb_stats)
career_qb_stats <- aggregate_qb_career_stats(weekly_qb_stats)

# RB
weekly_rb_stats <- process_rb_stats(off_player_stats_raw)
season_rb_stats <- aggregate_rb_season_stats(weekly_rb_stats)
career_rb_stats <- aggregate_rb_career_stats(weekly_rb_stats)

# WR
weekly_wr_stats <- process_receiver_stats(off_player_stats_raw, position_group = 'WR')
season_wr_stats <- aggregate_receiver_season_stats(weekly_wr_stats)
career_wr_stats <- aggregate_receiver_career_stats(weekly_wr_stats)

# TE
weekly_te_stats <- process_receiver_stats(off_player_stats_raw, position_group = 'TE')
season_te_stats <- aggregate_receiver_season_stats(weekly_te_stats)
career_te_stats <- aggregate_receiver_career_stats(weekly_te_stats)

# Team
weekly_team_stats <- aggregate_offense_team_week_stats(off_player_stats_raw)
season_team_stats <- aggregate_offense_team_season_stats(off_player_stats_raw)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(weekly_qb_stats %>% distinct(), "data/processed/weekly_stats_qb.parquet")
arrow::write_parquet(season_qb_stats %>% distinct(), "data/processed/season_stats_qb.parquet")
arrow::write_parquet(career_qb_stats %>% distinct(), "data/processed/career_stats_qb.parquet")
arrow::write_parquet(weekly_rb_stats %>% distinct(), "data/processed/weekly_stats_rb.parquet")
arrow::write_parquet(season_rb_stats %>% distinct(), "data/processed/season_stats_rb.parquet")
arrow::write_parquet(career_rb_stats %>% distinct(), "data/processed/career_stats_rb.parquet")
arrow::write_parquet(weekly_wr_stats %>% distinct(), "data/processed/weekly_stats_wr.parquet")
arrow::write_parquet(season_wr_stats %>% distinct(), "data/processed/season_stats_wr.parquet")
arrow::write_parquet(career_wr_stats %>% distinct(), "data/processed/career_stats_wr.parquet")
arrow::write_parquet(weekly_te_stats %>% distinct(), "data/processed/weekly_stats_te.parquet")
arrow::write_parquet(season_te_stats %>% distinct(), "data/processed/season_stats_te.parquet")
arrow::write_parquet(career_te_stats %>% distinct(), "data/processed/career_stats_te.parquet")
arrow::write_parquet(weekly_team_stats %>% distinct(), "data/processed/off_team_stats_week.parquet")
arrow::write_parquet(season_team_stats %>% distinct(), "data/processed/off_team_stats_season.parquet")
