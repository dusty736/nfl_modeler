# parquet_to_postgres.R

library(DBI)
library(RPostgres)
library(arrow)
library(dplyr)

# Database connection ----
con <- dbConnect(
  Postgres(),
  dbname = "nfl",
  host = "localhost",
  port = 5432,
  user = "nfl_user",
  password = "nfl_pass"
)

# Helper to load one file ----
load_parquet_to_postgres <- function(parquet_path, table_name) {
  cat("Loading:", parquet_path, "into table:", table_name, "\n")
  df <- as.data.frame(read_parquet(parquet_path))
  dbWriteTable(con, table_name, df, overwrite = TRUE, row.names = FALSE)
}

# Bulk load all tables ----
base_path <- "data/for_database"

load_parquet_to_postgres(file.path(base_path, "career_stats_qb_tbl.parquet"), "career_stats_qb_tbl")
load_parquet_to_postgres(file.path(base_path, "career_stats_rb_tbl.parquet"), "career_stats_rb_tbl")
load_parquet_to_postgres(file.path(base_path, "career_stats_te_tbl.parquet"), "career_stats_te_tbl")
load_parquet_to_postgres(file.path(base_path, "career_stats_wr_tbl.parquet"), "career_stats_wr_tbl")
load_parquet_to_postgres(file.path(base_path, "contracts_position_cap_pct_tbl.parquet"), "contracts_position_cap_pct_tbl")
load_parquet_to_postgres(file.path(base_path, "contracts_qb_tbl.parquet"), "contracts_qb_tbl")
load_parquet_to_postgres(file.path(base_path, "def_player_stats_career_tbl.parquet"), "def_player_stats_career_tbl")
load_parquet_to_postgres(file.path(base_path, "def_player_stats_season_tbl.parquet"), "def_player_stats_season_tbl")
load_parquet_to_postgres(file.path(base_path, "def_player_stats_weekly_tbl.parquet"), "def_player_stats_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "def_team_stats_season_tbl.parquet"), "def_team_stats_season_tbl")
load_parquet_to_postgres(file.path(base_path, "depth_charts_player_starts_tbl.parquet"), "depth_charts_player_starts_tbl")
load_parquet_to_postgres(file.path(base_path, "depth_charts_position_stability_tbl.parquet"), "depth_charts_position_stability_tbl")
load_parquet_to_postgres(file.path(base_path, "depth_charts_qb_team_tbl.parquet"), "depth_charts_qb_team_tbl")
load_parquet_to_postgres(file.path(base_path, "depth_charts_starters_tbl.parquet"), "depth_charts_starters_tbl")
load_parquet_to_postgres(file.path(base_path, "games_tbl.parquet"), "games_tbl")
load_parquet_to_postgres(file.path(base_path, "injuries_position_weekly_tbl.parquet"), "injuries_position_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "injuries_team_season_tbl.parquet"), "injuries_team_season_tbl")
load_parquet_to_postgres(file.path(base_path, "injuries_team_weekly_tbl.parquet"), "injuries_team_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "injuries_weekly_tbl.parquet"), "injuries_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_career_tbl.parquet"), "nextgen_stats_player_career_tbl")
load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_postseason_tbl.parquet"), "nextgen_stats_player_postseason_tbl")
load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_season_tbl.parquet"), "nextgen_stats_player_season_tbl")
load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_weekly_tbl.parquet"), "nextgen_stats_player_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "participation_defense_game_tbl.parquet"), "participation_defense_game_tbl")
load_parquet_to_postgres(file.path(base_path, "participation_defense_pbp_tbl.parquet"), "participation_defense_pbp_tbl")
load_parquet_to_postgres(file.path(base_path, "participation_defense_season_tbl.parquet"), "participation_defense_season_tbl")
load_parquet_to_postgres(file.path(base_path, "participation_offense_game_tbl.parquet"), "participation_offense_game_tbl")
load_parquet_to_postgres(file.path(base_path, "participation_offense_pbp_tbl.parquet"), "participation_offense_pbp_tbl")
load_parquet_to_postgres(file.path(base_path, "participation_offense_season_tbl.parquet"), "participation_offense_season_tbl")
load_parquet_to_postgres(file.path(base_path, "pbp_tbl.parquet"), "pbp_tbl")
load_parquet_to_postgres(file.path(base_path, "roster_position_summary_tbl.parquet"), "roster_position_summary_tbl")
load_parquet_to_postgres(file.path(base_path, "roster_summary_tbl.parquet"), "roster_summary_tbl")
load_parquet_to_postgres(file.path(base_path, "rosters_tbl.parquet"), "rosters_tbl")
load_parquet_to_postgres(file.path(base_path, "season_results_tbl.parquet"), "season_results_tbl")
load_parquet_to_postgres(file.path(base_path, "season_stats_qb_tbl.parquet"), "season_stats_qb_tbl")
load_parquet_to_postgres(file.path(base_path, "season_stats_rb_tbl.parquet"), "season_stats_rb_tbl")
load_parquet_to_postgres(file.path(base_path, "season_stats_te_tbl.parquet"), "season_stats_te_tbl")
load_parquet_to_postgres(file.path(base_path, "season_stats_wr_tbl.parquet"), "season_stats_wr_tbl")
load_parquet_to_postgres(file.path(base_path, "st_player_stats_season_tbl.parquet"), "st_player_stats_season_tbl")
load_parquet_to_postgres(file.path(base_path, "st_player_stats_weekly_tbl.parquet"), "st_player_stats_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "weekly_results_tbl.parquet"), "weekly_results_tbl")
load_parquet_to_postgres(file.path(base_path, "weekly_stats_qb_tbl.parquet"), "weekly_stats_qb_tbl")
load_parquet_to_postgres(file.path(base_path, "weekly_stats_rb_tbl.parquet"), "weekly_stats_rb_tbl")
load_parquet_to_postgres(file.path(base_path, "weekly_stats_te_tbl.parquet"), "weekly_stats_te_tbl")
load_parquet_to_postgres(file.path(base_path, "weekly_stats_wr_tbl.parquet"), "weekly_stats_wr_tbl")
load_parquet_to_postgres(file.path(base_path, "team_metadata_tbl.parquet"), "team_metadata_tbl")
load_parquet_to_postgres(file.path(base_path, "id_map_tbl.parquet"), "id_map_tbl")
load_parquet_to_postgres(file.path(base_path, "snapcount_weekly_tbl.parquet"), "snapcount_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "snapcount_season_tbl.parquet"), "snapcount_season_tbl")
load_parquet_to_postgres(file.path(base_path, "snapcount_career_tbl.parquet"), "snapcount_career_tbl")
load_parquet_to_postgres(file.path(base_path, "espn_qbr_season_tbl.parquet"), "espn_qbr_season_tbl")
load_parquet_to_postgres(file.path(base_path, "espn_qbr_career_tbl.parquet"), "espn_qbr_career_tbl")
load_parquet_to_postgres(file.path(base_path, "def_team_stats_week_tbl.parquet"), "def_team_stats_week_tbl")
load_parquet_to_postgres(file.path(base_path, "off_team_stats_week_tbl.parquet"), "off_team_stats_week_tbl")
load_parquet_to_postgres(file.path(base_path, "off_team_stats_season_tbl.parquet"), "off_team_stats_season_tbl")
load_parquet_to_postgres(file.path(base_path, "player_weekly_tbl.parquet"), "player_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "player_season_tbl.parquet"), "player_season_tbl")
load_parquet_to_postgres(file.path(base_path, "player_career_tbl.parquet"), "player_career_tbl")
load_parquet_to_postgres(file.path(base_path, "team_weekly_tbl.parquet"), "team_weekly_tbl")
load_parquet_to_postgres(file.path(base_path, "team_sesaon_tbl.parquet"), "team_sesaon_tbl")
load_parquet_to_postgres(file.path(base_path, "team_career_tbl.parquet"), "team_career_tbl")


