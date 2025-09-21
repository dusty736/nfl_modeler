# parquet_to_postgres.R

library(DBI)
library(RPostgres)
library(arrow)
library(dplyr)
library(here)

source(here("etl", "R", "step3_sql", "step3_parquet_to_postgres_functions.R"))

# Clear Schema ----
drop_schema(con, schema = "prod")

# Create Schema ----
create_schema(con, schema = "prod")

# Bulk load all tables ----
base_path <- "data/for_database"

# Fill Database ----
load_parquet_to_postgres(file.path(base_path, "career_stats_qb_tbl.parquet"), schema = 'prod', "career_stats_qb_tbl")
create_index(con = con, schema = 'prod', table = 'career_stats_qb_tbl', id_cols = c('player_id'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "career_stats_rb_tbl.parquet"), schema = 'prod', "career_stats_rb_tbl")
create_index(con = con, schema = 'prod', table = 'career_stats_rb_tbl', id_cols = c('player_id'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "career_stats_te_tbl.parquet"), schema = 'prod', "career_stats_te_tbl")
create_index(con = con, schema = 'prod', table = 'career_stats_te_tbl', id_cols = c('player_id'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "career_stats_wr_tbl.parquet"), schema = 'prod', "career_stats_wr_tbl")
create_index(con = con, schema = 'prod', table = 'career_stats_wr_tbl', id_cols = c('player_id'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "contracts_position_cap_pct_tbl.parquet"), schema = 'prod', "contracts_position_cap_pct_tbl")
create_index(con = con, schema = 'prod', table = 'contracts_position_cap_pct_tbl', id_cols = c("position","year_signed","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "contracts_qb_tbl.parquet"), schema = 'prod', "contracts_qb_tbl")
create_index(con = con, schema = 'prod', table = 'contracts_qb_tbl', id_cols = c("gsis_id", "team", "year_signed") , unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "def_player_stats_career_tbl.parquet"), schema = 'prod', "def_player_stats_career_tbl")
create_index(con = con, schema = 'prod', table = 'def_player_stats_career_tbl', id_cols = c('player_id'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "def_player_stats_season_tbl.parquet"), schema = 'prod', "def_player_stats_season_tbl")
create_index(con = con, schema = 'prod', table = 'def_player_stats_season_tbl', id_cols = c('player_id', 'season'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "def_player_stats_weekly_tbl.parquet"), schema = 'prod', "def_player_stats_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'def_player_stats_weekly_tbl', id_cols = c('player_id', 'team', 'season', 'week'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "def_team_stats_season_tbl.parquet"), schema = 'prod', "def_team_stats_season_tbl")
create_index(con = con, schema = 'prod', table = 'def_team_stats_season_tbl', id_cols = c('team', 'season'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "depth_charts_player_starts_tbl.parquet"), schema = 'prod', "depth_charts_player_starts_tbl")
create_index(con = con, schema = 'prod', table = 'depth_charts_player_starts_tbl', id_cols = c('team', 'season', 'position', 'gsis_id'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "depth_charts_position_stability_tbl.parquet"), schema = 'prod', "depth_charts_position_stability_tbl")
create_index(con = con, schema = 'prod', table = 'depth_charts_position_stability_tbl', id_cols = c('season', 'team', 'week', 'position'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "depth_charts_qb_team_tbl.parquet"), schema = 'prod', "depth_charts_qb_team_tbl")
create_index(con = con, schema = 'prod', table = 'depth_charts_qb_team_tbl', id_cols = c("season","week","team"), unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "depth_charts_starters_tbl.parquet"), schema = 'prod', "depth_charts_starters_tbl")
create_index(con = con, schema = 'prod', table = 'depth_charts_starters_tbl', id_cols = c("season","week","team","gsis_id") , unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "games_tbl.parquet"), schema = 'prod', "games_tbl")
create_index(con = con, schema = 'prod', table = 'games_tbl', id_cols = c('game_id'), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "injuries_position_weekly_tbl.parquet"), schema = 'prod', "injuries_position_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'injuries_position_weekly_tbl', id_cols = c("season","week","team","position") , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "injuries_team_season_tbl.parquet"), schema = 'prod', "injuries_team_season_tbl")
create_index(con = con, schema = 'prod', table = 'injuries_team_season_tbl', id_cols = c("season","team") , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "injuries_team_weekly_tbl.parquet"), schema = 'prod', "injuries_team_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'injuries_team_weekly_tbl', id_cols = c("season","week","team") , unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "injuries_weekly_tbl.parquet"), schema = 'prod', "injuries_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'injuries_weekly_tbl', id_cols = c("season","week","team","gsis_id"), unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_career_tbl.parquet"), schema = 'prod', "nextgen_stats_player_career_tbl")
create_index(con = con, schema = 'prod', table = 'nextgen_stats_player_career_tbl', id_cols = c("player_gsis_id") , unique = TRUE)

#load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_postseason_tbl.parquet"), schema = 'prod', "nextgen_stats_player_postseason_tbl")
#create_index(con = con, schema = 'prod', table = 'nextgen_stats_player_postseason_tbl', id_cols = c("player_gsis_id","team_abbr")  , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_season_tbl.parquet"), schema = 'prod', "nextgen_stats_player_season_tbl")
create_index(con = con, schema = 'prod', table = 'nextgen_stats_player_season_tbl', id_cols = c("season","player_gsis_id","team_abbr")    , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_weekly_tbl.parquet"), schema = 'prod', "nextgen_stats_player_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'nextgen_stats_player_weekly_tbl', id_cols = c("season","season_type","week","player_gsis_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "participation_defense_game_tbl.parquet"), schema = 'prod', "participation_defense_game_tbl")
create_index(con = con, schema = 'prod', table = 'participation_defense_game_tbl', id_cols = c("game_id","defense_team")   , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "participation_defense_pbp_tbl.parquet"), schema = 'prod', "participation_defense_pbp_tbl")
create_index(con = con, schema = 'prod', table = 'participation_defense_pbp_tbl', id_cols = c("game_id","play_id","defense_team") , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "participation_defense_season_tbl.parquet"), schema = 'prod', "participation_defense_season_tbl")
create_index(con = con, schema = 'prod', table = 'participation_defense_season_tbl', id_cols = c("season","defense_team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "participation_offense_game_tbl.parquet"), schema = 'prod', "participation_offense_game_tbl")
create_index(con = con, schema = 'prod', table = 'participation_offense_game_tbl', id_cols = c("game_id","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "participation_offense_pbp_tbl.parquet"), schema = 'prod', "participation_offense_pbp_tbl")
create_index(con = con, schema = 'prod', table = 'participation_offense_pbp_tbl', id_cols = c("game_id","play_id","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "participation_offense_season_tbl.parquet"), schema = 'prod', "participation_offense_season_tbl")
create_index(con = con, schema = 'prod', table = 'participation_offense_season_tbl', id_cols = c("season","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "pbp_tbl.parquet"), schema = 'prod', "pbp_tbl")
create_index(con = con, schema = 'prod', table = 'pbp_tbl', id_cols = c("game_id","play_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "pbp_game_tbl.parquet"), schema = 'prod', "pbp_game_tbl")
create_index(con = con, schema = 'prod', table = 'pbp_game_tbl', id_cols = c("game_id","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "roster_position_summary_tbl.parquet"), schema = 'prod', "roster_position_summary_tbl")
create_index(con = con, schema = 'prod', table = 'roster_position_summary_tbl', id_cols = c("season","team","position"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "roster_summary_tbl.parquet"), schema = 'prod', "roster_summary_tbl")
create_index(con = con, schema = 'prod', table = 'roster_summary_tbl', id_cols = c("season","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "rosters_tbl.parquet"), schema = 'prod', "rosters_tbl")
create_index(con = con, schema = 'prod', table = 'rosters_tbl', id_cols = c("season","week","team_id","player_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "season_results_tbl.parquet"), schema = 'prod', "season_results_tbl")
create_index(con = con, schema = 'prod', table = 'season_results_tbl', id_cols = c("season","team_id") , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "season_stats_qb_tbl.parquet"), schema = 'prod', "season_stats_qb_tbl")
create_index(con = con, schema = 'prod', table = 'season_stats_qb_tbl', id_cols = c("season","player_id") , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "season_stats_rb_tbl.parquet"), schema = 'prod', "season_stats_rb_tbl")
create_index(con = con, schema = 'prod', table = 'season_stats_rb_tbl', id_cols = c("season","player_id") , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "season_stats_te_tbl.parquet"), schema = 'prod', "season_stats_te_tbl")
create_index(con = con, schema = 'prod', table = 'season_stats_te_tbl', id_cols = c("season","player_id") , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "season_stats_wr_tbl.parquet"), schema = 'prod', "season_stats_wr_tbl")
create_index(con = con, schema = 'prod', table = 'season_stats_wr_tbl', id_cols = c("season","player_id") , unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "st_player_stats_season_tbl.parquet"), schema = 'prod', "st_player_stats_season_tbl")
create_index(con = con, schema = 'prod', table = 'st_player_stats_season_tbl', id_cols = c("season","player_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "st_player_stats_weekly_tbl.parquet"), schema = 'prod', "st_player_stats_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'st_player_stats_weekly_tbl', id_cols = c("season","season_type","week","player_id"), unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "weekly_results_tbl.parquet"), schema = 'prod', "weekly_results_tbl")
create_index(con = con, schema = 'prod', table = 'weekly_results_tbl', id_cols = c("game_id","team_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "weekly_stats_qb_tbl.parquet"), schema = 'prod', "weekly_stats_qb_tbl")
create_index(con = con, schema = 'prod', table = 'weekly_stats_qb_tbl', id_cols = c("season", "recent_team", "season_type","week","player_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "weekly_stats_rb_tbl.parquet"), schema = 'prod', "weekly_stats_rb_tbl")
create_index(con = con, schema = 'prod', table = 'weekly_stats_rb_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "weekly_stats_te_tbl.parquet"), schema = 'prod', "weekly_stats_te_tbl")
create_index(con = con, schema = 'prod', table = 'weekly_stats_te_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "weekly_stats_wr_tbl.parquet"), schema = 'prod', "weekly_stats_wr_tbl")
create_index(con = con, schema = 'prod', table = 'weekly_stats_wr_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "team_metadata_tbl.parquet"), schema = 'prod', "team_metadata_tbl")
create_index(con = con, schema = 'prod', table = 'team_metadata_tbl', id_cols = c("team_id", "team_abbr"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "id_map_tbl.parquet"), schema = 'prod', "id_map_tbl")
create_index(con = con, schema = 'prod', table = 'id_map_tbl', id_cols = c("gsis_id", "espn_id", "full_name"), unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "snapcount_weekly_tbl.parquet"), schema = 'prod', "snapcount_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'snapcount_weekly_tbl', id_cols = c("game_id","pfr_player_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "snapcount_season_tbl.parquet"), schema = 'prod', "snapcount_season_tbl")
create_index(con = con, schema = 'prod', table = 'snapcount_season_tbl', id_cols = c("season","pfr_player_id","team", "position"), unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "snapcount_career_tbl.parquet"), schema = 'prod', "snapcount_career_tbl")
create_index(con = con, schema = 'prod', table = 'snapcount_career_tbl', id_cols = c("pfr_player_id"), unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "espn_qbr_season_tbl.parquet"), schema = 'prod', "espn_qbr_season_tbl")
create_index(con = con, schema = 'prod', table = 'espn_qbr_season_tbl', id_cols = c("season","season_type","player_id"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "espn_qbr_career_tbl.parquet"), schema = 'prod', "espn_qbr_career_tbl")
create_index(con = con, schema = 'prod', table = 'espn_qbr_career_tbl', id_cols = c("player_id","season_type"), unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "def_team_stats_week_tbl.parquet"), schema = 'prod', "def_team_stats_week_tbl")
create_index(con = con, schema = 'prod', table = 'def_team_stats_week_tbl', id_cols = c("season","week","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "off_team_stats_week_tbl.parquet"), schema = 'prod', "off_team_stats_week_tbl")
create_index(con = con, schema = 'prod', table = 'off_team_stats_week_tbl', id_cols = c("season","week","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "off_team_stats_season_tbl.parquet"), schema = 'prod', "off_team_stats_season_tbl")
create_index(con = con, schema = 'prod', table = 'off_team_stats_season_tbl', id_cols = c("season","team"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "player_weekly_tbl.parquet"), schema = 'prod', "player_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'player_weekly_tbl', id_cols = c("season","season_type","week","player_id","stat_name", "stat_type"), unique = FALSE)

load_parquet_to_postgres(file.path(base_path, "player_season_tbl.parquet"), schema = 'prod', "player_season_tbl")
create_index(con = con, schema = 'prod', table = 'player_season_tbl', id_cols = c("season","season_type","player_id","name", "stat_name","agg_type"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "player_career_tbl.parquet"), schema = 'prod', "player_career_tbl")
create_index(con = con, schema = 'prod', table = 'player_career_tbl', id_cols = c("player_id","season_type","position","name","stat_name", "agg_type"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "team_weekly_rankings_tbl.parquet"), schema = 'prod', "team_weekly_rankings_tbl")
create_index(con = con, schema = 'prod', table = 'team_weekly_rankings_tbl', id_cols = c("season", "week", "team", "stat_name"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "team_weekly_tbl.parquet"), schema = 'prod', "team_weekly_tbl")
create_index(con = con, schema = 'prod', table = 'team_weekly_tbl', id_cols = c("season","season_type","week","team","stat_name", "stat_type"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "team_season_tbl.parquet"), schema = 'prod', "team_season_tbl")
create_index(con = con, schema = 'prod', table = 'team_season_tbl', id_cols = c("season","season_type","team","stat_name", "stat_type"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "team_career_tbl.parquet"), schema = 'prod', "team_career_tbl")
create_index(con = con, schema = 'prod', table = 'team_career_tbl', id_cols = c("team","season_type","stat_name", "stat_type"), unique = TRUE)

load_parquet_to_postgres(file.path(base_path, "team_strength_tbl.parquet"), schema = 'prod', "team_strength_tbl")
create_index(con = con, schema = 'prod', table = 'team_strength_tbl', id_cols = c("team", "season", "week"), unique = TRUE)

#source(file.path("etl", "R", "step3_sql", "step3_createIDx.R"))
source(file.path("etl", "R", "step3_sql", "step3_create_mv.R"))
