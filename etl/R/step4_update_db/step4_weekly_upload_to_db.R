################################################################################
# step3_database_file_prep
################################################################################

library(arrow)
library(here)
library(dplyr)
library(DBI)
library(RPostgres)

source(here("etl", "R", "step3_sql", "step3_parquet_to_postgres_functions.R"))
source(here("etl", "R", "step3_sql", "step3_database_file_prep_functions.R"))
source(here("etl", "R", "utils.R"))

# Schema ----
drop_schema(con, schema = "stage")
create_schema(con, schema = "stage")

# Bulk load all tables ----
base_path <- "data/staging"

################################################################################
# Process Data
################################################################################

# PBP 
form_pbp_play_for_sql(file.path("data", "staging", "pbp.parquet"),
                      file.path("data", "staging", "pbp.parquet"))
load_parquet_to_postgres(file.path(base_path, "pbp.parquet"), schema = 'stage', "pbp_tbl")
create_index(con = con, schema = 'stage', table = 'pbp_tbl', id_cols = c("game_id","play_id"), unique = TRUE)

form_team_game_for_sql(file.path("data", "staging", "pbp_games.parquet"),
                       file.path("data", "staging", "pbp_games.parquet"))
load_parquet_to_postgres(file.path(base_path, "pbp_games.parquet"), schema = 'stage', "pbp_games_tbl")
create_index(con = con, schema = 'stage', table = 'pbp_games_tbl', id_cols = c("game_id", "team"), unique = TRUE)

# Games (games.parquet)
format_game_for_sql(file.path("data", "staging", "games.parquet"),
                    file.path("data", "staging", "games.parquet"))
load_parquet_to_postgres(file.path(base_path, "games.parquet"), schema = 'stage', "games_tbl")
create_index(con = con, schema = 'stage', table = 'games_tbl', id_cols = c('game_id'), unique = TRUE)

# Seasons (season_results.parquet)
format_season_for_sql(file.path("data", "staging", "season_results.parquet"),
                      file.path("data", "staging", "season_results.parquet"))
load_parquet_to_postgres(file.path(base_path, "season_results.parquet"), schema = 'stage', "season_results_tbl")
create_index(con = con, schema = 'stage', table = 'season_results_tbl', id_cols = c("season","team_id") , unique = TRUE)

# Weeks (weekly_results.parquet)
format_weeks_for_sql(file.path("data", "staging", "weekly_results.parquet"),
                     file.path("data", "staging", "weekly_results.parquet"))
load_parquet_to_postgres(file.path(base_path, "weekly_results.parquet"), schema = 'stage', "weekly_results_tbl")
create_index(con = con, schema = 'stage', table = 'weekly_results_tbl', id_cols = c("game_id","team_id"), unique = TRUE)

# Roster (rosters.parquet)
format_roster_for_sql(file.path("data", "staging", "rosters.parquet"),
                      file.path("data", "staging", "rosters.parquet"))
load_parquet_to_postgres(file.path(base_path, "rosters.parquet"), schema = 'stage', "rosters_tbl")
create_index(con = con, schema = 'stage', table = 'rosters_tbl', id_cols = c("season","week","team_id","player_id"), unique = TRUE)

# Roster Summary (roster_summary.parquet)
format_roster_summary_for_sql(file.path("data", "staging", "roster_summary.parquet"),
                              file.path("data", "staging", "roster_summary.parquet"))
load_parquet_to_postgres(file.path(base_path, "roster_summary.parquet"), schema = 'stage', "roster_summary_tbl")
create_index(con = con, schema = 'stage', table = 'roster_summary_tbl', id_cols = c("season","team"), unique = TRUE)

# Roster Position Summary (roster_position_summary.parquet)
format_roster_position_summary_for_sql(file.path("data", "staging", "roster_position_summary.parquet"),
                                       file.path("data", "staging", "roster_position_summary.parquet"))
load_parquet_to_postgres(file.path(base_path, "roster_position_summary.parquet"), schema = 'stage', "roster_position_summary_tbl")
create_index(con = con, schema = 'stage', table = 'roster_position_summary_tbl', id_cols = c("season","team","position"), unique = TRUE)

# Weekly Qb Stats (weekly_stats_qb.parquet)
format_weekly_qb_stats_for_sql(file.path("data", "staging", "weekly_stats_qb.parquet"),
                               file.path("data", "staging", "weekly_stats_qb.parquet"))
load_parquet_to_postgres(file.path(base_path, "weekly_stats_qb.parquet"), schema = 'stage', "weekly_stats_qb_tbl")
create_index(con = con, schema = 'stage', table = 'weekly_stats_qb_tbl', id_cols = c("season", "recent_team", "season_type","week","player_id"), unique = TRUE)

# Weekly Rb Stats (weekly_stats_rb.parquet)
format_weekly_rb_stats_for_sql(file.path("data", "staging", "weekly_stats_rb.parquet"),
                               file.path("data", "staging", "weekly_stats_rb.parquet"))
load_parquet_to_postgres(file.path(base_path, "weekly_stats_rb.parquet"), schema = 'stage', "weekly_stats_rb_tbl")
create_index(con = con, schema = 'stage', table = 'weekly_stats_rb_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

# Weekly Wrte Stats (weekly_stats_wr.parquet)
format_weekly_wrte_stats_for_sql(file.path("data", "staging", "weekly_stats_wr.parquet"),
                                 file.path("data", "staging", "weekly_stats_wr.parquet"))
load_parquet_to_postgres(file.path(base_path, "weekly_stats_wr.parquet"), schema = 'stage', "weekly_stats_wr_tbl")
create_index(con = con, schema = 'stage', table = 'weekly_stats_wr_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

# Weekly Wrte Stats (weekly_stats_te.parquet)
format_weekly_wrte_stats_for_sql(file.path("data", "staging", "weekly_stats_te.parquet"),
                                 file.path("data", "staging", "weekly_stats_te.parquet"))
load_parquet_to_postgres(file.path(base_path, "weekly_stats_te.parquet"), schema = 'stage', "weekly_stats_te_tbl")
create_index(con = con, schema = 'stage', table = 'weekly_stats_te_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

# Season Qb Stats (season_stats_qb.parquet)
format_season_qb_stats_for_sql(file.path("data", "staging", "season_stats_qb.parquet"),
                               file.path("data", "staging", "season_stats_qb.parquet"))
load_parquet_to_postgres(file.path(base_path, "season_stats_qb.parquet"), schema = 'stage', "season_stats_qb_tbl")
create_index(con = con, schema = 'stage', table = 'season_stats_qb_tbl', id_cols = c("season","player_id"), unique = TRUE)

# Season Rb Stats (season_stats_rb.parquet)
format_season_rb_stats_for_sql(file.path("data", "staging", "season_stats_rb.parquet"),
                               file.path("data", "staging", "season_stats_rb.parquet"))
load_parquet_to_postgres(file.path(base_path, "season_stats_rb.parquet"), schema = 'stage', "season_stats_rb_tbl")
create_index(con = con, schema = 'stage', table = 'season_stats_rb_tbl', id_cols = c("season","player_id"), unique = TRUE)

# Season Wrte Stats (season_stats_wr.parquet)
format_season_wrte_stats_for_sql(file.path("data", "staging", "season_stats_wr.parquet"),
                                 file.path("data", "staging", "season_stats_wr.parquet"))
load_parquet_to_postgres(file.path(base_path, "season_stats_wr.parquet"), schema = 'stage', "season_stats_wr_tbl")
create_index(con = con, schema = 'stage', table = 'season_stats_wr_tbl', id_cols = c("season","player_id"), unique = TRUE)

# Season Wrte Stats (season_stats_te.parquet)
format_season_wrte_stats_for_sql(file.path("data", "staging", "season_stats_te.parquet"),
                                 file.path("data", "staging", "season_stats_te.parquet"))
load_parquet_to_postgres(file.path(base_path, "season_stats_te.parquet"), schema = 'stage', "season_stats_te_tbl")
create_index(con = con, schema = 'stage', table = 'season_stats_te_tbl', id_cols = c("season","player_id"), unique = TRUE)

# Career Qb Stats (career_stats_qb.parquet)
format_career_qb_stats_for_sql(file.path("data", "staging", "career_stats_qb.parquet"),
                               file.path("data", "staging", "career_stats_qb.parquet"))
load_parquet_to_postgres(file.path(base_path, "career_stats_qb.parquet"), schema = 'stage', "career_stats_qb_tbl")
create_index(con = con, schema = 'stage', table = 'career_stats_qb_tbl', id_cols = c('player_id'), unique = TRUE)

# Career Rb Stats (career_stats_rb.parquet)
format_career_rb_stats_for_sql(file.path("data", "staging", "career_stats_rb.parquet"),
                               file.path("data", "staging", "career_stats_rb.parquet"))
load_parquet_to_postgres(file.path(base_path, "career_stats_rb.parquet"), schema = 'stage', "career_stats_rb_tbl")
create_index(con = con, schema = 'stage', table = 'career_stats_rb_tbl', id_cols = c('player_id'), unique = TRUE)

# Career Wrte Stats (career_stats_wr.parquet)
format_career_wrte_stats_for_sql(file.path("data", "staging", "career_stats_wr.parquet"),
                                 file.path("data", "staging", "career_stats_wr.parquet"))
load_parquet_to_postgres(file.path(base_path, "career_stats_wr.parquet"), schema = 'stage', "career_stats_wr_tbl")
create_index(con = con, schema = 'stage', table = 'career_stats_wr_tbl', id_cols = c('player_id'), unique = TRUE)

# Career Wrte Stats (career_stats_te.parquet)
format_career_wrte_stats_for_sql(file.path("data", "staging", "career_stats_te.parquet"),
                                 file.path("data", "staging", "career_stats_te.parquet"))
load_parquet_to_postgres(file.path(base_path, "career_stats_te.parquet"), schema = 'stage', "career_stats_te_tbl")
create_index(con = con, schema = 'stage', table = 'career_stats_te_tbl', id_cols = c('player_id'), unique = TRUE)

# Injuries Weekly (injuries_weekly.parquet)
format_injuries_weekly_for_sql(file.path("data", "staging", "injuries_weekly.parquet"),
                               file.path("data", "staging", "injuries_weekly.parquet"))
load_parquet_to_postgres(file.path(base_path, "injuries_weekly.parquet"), schema = 'stage', "injuries_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'injuries_weekly_tbl', id_cols = c("season","week","team","gsis_id"), unique = FALSE)

# Injuries Team Weekly (injuries_team_weekly.parquet)
format_injuries_team_weekly_for_sql(file.path("data", "staging", "injuries_team_weekly.parquet"),
                                    file.path("data", "staging", "injuries_team_weekly.parquet"))
load_parquet_to_postgres(file.path(base_path, "injuries_team_weekly.parquet"), schema = 'stage', "injuries_team_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'injuries_team_weekly_tbl', id_cols = c("season","week","team"), unique = FALSE)

# Injuries Team Season (injuries_team_season.parquet)
format_injuries_team_season_for_sql(file.path("data", "staging", "injuries_team_season.parquet"),
                                    file.path("data", "staging", "injuries_team_season.parquet"))
load_parquet_to_postgres(file.path(base_path, "injuries_team_season.parquet"), schema = 'stage', "injuries_team_season_tbl")
create_index(con = con, schema = 'stage', table = 'injuries_team_season_tbl', id_cols = c("season","team") , unique = TRUE)

# Injuries Team Position Weekly (injuries_position_weekly.parquet)
format_injuries_team_position_weekly_for_sql(file.path("data", "staging", "injuries_position_weekly.parquet"),
                                             file.path("data", "staging", "injuries_position_weekly.parquet"))
load_parquet_to_postgres(file.path(base_path, "injuries_position_weekly.parquet"), schema = 'stage', "injuries_position_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'injuries_position_weekly_tbl', id_cols = c("season","week","team","position"), unique = TRUE)

# Contracts Qb (contracts_qb.parquet)
format_contracts_qb_for_sql(file.path("data", "staging", "contracts_qb.parquet"),
                            file.path("data", "staging", "contracts_qb.parquet"))
load_parquet_to_postgres(file.path(base_path, "contracts_qb.parquet"), schema = 'stage', "contracts_qb_tbl")
create_index(con = con, schema = 'stage', table = 'contracts_qb_tbl', id_cols = c("gsis_id", "team", "year_signed") , unique = FALSE)

# Contracts Cap Pct (contracts_position_cap_pct.parquet)
format_contracts_cap_pct_for_sql(file.path("data", "staging", "contracts_position_cap_pct.parquet"),
                                 file.path("data", "staging", "contracts_position_cap_pct.parquet"))
load_parquet_to_postgres(file.path(base_path, "contracts_position_cap_pct.parquet"), schema = 'stage', "contracts_position_cap_pct_tbl")
create_index(con = con, schema = 'stage', table = 'contracts_position_cap_pct_tbl', id_cols = c("position","year_signed","team"), unique = TRUE)

# Special Teams Weekly (st_player_stats_weekly.parquet)
format_weekly_special_teams_for_sql(file.path("data", "staging", "st_player_stats_weekly.parquet"),
                                    file.path("data", "staging", "st_player_stats_weekly.parquet"))
load_parquet_to_postgres(file.path(base_path, "st_player_stats_weekly.parquet"), schema = 'stage', "st_player_stats_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'st_player_stats_weekly_tbl', id_cols = c("season","season_type","week","player_id"), unique = FALSE)

# Special Teams Season (st_player_stats_season.parquet)
format_season_special_teams_for_sql(file.path("data", "staging", "st_player_stats_season.parquet"),
                                    file.path("data", "staging", "st_player_stats_season.parquet"))
load_parquet_to_postgres(file.path(base_path, "st_player_stats_season.parquet"), schema = 'stage', "st_player_stats_season_tbl")
create_index(con = con, schema = 'stage', table = 'st_player_stats_season_tbl', id_cols = c("season","player_id"), unique = TRUE)

# Defensive Players (def_player_stats_weekly.parquet)
format_weekly_defense_player_stats_for_sql(
  file.path("data", "staging", "def_player_stats_weekly.parquet"),
  file.path("data", "staging", "def_player_stats_weekly.parquet")
)
load_parquet_to_postgres(file.path(base_path, "def_player_stats_weekly.parquet"), schema = 'stage', "def_player_stats_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'def_player_stats_weekly_tbl', id_cols = c('player_id','team','season','week'), unique = TRUE)

# Defensive Players (def_player_stats_season.parquet)
format_season_defense_player_stats_for_sql(
  file.path("data", "staging", "def_player_stats_season.parquet"),
  file.path("data", "staging", "def_player_stats_season.parquet")
)
load_parquet_to_postgres(file.path(base_path, "def_player_stats_season.parquet"), schema = 'stage', "def_player_stats_season_tbl")
create_index(con = con, schema = 'stage', table = 'def_player_stats_season_tbl', id_cols = c('player_id','season'), unique = TRUE)

# Defensive Players (def_player_stats_career.parquet)
format_career_defense_player_stats_for_sql(
  file.path("data", "staging", "def_player_stats_career.parquet"),
  file.path("data", "staging", "def_player_stats_career.parquet")
)
load_parquet_to_postgres(file.path(base_path, "def_player_stats_career.parquet"), schema = 'stage', "def_player_stats_career_tbl")
create_index(con = con, schema = 'stage', table = 'def_player_stats_career_tbl', id_cols = c('player_id'), unique = TRUE)

# Defensive Teams (def_team_stats_season.parquet)
format_season_defense_team_stats_for_sql(
  file.path("data", "staging", "def_team_stats_season.parquet"),
  file.path("data", "staging", "def_team_stats_season.parquet")
)
load_parquet_to_postgres(file.path(base_path, "def_team_stats_season.parquet"), schema = 'stage', "def_team_stats_season_tbl")
create_index(con = con, schema = 'stage', table = 'def_team_stats_season_tbl', id_cols = c('team','season'), unique = TRUE)

# Depth Charts (depth_charts_player_starts.parquet)
format_depth_chart_player_starts_for_sql(
  file.path("data", "staging", "depth_charts_player_starts.parquet"),
  file.path("data", "staging", "depth_charts_player_starts.parquet")
)
load_parquet_to_postgres(file.path(base_path, "depth_charts_player_starts.parquet"), schema = 'stage', "depth_charts_player_starts_tbl")
create_index(con = con, schema = 'stage', table = 'depth_charts_player_starts_tbl', id_cols = c('team','season','position','gsis_id'), unique = TRUE)

# Depth Charts (depth_charts_position_stability.parquet)
format_depth_chart_position_stability_for_sql(
  file.path("data", "staging", "depth_charts_position_stability.parquet"),
  file.path("data", "staging", "depth_charts_position_stability.parquet")
)
load_parquet_to_postgres(file.path(base_path, "depth_charts_position_stability.parquet"), schema = 'stage', "depth_charts_position_stability_tbl")
create_index(con = con, schema = 'stage', table = 'depth_charts_position_stability_tbl', id_cols = c('season','team','week','position'), unique = TRUE)

# Depth Charts (depth_charts_qb_team.parquet)
format_depth_chart_qb_for_sql(
  file.path("data", "staging", "depth_charts_qb_team.parquet"),
  file.path("data", "staging", "depth_charts_qb_team.parquet")
)
load_parquet_to_postgres(file.path(base_path, "depth_charts_qb_team.parquet"), schema = 'stage', "depth_charts_qb_team_tbl")
create_index(con = con, schema = 'stage', table = 'depth_charts_qb_team_tbl', id_cols = c("season","week","team") , unique = FALSE)

# Depth Charts (depth_charts_starters.parquet)
format_depth_chart_starters_for_sql(
  file.path("data", "staging", "depth_charts_starters.parquet"),
  file.path("data", "staging", "depth_charts_starters.parquet")
)
load_parquet_to_postgres(file.path(base_path, "depth_charts_starters.parquet"), schema = 'stage', "depth_charts_starters_tbl")
create_index(con = con, schema = 'stage', table = 'depth_charts_starters_tbl', id_cols = c("season","week","team","gsis_id") , unique = FALSE)

# Next Gen Stats (nextgen_stats_player_weekly.parquet)
format_weekly_nextgen_stats_for_sql(
  file.path("data", "staging", "nextgen_stats_player_weekly.parquet"),
  file.path("data", "staging", "nextgen_stats_player_weekly.parquet")
)
load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_weekly.parquet"), schema = 'stage', "nextgen_stats_player_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'nextgen_stats_player_weekly_tbl', id_cols = c("season","season_type","week","player_gsis_id"), unique = TRUE)

# Next Gen Stats (nextgen_stats_player_season.parquet)
format_season_nextgen_stats_for_sql(
  file.path("data", "staging", "nextgen_stats_player_season.parquet"),
  file.path("data", "staging", "nextgen_stats_player_season.parquet")
)
load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_season.parquet"), schema = 'stage', "nextgen_stats_player_season_tbl")
create_index(con = con, schema = 'stage', table = 'nextgen_stats_player_season_tbl', id_cols = c("season","player_gsis_id","team_abbr"), unique = TRUE)

# Next Gen Stats (nextgen_stats_player_postseason.parquet)
format_postseason_nextgen_stats_for_sql(
  file.path("data", "staging", "nextgen_stats_player_postseason.parquet"),
  file.path("data", "staging", "nextgen_stats_player_postseason.parquet")
)
load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_postseason.parquet"), schema = 'stage', "nextgen_stats_player_postseason_tbl")
create_index(con = con, schema = 'stage', table = 'nextgen_stats_player_postseason_tbl', id_cols = c("player_gsis_id","team_abbr"), unique = TRUE)

# Next Gen Stats (nextgen_stats_player_career.parquet)
format_career_nextgen_stats_for_sql(
  file.path("data", "staging", "nextgen_stats_player_career.parquet"),
  file.path("data", "staging", "nextgen_stats_player_career.parquet")
)
load_parquet_to_postgres(file.path(base_path, "nextgen_stats_player_career.parquet"), schema = 'stage', "nextgen_stats_player_career_tbl")
create_index(con = con, schema = 'stage', table = 'nextgen_stats_player_career_tbl', id_cols = c("player_gsis_id"), unique = TRUE)

# Participation Offense (participation_offense_pbp.parquet)
format_participation_offense_pbp_for_sql(
  file.path("data", "staging", "participation_offense_pbp.parquet"),
  file.path("data", "staging", "participation_offense_pbp.parquet")
)
load_parquet_to_postgres(file.path(base_path, "participation_offense_pbp.parquet"), schema = 'stage', "participation_offense_pbp_tbl")
create_index(con = con, schema = 'stage', table = 'participation_offense_pbp_tbl', id_cols = c("game_id","play_id","team"), unique = TRUE)

# Participation Offense (participation_offense_game.parquet)
format_participation_offense_game_for_sql(
  file.path("data", "staging", "participation_offense_game.parquet"),
  file.path("data", "staging", "participation_offense_game.parquet")
)
load_parquet_to_postgres(file.path(base_path, "participation_offense_game.parquet"), schema = 'stage', "participation_offense_game_tbl")
create_index(con = con, schema = 'stage', table = 'participation_offense_game_tbl', id_cols = c("game_id","team"), unique = TRUE)

# Participation Offense (participation_offense_season.parquet)
format_participation_offense_season_for_sql(
  file.path("data", "staging", "participation_offense_season.parquet"),
  file.path("data", "staging", "participation_offense_season.parquet")
)
load_parquet_to_postgres(file.path(base_path, "participation_offense_season.parquet"), schema = 'stage', "participation_offense_season_tbl")
create_index(con = con, schema = 'stage', table = 'participation_offense_season_tbl', id_cols = c("season","team"), unique = TRUE)

# Participation Defense (participation_defense_pbp.parquet)
format_participation_defense_pbp_for_sql(
  file.path("data", "staging", "participation_defense_pbp.parquet"),
  file.path("data", "staging", "participation_defense_pbp.parquet")
)
load_parquet_to_postgres(file.path(base_path, "participation_defense_pbp.parquet"), schema = 'stage', "participation_defense_pbp_tbl")
create_index(con = con, schema = 'stage', table = 'participation_defense_pbp_tbl', id_cols = c("game_id","play_id","defense_team"), unique = TRUE)

# Participation Defense (participation_defense_game.parquet)
format_participation_defense_game_for_sql(
  file.path("data", "staging", "participation_defense_game.parquet"),
  file.path("data", "staging", "participation_defense_game.parquet")
)
load_parquet_to_postgres(file.path(base_path, "participation_defense_game.parquet"), schema = 'stage', "participation_defense_game_tbl")
create_index(con = con, schema = 'stage', table = 'participation_defense_game_tbl', id_cols = c("game_id","defense_team"), unique = TRUE)

# Participation Defense (participation_defense_season.parquet)
format_participation_defense_season_for_sql(
  file.path("data", "staging", "participation_defense_season.parquet"),
  file.path("data", "staging", "participation_defense_season.parquet")
)
load_parquet_to_postgres(file.path(base_path, "participation_defense_season.parquet"), schema = 'stage', "participation_defense_season_tbl")
create_index(con = con, schema = 'stage', table = 'participation_defense_season_tbl', id_cols = c("season","defense_team"), unique = TRUE)

# ID Map
format_id_map_for_sql(
  file.path("data", "staging", "id_map.parquet"),
  file.path("data", "staging", "id_map.parquet")
)
load_parquet_to_postgres(file.path(base_path, "id_map.parquet"), schema = 'stage', "id_map_tbl")
create_index(con = con, schema = 'stage', table = 'id_map_tbl', id_cols = c("gsis_id", "espn_id", "full_name"), unique = FALSE)

# Snapcount
format_snapcount_weekly_for_sql(
  file.path("data", "staging", "snapcount_weekly.parquet"),
  file.path("data", "staging", "snapcount_weekly.parquet")
)
load_parquet_to_postgres(file.path(base_path, "snapcount_weekly.parquet"), schema = 'stage', "snapcount_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'snapcount_weekly_tbl', id_cols = c("game_id","pfr_player_id"), unique = TRUE)

# Snapcount Season
format_snapcount_season_for_sql(
  file.path("data", "staging", "snapcount_season.parquet"),
  file.path("data", "staging", "snapcount_season.parquet")
)
load_parquet_to_postgres(file.path(base_path, "snapcount_season.parquet"), schema = 'stage', "snapcount_season_tbl")
create_index(con = con, schema = 'stage', table = 'snapcount_season_tbl', id_cols = c("season","pfr_player_id","team", "position"), unique = FALSE)

# Snapcount Career
format_snapcount_career_for_sql(
  file.path("data", "staging", "snapcount_career.parquet"),
  file.path("data", "staging", "snapcount_career.parquet")
)
load_parquet_to_postgres(file.path(base_path, "snapcount_career.parquet"), schema = 'stage', "snapcount_career_tbl")
create_index(con = con, schema = 'stage', table = 'snapcount_career_tbl', id_cols = c("pfr_player_id"), unique = FALSE)

# ESPN QBR
format_espn_qbr_season_for_sql(
  file.path("data", "staging", "espn_qbr_season.parquet"),
  file.path("data", "staging", "espn_qbr_season.parquet")
)
load_parquet_to_postgres(file.path(base_path, "espn_qbr_season.parquet"), schema = 'stage', "espn_qbr_season_tbl")
create_index(con = con, schema = 'stage', table = 'espn_qbr_season_tbl', id_cols = c("season","season_type","player_id"), unique = TRUE)

# ESPN QBR Career
format_espn_qbr_career_for_sql(
  file.path("data", "staging", "espn_qbr_career.parquet"),
  file.path("data", "staging", "espn_qbr_career.parquet")
)
load_parquet_to_postgres(file.path(base_path, "espn_qbr_career.parquet"), schema = 'stage', "espn_qbr_career_tbl")
create_index(con = con, schema = 'stage', table = 'espn_qbr_career_tbl', id_cols = c("player_id","season_type"), unique = FALSE)

# Defense Team Weekly
form_def_team_weekly_for_sql(
  file.path("data", "staging", "def_team_stats_week.parquet"),
  file.path("data", "staging", "def_team_stats_week.parquet")
)
load_parquet_to_postgres(file.path(base_path, "def_team_stats_week.parquet"), schema = 'stage', "def_team_stats_week_tbl")
create_index(con = con, schema = 'stage', table = 'def_team_stats_week_tbl', id_cols = c("season","week","team"), unique = TRUE)

# Offense Team Weekly
form_off_team_weekly_for_sql(
  file.path("data", "staging", "off_team_stats_week.parquet"),
  file.path("data", "staging", "off_team_stats_week.parquet")
)
load_parquet_to_postgres(file.path(base_path, "off_team_stats_week.parquet"), schema = 'stage', "off_team_stats_week_tbl")
create_index(con = con, schema = 'stage', table = 'off_team_stats_week_tbl', id_cols = c("season","week","team"), unique = TRUE)

# Offense Team Seasonally
form_off_team_season_for_sql(
  file.path("data", "staging", "off_team_stats_season.parquet"),
  file.path("data", "staging", "off_team_stats_season.parquet")
)
load_parquet_to_postgres(file.path(base_path, "off_team_stats_season.parquet"), schema = 'stage', "off_team_stats_season_tbl")
create_index(con = con, schema = 'stage', table = 'off_team_stats_season_tbl', id_cols = c("season","team"), unique = TRUE)

# Team weekly long
load_parquet_to_postgres(file.path(base_path, "team_weekly_tbl.parquet"), schema = 'stage', "team_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'team_weekly_tbl', id_cols = c("season","season_type","week","team","stat_name", "stat_type"), unique = TRUE)

# Team season long
parquet_to_postgres(file.path(base_path, "team_season_tbl.parquet"), schema = 'stage', "team_season_tbl")
create_index(con = con, schema = 'stage', table = 'team_season_tbl', id_cols = c("season","season_type","team","stat_name", "stat_type"), unique = TRUE)

# Team total long
load_parquet_to_postgres(file.path(base_path, "team_career_tbl.parquet"), schema = 'stage', "team_career_tbl")
create_index(con = con, schema = 'stage', table = 'team_career_tbl', id_cols = c("team","season_type","stat_name", "stat_type"), unique = TRUE)

# Player weekly long
load_parquet_to_postgres(file.path(base_path, "player_weekly_tbl.parquet"), schema = 'stage', "player_weekly_tbl")
create_index(con = con, schema = 'stage', table = 'player_weekly_tbl', id_cols = c("season","season_type","week","player_id","stat_name", "stat_type"), unique = FALSE)

# Player season long
load_parquet_to_postgres(file.path(base_path, "player_season_tbl.parquet"), schema = 'stage', "player_season_tbl")
create_index(con = con, schema = 'stage', table = 'player_season_tbl', id_cols = c("season","season_type","player_id","name", "stat_name","agg_type"), unique = TRUE)

# Player total long
load_parquet_to_postgres(file.path(base_path, "player_career_tbl.parquet"), schema = 'stage', "player_career_tbl")
create_index(con = con, schema = 'stage', table = 'player_career_tbl', id_cols = c("player_id","season_type","position","name","stat_name", "agg_type"), unique = TRUE)



