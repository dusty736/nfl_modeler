################################################################################
# step4_update_prod
################################################################################

library(arrow)
library(here)
library(dplyr)
library(DBI)
library(RPostgres)

source(here("etl", "R", "step4_update_db", "step4_update_prod_functions.R"))
source(here("etl", "R", "step3_sql", "step3_parquet_to_postgres_functions.R"))
source(here("etl", "R", "step3_sql", "step3_database_file_prep_functions.R"))
source(here("etl", "R", "utils.R"))

################################################################################
# Update Prod
################################################################################

key_map <- list(
  pbp_tbl                              = c("game_id","play_id"),
  pbp_games_tbl                        = c("game_id","team"),
  games_tbl                            = c("game_id"),
  season_results_tbl                   = c("season","team_id"),
  weekly_results_tbl                   = c("game_id","team_id"),
  rosters_tbl                          = c("season","week","team_id","player_id"),
  roster_summary_tbl                   = c("season","team"),
  roster_position_summary_tbl          = c("season","team","position"),
  
  weekly_stats_qb_tbl                  = c("season","recent_team","season_type","week","player_id"),
  weekly_stats_rb_tbl                  = c("season","recent_team","season_type","week","player_id"),
  weekly_stats_wr_tbl                  = c("season","recent_team","season_type","week","player_id"),
  weekly_stats_te_tbl                  = c("season","recent_team","season_type","week","player_id"),
  
  season_stats_qb_tbl                  = c("season","player_id"),
  season_stats_rb_tbl                  = c("season","player_id"),
  season_stats_wr_tbl                  = c("season","player_id"),
  season_stats_te_tbl                  = c("season","player_id"),
  
  career_stats_qb_tbl                  = c("player_id"),
  career_stats_rb_tbl                  = c("player_id"),
  career_stats_wr_tbl                  = c("player_id"),
  career_stats_te_tbl                  = c("player_id"),
  
  injuries_weekly_tbl                  = c("season","week","team","gsis_id"),
  injuries_team_weekly_tbl             = c("season","week","team"),
  injuries_team_season_tbl             = c("season","team"),
  injuries_position_weekly_tbl         = c("season","week","team","position"),
  
  contracts_qb_tbl                     = c("gsis_id","team","year_signed"),
  contracts_position_cap_pct_tbl       = c("position","year_signed","team"),
  
  st_player_stats_weekly_tbl           = c("season","season_type","week","player_id"),
  st_player_stats_season_tbl           = c("season","player_id"),
  
  def_player_stats_weekly_tbl          = c("player_id","team","season","week"),
  def_player_stats_season_tbl          = c("player_id","season"),
  def_player_stats_career_tbl          = c("player_id"),
  def_team_stats_season_tbl            = c("team","season"),
  
  depth_charts_player_starts_tbl       = c("team","season","position","gsis_id"),
  depth_charts_position_stability_tbl  = c("season","team","week","position"),
  depth_charts_qb_team_tbl             = c("season","week","team"),
  depth_charts_starters_tbl            = c("season","week","team","gsis_id"),
  
  nextgen_stats_player_weekly_tbl      = c("season","season_type","week","player_gsis_id"),
  nextgen_stats_player_season_tbl      = c("season","player_gsis_id","team_abbr"),
  nextgen_stats_player_postseason_tbl  = c("player_gsis_id","team_abbr"),
  nextgen_stats_player_career_tbl      = c("player_gsis_id"),
  
  participation_offense_pbp_tbl        = c("game_id","play_id","team"),
  participation_offense_game_tbl       = c("game_id","team"),
  participation_offense_season_tbl     = c("season","team"),
  participation_defense_pbp_tbl        = c("game_id","play_id","defense_team"),
  participation_defense_game_tbl       = c("game_id","defense_team"),
  participation_defense_season_tbl     = c("season","defense_team"),
  
  id_map_tbl                           = c("gsis_id","espn_id","full_name"),
  
  snapcount_weekly_tbl                 = c("game_id","pfr_player_id"),
  snapcount_season_tbl                 = c("season","pfr_player_id","team","position"),
  snapcount_career_tbl                 = c("pfr_player_id"),
  
  espn_qbr_season_tbl                  = c("season","season_type","player_id"),
  espn_qbr_career_tbl                  = c("player_id","season_type"),
  
  def_team_stats_week_tbl              = c("season","week","team"),
  off_team_stats_week_tbl              = c("season","week","team"),
  off_team_stats_season_tbl            = c("season","team"),
  
  team_weekly_tbl                      = c("season","season_type","week","team","stat_name","stat_type"),
  team_season_tbl                      = c("season","season_type","team","stat_name","stat_type"),
  team_career_tbl                      = c("team","season_type","stat_name","stat_type"),
  
  player_weekly_tbl                    = c("season","season_type","week","player_id","stat_name","stat_type"),
  player_season_tbl                    = c("season","season_type","player_id","name","stat_name","agg_type"),
  player_career_tbl                    = c("player_id","season_type","position","name","stat_name","agg_type")
)

key_map <- list(
  games_tbl                            = c("game_id"),
  season_results_tbl                   = c("season","team_id"),
  weekly_results_tbl                   = c("game_id","team_id"),
  rosters_tbl                          = c("season","week","team_id","player_id"),
  roster_summary_tbl                   = c("season","team"),
  roster_position_summary_tbl          = c("season","team","position"),
  
  # injuries_weekly_tbl                  = c("season","week","team","gsis_id"),
  # injuries_team_weekly_tbl             = c("season","week","team"),
  # injuries_team_season_tbl             = c("season","team"),
  # injuries_position_weekly_tbl         = c("season","week","team","position"),
  
  contracts_qb_tbl                     = c("gsis_id","team","year_signed"),
  contracts_position_cap_pct_tbl       = c("position","year_signed","team"),
  
  depth_charts_player_starts_tbl       = c("team","season","position","gsis_id"),
  depth_charts_position_stability_tbl  = c("season","team","week","position"),
  depth_charts_qb_team_tbl             = c("season","week","team"),
  depth_charts_starters_tbl            = c("season","week","team","gsis_id"),
  
  team_weekly_tbl                      = c("season","season_type","week","team","stat_name","stat_type"),
  team_season_tbl                      = c("season","season_type","team","stat_name","stat_type"),
  team_career_tbl                      = c("team","season_type","stat_name","stat_type"),
  
  player_weekly_tbl                    = c("season","season_type","week","player_id","stat_name","stat_type"),
  player_season_tbl                    = c("season","season_type","player_id","name","stat_name","agg_type"),
  player_career_tbl                    = c("player_id","season_type","position","name","stat_name","agg_type"),
  
  id_map_tbl                           = c("gsis_id","espn_id","full_name")
)

DBI::dbExecute(con, "SET lock_timeout = '5s';")
DBI::dbExecute(con, "SET statement_timeout = '0';")

upsert_all(con, key_map, src_schema = "stage", dest_schema = "prod")

################################################################################
# Update MVR
################################################################################

positions <- c("QB","RB","WR","TE")

# The four MVs in prod
mvs <- c(
  "prod.player_weekly_qb_mv",
  "prod.player_weekly_rb_mv",
  "prod.player_weekly_wr_mv",
  "prod.player_weekly_te_mv"
)

# Optional: be polite with locks / timeouts
for (mv in mvs) {
  message("Refreshing ", mv, " CONCURRENTLY…")
  tryCatch({
    dbExecute(con, glue("REFRESH MATERIALIZED VIEW CONCURRENTLY {mv};"))
  }, error = function(e) {
    message("  Concurrent refresh failed for ", mv, ": ", e$message,
            " — falling back to blocking refresh.")
    dbExecute(con, glue("REFRESH MATERIALIZED VIEW {mv};"))
  })
}

dbDisconnect(con)
