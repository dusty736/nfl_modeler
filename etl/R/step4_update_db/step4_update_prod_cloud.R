################################################################################
# step4_update_prod
################################################################################

# Source statements are removed as the functions are included above.
source(here("etl", "R", "step4_update_db", "step4_update_prod_functions.R"))
source(here("etl", "R", "step3_sql", "step3_parquet_to_postgres_functions.R"))
source(here("etl", "R", "step3_sql", "step3_database_file_prep_functions.R"))
source(here("etl", "R", "utils.R"))

# Function to connect to Cloud SQL using the proxy
Sys.setenv(
  DB_HOST = "localhost",
  DB_USER = "nfl_user", # Replace with your local user
  DB_PASS = "nfl_pass"  # Replace with your local password
)

con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "nfl",
  host = "/tmp/nfl-modeling:europe-west2:nfl-pg-01", 
  user = "nfl_app",
  password = "CHOOSE_A_STRONG_PASS"
)

################################################################################
# Update Prod
################################################################################

# Define the primary key map for each table to be updated
key_map <- list(
  games_tbl                           = c("game_id"),
  season_results_tbl                  = c("season","team_id"),
  weekly_results_tbl                  = c("game_id","team_id"),
  rosters_tbl                         = c("season","week","team_id","player_id"),
  roster_summary_tbl                  = c("season","team"),
  roster_position_summary_tbl         = c("season","team","position"),
  contracts_qb_tbl                    = c("gsis_id","team","year_signed"),
  contracts_position_cap_pct_tbl      = c("position","year_signed","team"),
  depth_charts_player_starts_tbl      = c("team","season","position","gsis_id"),
  depth_charts_position_stability_tbl = c("season","team","week","position"),
  depth_charts_qb_team_tbl            = c("season","week","team"),
  depth_charts_starters_tbl           = c("season","week","team","gsis_id"),
  team_weekly_tbl                     = c("season","season_type","week","team","stat_name","stat_type"),
  team_season_tbl                     = c("season","season_type","team","stat_name","stat_type"),
  team_career_tbl                     = c("team","season_type","stat_name","stat_type"),
  team_weekly_rankings_tbl            = c("season", "week", "team", "stat_name"),
  player_weekly_tbl                   = c("season","season_type","week","player_id","stat_name","stat_type"),
  player_season_tbl                   = c("season","season_type","player_id","name","stat_name","agg_type"),
  player_career_tbl                   = c("player_id","season_type","position","name","stat_name","agg_type"),
  id_map_tbl                          = c("gsis_id","espn_id","full_name"),
  game_level_modeling_tbl             = c("game_id", "season", "week")
)

# Set database connection parameters
DBI::dbExecute(con, "SET lock_timeout = '5s';")
DBI::dbExecute(con, "SET statement_timeout = '0';")

# Perform the upsert operation from stage to prod
upsert_all(con, key_map, src_schema = "stage", dest_schema = "prod")

################################################################################
# Update Materialized Views
################################################################################

positions <- c("QB","RB","WR","TE")

# The four MVs in prod
mvs <- c(
  "prod.player_weekly_qb_mv",
  "prod.player_weekly_rb_mv",
  "prod.player_weekly_wr_mv",
  "prod.player_weekly_te_mv"
)

# Refresh materialized views concurrently to avoid locking
for (mv in mvs) {
  message("Refreshing ", mv, " CONCURRENTLY…")
  tryCatch({
    dbExecute(con, glue::glue("REFRESH MATERIALIZED VIEW CONCURRENTLY {mv};"))
  }, error = function(e) {
    message("  Concurrent refresh failed for ", mv, ": ", e$message,
            " — falling back to blocking refresh.")
    dbExecute(con, glue::glue("REFRESH MATERIALIZED VIEW {mv};"))
  })
}

dbDisconnect(con)
