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
  
  team_weekly_tbl                      = c("season","season_type","week","team","stat_name","stat_type"),
  team_season_tbl                      = c("season","season_type","team","stat_name","stat_type"),
  team_career_tbl                      = c("team","season_type","stat_name","stat_type"),
  
  player_weekly_tbl                    = c("season","season_type","week","player_id","stat_name","stat_type"),
  player_season_tbl                    = c("season","season_type","player_id","name","stat_name","agg_type"),
  player_career_tbl                    = c("player_id","season_type","position","name","stat_name","agg_type")
)

DBI::dbExecute(con, "SET lock_timeout = '5s'; SET statement_timeout = '0';")

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
dbExecute(con, "SET lock_timeout = '5s'; SET statement_timeout = '0';")

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
