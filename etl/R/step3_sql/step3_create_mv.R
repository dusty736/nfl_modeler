library(DBI)
library(RPostgres)
library(glue)

# Connect to Postgres
con <- dbConnect(
  Postgres(),
  dbname = "nfl",
  host = "localhost",
  port = 5432,
  user = "nfl_user",
  password = "nfl_pass"
)

# Define positions
positions <- c("QB", "RB", "WR", "TE")

# Function to check if a materialized view exists
mv_exists <- function(con, view_name) {
  res <- dbGetQuery(
    con,
    glue("SELECT COUNT(*) FROM pg_matviews WHERE matviewname = '{view_name}'")
  )
  res[[1]] > 0
}

# Create or refresh MVs
for (pos in positions) {
  view_name <- paste0("player_weekly_", tolower(pos), "_mv")
  
  if (mv_exists(con, view_name)) {
    message("Refreshing materialized view: ", view_name)
    dbExecute(con, glue("REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name};"))
  } else {
    message("Creating materialized view: ", view_name)
    
    dbExecute(con, glue("DROP MATERIALIZED VIEW IF EXISTS prod.{view_name} CASCADE;"))
    dbExecute(con, glue("
      CREATE MATERIALIZED VIEW prod.{view_name} AS
      SELECT distinct pwt.*,
             tmt.team_color,
             tmt.team_color2
      FROM prod.player_weekly_tbl pwt
      LEFT JOIN prod.team_metadata_tbl tmt
             ON pwt.team = tmt.team_abbr
      WHERE pwt.position = '{pos}'
        AND pwt.season BETWEEN 2019 AND 2025;
    "))
    
    message("Creating index on: ", view_name)
    dbExecute(con, glue("
      CREATE UNIQUE INDEX uq_{view_name} ON prod.{view_name} (player_id, season, season_type, week, stat_name, stat_type);
    "))
  }
}

dbDisconnect(con)
