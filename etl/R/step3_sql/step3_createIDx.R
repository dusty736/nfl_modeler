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

DBI::dbExecute(con, "CREATE INDEX idx_player_weekly_core ON public.player_weekly_tbl (season, season_type, week, stat_name, position, player_id);")
DBI::dbExecute(con, "CREATE INDEX idx_team_weekly_core ON public.team_weekly_tbl (season, season_type, week, stat_name, team);")

dbDisconnect(con)
