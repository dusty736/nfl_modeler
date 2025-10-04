# step3_create_idx.R
library(DBI)
library(RPostgres)
library(glue)
library(digest)

# con <- dbConnect(
#   Postgres(),
#   dbname = "nfl",
#   host = "localhost",
#   port = 5432,
#   user = "nfl_user",
#   password = "nfl_pass"
# )

# etl/R/lib/db.R
get_pg_con <- function() {
  # Env-driven, with sensible local defaults
  host <- Sys.getenv("DB_HOST", "127.0.0.1")  # Cloud SQL socket: /cloudsql/PROJECT:REGION:INSTANCE
  port <- as.integer(Sys.getenv("DB_PORT", "5432"))
  db   <- Sys.getenv("DB_NAME", "nfl")
  user <- Sys.getenv("DB_USER", "nfl_app")
  pass <- Sys.getenv("DB_PASS", "")
  
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = host,
    port     = port,
    dbname   = db,
    user     = user,
    password = pass,
    # For Unix socket connections SSL is not used
    sslmode  = "disable"
  )
}

con <- get_pg_con()
on.exit(DBI::dbDisconnect(con), add = TRUE)

# --- helpers ---------------------------------------------------------------

# Build a deterministic, length-safe index name
mk_index_name <- function(table, cols, prefix = "idx") {
  base <- paste0(prefix, "_", table, "_", paste(cols, collapse = "_"))
  if (nchar(base) <= 63) return(base)
  # truncate & add short hash
  paste0(substr(base, 1, 40), "_", substr(digest(base, algo = "xxhash64"), 1, 12))
}

# Create an index if it doesn't exist (btree by default)
create_index_if_missing <- function(con, table, cols,
                                    unique = FALSE,
                                    using = "btree",
                                    where = NULL,
                                    include = NULL,
                                    concurrently = FALSE) {
  stopifnot(length(cols) >= 1)
  idx_name <- mk_index_name(table, cols)
  
  cols_sql    <- paste(DBI::dbQuoteIdentifier(con, cols), collapse = ", ")
  table_sql   <- DBI::dbQuoteIdentifier(con, DBI::Id(schema = "public", table = table))
  include_sql <- if (length(include)) paste0(" INCLUDE (", paste(DBI::dbQuoteIdentifier(con, include), collapse = ", "), ")") else ""
  where_sql   <- if (length(where))   paste0(" WHERE ", where) else ""
  uniq_sql    <- if (unique) "UNIQUE " else ""
  conc_sql    <- if (concurrently) " CONCURRENTLY" else ""
  using_sql   <- if (length(using)) paste(" USING", using) else ""
  
  sql <- glue("
    CREATE {uniq_sql}INDEX IF NOT EXISTS {DBI::dbQuoteIdentifier(con, idx_name)}
    {conc_sql} ON {table_sql}{using_sql} ({SQL(cols_sql)}){include_sql}{where_sql};
  ")
  
  DBI::dbExecute(con, sql)
}

# Inspect DB to get columns per table
get_table_columns <- function(con, schema = "public") {
  DBI::dbGetQuery(con, "
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = $1
  ", params = list(schema))
}

# Heuristic: infer sensible multi-column indexes from the actual columns
infer_index_specs <- function(con) {
  meta <- get_table_columns(con)
  by_tbl <- split(meta$column_name, meta$table_name)
  specs <- list()
  
  add <- function(t, v) {
    if (all(v %in% by_tbl[[t]])) specs[[t]] <<- append(specs[[t]], list(v))
  }
  
  for (t in names(by_tbl)) {
    cols <- by_tbl[[t]]
    
    # play-level joins
    if (all(c("game_id","play_id") %in% cols)) add(t, c("game_id","play_id"))
    if ("game_id" %in% cols) add(t, c("game_id"))
    
    # weekly grains
    if (all(c("season","week") %in% cols)) {
      st <- c("season", if ("season_type" %in% cols) "season_type", "week")
      for (idc in c("team","team_id","player_id")) if (idc %in% cols) add(t, c(st, idc))
      if ("stat_name" %in% cols) {
        if (all(c("position","player_id") %in% cols)) add(t, c(st, "stat_name","position","player_id"))
        if ("team"    %in% cols) add(t, c(st, "stat_name","team"))
        if ("team_id" %in% cols) add(t, c(st, "stat_name","team_id"))
      }
    }
    
    # season-level grains
    if ("season" %in% cols && !("week" %in% cols)) {
      for (idc in c("team","team_id","player_id")) if (idc %in% cols)
        add(t, c("season", if ("season_type" %in% cols) "season_type", idc))
    }
    
    # table-specific
    if (t == "games_tbl") add(t, c("season","week"))
    if (t == "team_metadata_tbl") { add(t, c("team_id")); add(t, c("team_abbr")) }
    if (t == "id_map_tbl") {
      id_cols <- cols[grepl("_id$", cols) | grepl("_it_id$", cols)]
      for (idc in id_cols) add(t, c(idc))
    }
  }
  specs
}

# Apply all requested indexes
apply_index_specs <- function(con, index_specs) {
  for (tbl in names(index_specs)) {
    for (cols in index_specs[[tbl]]) {
      create_index_if_missing(con, tbl, cols)
    }
  }
}

# --- 1) Hand-written core indexes (minimal, explicit) ----------------------

core_specs <- list(
  player_weekly_tbl = list(
    c("season","season_type","week","stat_name","position","player_id"),
    c("player_id","season","season_type","week")
  ),
  team_weekly_tbl = list(
    c("season","season_type","week","stat_name","team"),
    c("team","season","season_type","week")
  ),
  pbp_tbl = list(
    c("game_id","play_id")
  ),
  games_tbl = list(
    c("game_id"),
    c("season","week")
  ),
  rosters_tbl = list(
    c("season","week","team_id"),
    c("season","week","player_id")
  ),
  weekly_results_tbl = list(
    c("game_id"),
    c("season","season_type","week","team_id")
  ),
  off_team_stats_week_tbl = list(
    c("season","week","team")
  ),
  def_team_stats_week_tbl = list(
    c("season","week","team")
  ),
  team_sesaon_tbl = list(  # keep name as loaded; fix to team_season_tbl once you correct the typo
    c("season","season_type","team")
  ),
  id_map_tbl = list(
    c("gsis_id"), c("pfr_id"), c("pff_id"), c("espn_id"),
    c("sportradar_id"), c("yahoo_id"), c("rotowire_id"), c("fantasy_data_id"),
    c("sleeper_id"), c("gsis_it_id"), c("smart_id")
  ),
  team_metadata_tbl = list(
    c("team_id"), c("team_abbr")
  )
)

# --- 2) Auto-infer the rest (optional; merges with core) -------------------

auto_specs <- infer_index_specs(con)

# Merge: keep core first (so you’re explicit for hot paths), then add any extras
merged_specs <- core_specs
for (t in names(auto_specs)) {
  if (!t %in% names(merged_specs)) {
    merged_specs[[t]] <- auto_specs[[t]]
  } else {
    # append any inferred combos not already present
    already <- lapply(merged_specs[[t]], paste, collapse = "|")
    for (cols in auto_specs[[t]]) {
      if (!(paste(cols, collapse="|") %in% already))
        merged_specs[[t]] <- append(merged_specs[[t]], list(cols))
    }
  }
}

# Create all indexes
apply_index_specs(con, merged_specs)

# Optional: refresh planner stats
DBI::dbExecute(con, "ANALYZE;")

dbDisconnect(con)
