# Helper to load one file ----
load_parquet_to_postgres <- function(parquet_path, schema = 'public', table_name) {
  cat("Loading:", parquet_path, "into table:", table_name, "\n")
  df <- as.data.frame(read_parquet(parquet_path))
  dbWriteTable(con, DBI::Id(schema = schema, table = table_name), df, overwrite = TRUE, row.names = FALSE)
}

# Database connection ----
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

#con <- get_pg_con()
#on.exit(DBI::dbDisconnect(con), add = TRUE)

drop_schema <- function(con, schema) {
  sch <- DBI::dbQuoteIdentifier(con, schema)
  sql <- paste0("DROP SCHEMA IF EXISTS ", sch, " CASCADE;")
  DBI::dbExecute(con, sql)
  invisible(TRUE)
}

create_schema <- function(con, schema) {
  sch <- DBI::dbQuoteIdentifier(con, schema)
  sql <- paste0("CREATE SCHEMA IF NOT EXISTS ", sch, ";")
  DBI::dbExecute(con, sql)
  invisible(TRUE)
}

create_index <- function(con, schema, table, id_cols, unique = FALSE) {
  idx_name <- paste0("idx_", table, "_", paste(id_cols, collapse = "_"))
  table_sql <- DBI::dbQuoteIdentifier(con, DBI::Id(schema = schema, table = table))
  cols_sql  <- paste(sapply(id_cols, DBI::dbQuoteIdentifier, conn = con), collapse = ", ")
  sql <- paste0(
    "CREATE ", if (unique) "UNIQUE " else "", "INDEX IF NOT EXISTS ",
    DBI::dbQuoteIdentifier(con, idx_name), " ON ", table_sql, " (", cols_sql, ");"
  )
  DBI::dbExecute(con, sql)
}

