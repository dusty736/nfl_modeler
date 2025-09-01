# Helper to load one file ----
load_parquet_to_postgres <- function(parquet_path, schema = 'public', table_name) {
  cat("Loading:", parquet_path, "into table:", table_name, "\n")
  df <- as.data.frame(read_parquet(parquet_path))
  dbWriteTable(con, DBI::Id(schema = schema, table = table_name), df, overwrite = TRUE, row.names = FALSE)
}

# Database connection ----
con <- dbConnect(
  Postgres(),
  dbname = "nfl",
  host = "localhost",
  port = 5432,
  user = "nfl_user",
  password = "nfl_pass"
)

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

