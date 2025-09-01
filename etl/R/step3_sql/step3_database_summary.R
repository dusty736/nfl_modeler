# -------------------------------------------------------------------
# DB Specs Extractor (Postgres) — columns, types, keys, indexes, sizes
# Requires: RPostgres, DBI (no tidyverse required)
#
# Usage:
#   con <- db_connect_from_env()
#   specs <- collect_db_specs(con, schemas = c("prod","public","stg"))
#   # Example: view columns + key flags
#   head(specs$columns_enriched)
#   # Optionally write to disk:
#   # write.csv(specs$columns_enriched, "db_columns_enriched.csv", row.names = FALSE)
# -------------------------------------------------------------------

if (!requireNamespace("DBI", quietly = TRUE)) stop("Install DBI")
if (!requireNamespace("RPostgres", quietly = TRUE)) stop("Install RPostgres")

con <- dbConnect(
  Postgres(),
  dbname = "nfl",
  host = "localhost",
  port = 5432,
  user = "nfl_user",
  password = "nfl_pass"
)

sql_in_vec <- function(x) paste0("(", paste(sprintf("'%s'", gsub("'", "''", x)), collapse = ","), ")")

collect_db_specs <- function(con, schemas = c("public")) {
  if (length(schemas) == 0) stop("Provide at least one schema")
  in_schemas <- sql_in_vec(schemas)
  
  # --- Tables (BASE TABLEs + partitioned parents) ---
  tables <- DBI::dbGetQuery(con, sprintf("
    SELECT
      t.table_schema,
      t.table_name,
      t.table_type,
      obj_description(c.oid) AS table_comment
    FROM information_schema.tables t
    JOIN pg_catalog.pg_class c
      ON c.relname = t.table_name
      AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = t.table_schema)
    WHERE t.table_schema IN %s
      AND t.table_type IN ('BASE TABLE','PARTITIONED TABLE')
    ORDER BY t.table_schema, t.table_name;
  ", in_schemas))
  
  # --- Columns ---
  columns <- DBI::dbGetQuery(con, sprintf("
    SELECT
      c.table_schema,
      c.table_name,
      c.column_name,
      c.ordinal_position,
      c.is_nullable,
      c.data_type,
      c.udt_name,
      c.character_maximum_length,
      c.numeric_precision,
      c.numeric_scale,
      c.datetime_precision,
      c.column_default
    FROM information_schema.columns c
    WHERE c.table_schema IN %s
    ORDER BY c.table_schema, c.table_name, c.ordinal_position;
  ", in_schemas))
  
  # --- Primary keys ---
  pks <- DBI::dbGetQuery(con, sprintf("
    SELECT
      kcu.table_schema,
      kcu.table_name,
      tco.constraint_name,
      kcu.column_name
    FROM information_schema.table_constraints tco
    JOIN information_schema.key_column_usage kcu
      ON tco.constraint_name = kcu.constraint_name
      AND tco.table_schema = kcu.table_schema
    WHERE tco.constraint_type = 'PRIMARY KEY'
      AND kcu.table_schema IN %s
    ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;
  ", in_schemas))
  
  # --- Unique constraints (non-PK uniques included) ---
  uniques <- DBI::dbGetQuery(con, sprintf("
    SELECT
      kcu.table_schema,
      kcu.table_name,
      tco.constraint_name,
      kcu.column_name
    FROM information_schema.table_constraints tco
    JOIN information_schema.key_column_usage kcu
      ON tco.constraint_name = kcu.constraint_name
      AND tco.table_schema = kcu.table_schema
    WHERE tco.constraint_type = 'UNIQUE'
      AND kcu.table_schema IN %s
    ORDER BY kcu.table_schema, kcu.table_name, tco.constraint_name, kcu.ordinal_position;
  ", in_schemas))
  
  # --- Foreign keys ---
  fks <- DBI::dbGetQuery(con, sprintf("
    SELECT
      tc.table_schema,
      tc.table_name,
      tc.constraint_name,
      kcu.column_name,
      ccu.table_schema AS foreign_table_schema,
      ccu.table_name   AS foreign_table_name,
      ccu.column_name  AS foreign_column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema IN %s
    ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position NULLS LAST;
  ", in_schemas))
  
  # --- Indexes ---
  indexes <- DBI::dbGetQuery(con, sprintf("
    SELECT
      schemaname AS table_schema,
      tablename  AS table_name,
      indexname,
      indexdef
    FROM pg_indexes
    WHERE schemaname IN %s
    ORDER BY schemaname, tablename, indexname;
  ", in_schemas))
  
  # --- Check constraints ---
  checks <- DBI::dbGetQuery(con, sprintf("
    SELECT
      n.nspname AS table_schema,
      c.relname AS table_name,
      con.conname AS constraint_name,
      pg_get_constraintdef(con.oid) AS constraint_def
    FROM pg_constraint con
    JOIN pg_class c      ON c.oid = con.conrelid
    JOIN pg_namespace n  ON n.oid = c.relnamespace
    WHERE con.contype = 'c'
      AND n.nspname IN %s
    ORDER BY n.nspname, c.relname, con.conname;
  ", in_schemas))
  
  # --- Partitioned parents & children ---
  partition_parents <- DBI::dbGetQuery(con, sprintf("
    SELECT
      n.nspname AS table_schema,
      c.relname AS table_name,
      pg_get_partkeydef(c.oid) AS partition_key
    FROM pg_partitioned_table pt
    JOIN pg_class c     ON c.oid = pt.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN %s
    ORDER BY n.nspname, c.relname;
  ", in_schemas))
  
  partition_children <- DBI::dbGetQuery(con, sprintf("
    SELECT
      nc.nspname AS partition_schema,
      c.relname  AS partition_name,
      np.nspname AS parent_schema,
      p.relname  AS parent_table
    FROM pg_inherits i
    JOIN pg_class c      ON i.inhrelid  = c.oid
    JOIN pg_namespace nc ON c.relnamespace = nc.oid
    JOIN pg_class p      ON i.inhparent = p.oid
    JOIN pg_namespace np ON p.relnamespace = np.oid
    WHERE np.nspname IN %s
    ORDER BY np.nspname, p.relname, nc.nspname, c.relname;
  ", in_schemas))
  
  # --- Size & row estimates ---
  sizes <- DBI::dbGetQuery(con, sprintf("
    SELECT
      n.nspname AS table_schema,
      c.relname AS table_name,
      pg_total_relation_size(c.oid) AS total_bytes,
      pg_relation_size(c.oid)       AS table_bytes,
      pg_indexes_size(c.oid)        AS index_bytes,
      pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid) AS toast_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','p') AND n.nspname IN %s
    ORDER BY n.nspname, c.relname;
  ", in_schemas))
  
  row_estimates <- DBI::dbGetQuery(con, sprintf("
    SELECT
      n.nspname AS table_schema,
      c.relname AS table_name,
      c.reltuples::bigint AS row_estimate
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','p') AND n.nspname IN %s
    ORDER BY n.nspname, c.relname;
  ", in_schemas))
  
  # --- Enrich columns with key flags ---
  # Helper: fast left-join via merge (base R)
  flag_in <- function(df, key_df, by_cols, flag_name) {
    if (nrow(key_df) == 0) {
      df[[flag_name]] <- FALSE
      return(df)
    }
    key_df$.__flag__ <- TRUE
    out <- merge(df, key_df[, c(by_cols, ".__flag__")], by = by_cols, all.x = TRUE, sort = FALSE)
    out[[flag_name]] <- isTRUE(FALSE) # initialize
    out[[flag_name]] <- !is.na(out$.__flag__)
    out$.__flag__ <- NULL
    out
  }
  
  by <- c("table_schema","table_name","column_name")
  pk_cols <- unique(pks[, c("table_schema","table_name","column_name")])
  uq_cols <- unique(uniques[, c("table_schema","table_name","column_name")])
  fk_cols <- unique(fks[, c("table_schema","table_name","column_name")])
  
  cols_enriched <- columns
  cols_enriched <- flag_in(cols_enriched, pk_cols, by, "is_primary_key")
  cols_enriched <- flag_in(cols_enriched, uq_cols, by, "in_unique_constraint")
  cols_enriched <- flag_in(cols_enriched, fk_cols, by, "is_foreign_key")
  
  # Return everything
  list(
    tables               = tables,
    columns              = columns,
    primary_keys         = pks,
    unique_constraints   = uniques,
    foreign_keys         = fks,
    indexes              = indexes,
    check_constraints    = checks,
    partition_parents    = partition_parents,
    partition_children   = partition_children,
    sizes                = sizes,
    row_estimates        = row_estimates,
    columns_enriched     = cols_enriched
  )
}

# Optional helper: pretty byte formatting
format_bytes <- function(bytes) {
  units <- c("B","KB","MB","GB","TB","PB")
  pow <- ifelse(bytes > 0, floor(log(bytes, 1024)), 0)
  pow <- pmin(pow, length(units) - 1)
  sprintf("%.2f %s", bytes / (1024 ^ pow), units[pow + 1])
}

# Get the Specs
specs <- collect_db_specs(con, schemas = c("prod","public","stg"))
head(specs$columns_enriched)
DBI::dbDisconnect(con)

# Create contracts
table_names <- unique(specs$tables$table_name)
for (tbl in table_names) {
  file.create(file.path("documents", "etl_contracts", 
                        paste0(tbl, ".md")))
}



