# upsert_stage_to_prod.R
library(DBI)
library(glue)

# Return columns (in order) for a table
get_cols <- function(con, schema, table) {
  DBI::dbGetQuery(
    con,
    "SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position",
    params = list(schema, table)
  )$column_name
}

# Return list of unique index column sets for (schema.table)
unique_index_colsets <- function(con, schema, table) {
  sql <- "
  SELECT idx.indexrelid::regclass AS index_name,
         array_agg(att.attname ORDER BY k.n) AS cols
    FROM pg_index idx
    JOIN pg_class t   ON t.oid = idx.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS k(attnum, n) ON TRUE
    JOIN pg_attribute att ON att.attrelid = t.oid AND att.attnum = k.attnum
   WHERE n.nspname = $1
     AND t.relname = $2
     AND idx.indisunique
   GROUP BY idx.indexrelid"
  res <- DBI::dbGetQuery(con, sql, params = list(schema, table))
  lapply(res$cols, identity)
}

# Does a unique index/constraint exist on exactly these key cols (order-insensitive)?
has_matching_unique_index <- function(con, schema, table, key_cols) {
  target <- sort(key_cols)
  any(vapply(unique_index_colsets(con, schema, table),
             function(x) identical(sort(unname(x)), target),
             logical(1)))
}

# Quote idents as a comma-separated list
qcsv <- function(con, idents) paste(DBI::dbQuoteIdentifier(con, idents), collapse = ", ")

# Build tuple expressions like: ("c1","c2") and (EXCLUDED."c1",EXCLUDED."c2")
tuple_expr <- function(con, cols, prefix = NULL) {
  left <- DBI::dbQuoteIdentifier(con, cols)
  if (!is.null(prefix)) left <- paste0(prefix, ".", left)
  paste0("(", paste(left, collapse = ", "), ")")
}

# Main upsert for one table
upsert_table <- function(con,
                         table,
                         key_cols,
                         src_schema  = "stage",
                         dest_schema = "prod",
                         stage_filter = NULL,      # e.g., "season >= 2024"
                         analyze = TRUE) {
  stopifnot(length(key_cols) >= 1)
  
  # Discover column list from DEST (prod) to enforce exact cdbQuoteIdentifierolumn ordering
  all_cols <- get_cols(con, dest_schema, table)
  if (length(all_cols) == 0) stop(glue("No columns found for {dest_schema}.{table}"))
  
  # Sanity: keys must exist in the table
  if (!all(key_cols %in% all_cols)) {
    missing <- paste(setdiff(key_cols, all_cols), collapse = ", ")
    stop(glue("Key columns not found in {dest_schema}.{table}: {missing}"))
  }
  
  non_keys <- setdiff(all_cols, key_cols)
  
  q_dest <- paste(DBI::dbQuoteIdentifier(con, c(dest_schema, table)), collapse = ".")
  q_src  <- paste(DBI::dbQuoteIdentifier(con, c(src_schema,  table)), collapse = ".")
  q_cols <- qcsv(con, all_cols)
  q_keys <- qcsv(con, key_cols)
  
  where_src <- if (!is.null(stage_filter)) paste0(" WHERE ", stage_filter) else ""
  
  # Insert-select body
  insert_select <- glue("INSERT INTO {q_dest} ({q_cols}) SELECT {q_cols} FROM {q_src}{where_src}")
  
  # If there's at least one non-key col, we can do UPDATE on change
  set_clause <- if (length(non_keys) > 0) {
    assigns <- paste0(DBI::dbQuoteIdentifier(con, non_keys),
                      " = EXCLUDED.", DBI::dbQuoteIdentifier(con, non_keys))
    paste(assigns, collapse = ", ")
  } else {
    NULL
  }
  
  # Optional change-guard: only update if any non-key differs
  change_guard <- if (length(non_keys) > 0) {
    paste0(" WHERE ",
           tuple_expr(con, non_keys),
           " IS DISTINCT FROM ",
           tuple_expr(con, non_keys, prefix = "EXCLUDED"))
  } else {
    ""
  }
  
  has_unique <- has_matching_unique_index(con, dest_schema, table, key_cols)
  
  if (has_unique && length(non_keys) > 0) {
    sql <- glue("{insert_select}
                 ON CONFLICT ({q_keys}) DO UPDATE
                 SET {set_clause}{change_guard};")
    DBI::dbExecute(con, sql)
  } else if (has_unique && length(non_keys) == 0) {
    # Nothing to update beyond keys — just do nothing on conflict
    sql <- glue("{insert_select} ON CONFLICT ({q_keys}) DO NOTHING;")
    DBI::dbExecute(con, sql)
  } else {
    # Fallback: no unique constraint on keys in prod — replace matching key groups
    # 1) DELETE target rows that match any staged key tuple
    q_keys_vec <- as.character(DBI::dbQuoteIdentifier(con, key_cols))
    key_join <- paste(sprintf("t.%s = s.%s", q_keys_vec, q_keys_vec), collapse = " AND ")
    
    del_sql <- glue(
      "DELETE FROM {q_dest} t
         USING (SELECT DISTINCT {qcsv(con, key_cols)} FROM {q_src}{where_src}) s
         WHERE {key_join};"
    )
    
    # 2) INSERT fresh rows from stage
    ins_sql <- glue("{insert_select};")
    
    DBI::dbWithTransaction(con, {
      DBI::dbExecute(con, del_sql)
      DBI::dbExecute(con, ins_sql)
    })
  }
  
  if (analyze) DBI::dbExecute(con, glue("ANALYZE {q_dest};"))
}

# Convenience runner — pass a named list table -> key vector
upsert_all <- function(con, key_map, src_schema = "stage", dest_schema = "prod",
                       stage_filters = list(), analyze_each = TRUE) {
  for (tbl in names(key_map)) {
    message(glue(">> Upserting {src_schema}.{tbl} -> {dest_schema}.{tbl}"))
    upsert_table(
      con,
      table        = tbl,
      key_cols     = key_map[[tbl]],
      src_schema   = src_schema,
      dest_schema  = dest_schema,
      #stage_filter = stage_filters[[tbl]],
      analyze      = analyze_each
    )
  }
  invisible(TRUE)
}

refresh_mv <- function(con, schema, mv_name, concurrent = TRUE) {
  q_schema <- DBI::dbQuoteIdentifier(con, schema)
  q_mv     <- DBI::dbQuoteIdentifier(con, mv_name)
  if (concurrent) {
    # Note: cannot run inside an explicit transaction
    tryCatch({
      dbExecute(con, glue("REFRESH MATERIALIZED VIEW CONCURRENTLY {q_schema}.{q_mv};"))
    }, error = function(e) {
      message("Concurrent refresh failed for ", schema, ".", mv_name, " — falling back: ", e$message)
      dbExecute(con, glue("REFRESH MATERIALIZED VIEW {q_schema}.{q_mv};"))
    })
  } else {
    dbExecute(con, glue("REFRESH MATERIALIZED VIEW {q_schema}.{q_mv};"))
  }
}
