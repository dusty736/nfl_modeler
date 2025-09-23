# Basic, robust tests for step3_parquet_to_postgres_functions.R
# No real DB: we stub DBI generics for a fake connection class.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(arrow)
  library(here)
  library(withr)
  library(DBI)
  library(methods)
})

# Load functions under test (defines load_parquet_to_postgres, drop_schema, create_schema, create_index)
source(here::here("etl", "R", "step3_sql", "step3_parquet_to_postgres_functions.R"))

# --- Test helpers: fake DBI connection + stub methods -------------------------

# A tiny helper like `%||%`
`%||%` <- function(a, b) if (!is.null(a)) a else b

# Define a fake DBI connection class that DBI will dispatch on
setClass("TestConnection", contains = "DBIConnection")

# Capture sinks
.db_calls <- new.env(parent = emptyenv())
.db_calls$write_calls <- list()
.db_calls$exec_calls  <- character()

fn_env <- environment(load_parquet_to_postgres)
assign("con", new("TestConnection"), envir = fn_env)

# Quote identifier for our fake connection
setMethod(
  "dbQuoteIdentifier",
  signature(conn = "TestConnection", x = "ANY"),
  function(conn, x, ...) {
    # If it's an Id(schema=..., table=...), try to format "schema"."table"
    if (inherits(x, "Id")) {
      # DBI::Id stores components in slot @name (a named list)
      nm <- tryCatch(methods::slot(x, "name"), error = function(e) NULL)
      if (is.list(nm)) {
        schema <- nm[["schema"]] %||% nm[[1]] %||% "?"
        table  <- nm[["table"]]  %||% nm[[length(nm)]] %||% "?"
        return(DBI::SQL(sprintf('"%s"."%s"', as.character(schema), as.character(table))))
      }
    }
    DBI::SQL(sprintf('"%s"', as.character(x)))
  }
)

# dbWriteTable stub: just capture args
setMethod(
  "dbWriteTable",
  signature(conn = "TestConnection", name = "ANY", value = "data.frame"),
  function(conn, name, value, ...) {
    .db_calls$write_calls <- append(.db_calls$write_calls, list(
      list(name = name, nrow = nrow(value), dots = list(...))
    ))
    TRUE
  }
)

# dbExecute stub: capture SQL text
setMethod(
  "dbExecute",
  signature(conn = "TestConnection", statement = "character"),
  function(conn, statement, ...) {
    .db_calls$exec_calls <- c(.db_calls$exec_calls, statement)
    1L
  }
)

# Build a fake connection and leak it as the global 'con' expected by functions
con <- new("TestConnection")

# ------------------------------------------------------------------------------
# load_parquet_to_postgres ------------------------------------------------------
# ------------------------------------------------------------------------------
test_that("load_parquet_to_postgres reads parquet and calls dbWriteTable overwrite=TRUE", {
  td <- withr::local_tempdir()
  fp <- file.path(td, "mini.parquet")
  
  df_in <- tibble::tibble(a = c(1L, 2L), b = c("x", "y"))
  arrow::write_parquet(df_in, fp)
  
  # Clear prior captures
  .db_calls$write_calls <- list()
  
  load_parquet_to_postgres(fp, schema = "prod", table_name = "some_tbl")
  
  expect_equal(length(.db_calls$write_calls), 1L)
  call <- .db_calls$write_calls[[1]]
  
  # name is a DBI::Id(schema=..., table=...) — we won't decompose it, just ensure it's there
  expect_true(!is.null(call$name))
  
  # right number of rows written
  expect_equal(call$nrow, nrow(df_in))
  
  # overwrite=TRUE and row.names=FALSE used
  expect_true(isTRUE(call$dots$overwrite))
  expect_true(isFALSE(call$dots$row.names))
})

# ------------------------------------------------------------------------------
# drop_schema -------------------------------------------------------------------
# ------------------------------------------------------------------------------
test_that("drop_schema builds DROP SCHEMA ... CASCADE SQL", {
  .db_calls$exec_calls <- character()
  
  res <- drop_schema(con, schema = "prod")
  expect_true(isTRUE(res))
  
  # One statement, contains the right tokens (don’t be precious about exact quoting)
  expect_equal(length(.db_calls$exec_calls), 1L)
  sql <- .db_calls$exec_calls[[1]]
  expect_true(grepl("^DROP SCHEMA IF EXISTS", sql))
  expect_true(grepl("prod", sql))
  expect_true(grepl("CASCADE;?$", sql))
})

# ------------------------------------------------------------------------------
# create_schema -----------------------------------------------------------------
# ------------------------------------------------------------------------------
test_that("create_schema builds CREATE SCHEMA IF NOT EXISTS SQL", {
  .db_calls$exec_calls <- character()
  
  res <- create_schema(con, schema = "prod")
  expect_true(isTRUE(res))
  
  expect_equal(length(.db_calls$exec_calls), 1L)
  sql <- .db_calls$exec_calls[[1]]
  expect_true(grepl("^CREATE SCHEMA IF NOT EXISTS", sql))
  expect_true(grepl("prod", sql))
})

# ------------------------------------------------------------------------------
# create_index ------------------------------------------------------------------
# ------------------------------------------------------------------------------
test_that("create_index builds expected CREATE INDEX statements", {
  .db_calls$exec_calls <- character()
  
  # Non-unique
  create_index(con, schema = "prod", table = "some_tbl",
               id_cols = c("col1","col2"), unique = FALSE)
  # Unique
  create_index(con, schema = "prod", table = "other_tbl",
               id_cols = c("pkey"), unique = TRUE)
  
  expect_equal(length(.db_calls$exec_calls), 2L)
  
  sql1 <- .db_calls$exec_calls[[1]]
  sql2 <- .db_calls$exec_calls[[2]]
  
  # Non-unique contains expected pieces
  expect_true(grepl("^CREATE\\s+INDEX\\s+IF NOT EXISTS\\s+", sql1))
  expect_true(grepl("idx_some_tbl_col1_col2", sql1))
  expect_true(grepl("prod", sql1))
  expect_true(grepl("some_tbl", sql1))
  expect_true(grepl("\\(.*col1.*col2.*\\)", sql1))
  
  # Unique contains expected pieces
  expect_true(grepl("^CREATE\\s+UNIQUE\\s+INDEX\\s+IF NOT EXISTS\\s+", sql2))
  expect_true(grepl("idx_other_tbl_pkey", sql2))
  expect_true(grepl("prod", sql2))
  expect_true(grepl("other_tbl", sql2))
  expect_true(grepl("\\(.*pkey.*\\)", sql2))
})

