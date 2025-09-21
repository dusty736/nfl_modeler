#' Summarize Structure of Parquet Files in a Directory
#'
#' Reads all `.parquet` files in a given directory and summarizes each file's structure,
#' including column names, data types, and example values.
#'
#' @param dir Character. Path to the directory containing `.parquet` files. Default is `"data/processed"`.
#' @param n_examples Integer. Number of example values to show per column. Default is `3`.
#'
#' @return A tibble with columns: `file`, `column`, `type`, and `example`.
#'
#' @importFrom arrow read_parquet
#' @importFrom dplyr %>% tibble
#' @importFrom purrr map_dfr map_chr
#' @export
#'
#' @examples
#' \dontrun{
#' # Generate a summary of all Parquet files in the processed data folder
#' parquet_summary <- summarize_parquet_structure()
#'
#' # View the first few rows
#' head(parquet_summary)
#' }
summarize_parquet_structure <- function(dir = "data/processed", n_examples = 3) {
  files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE)
  
  purrr::map_dfr(files, function(file) {
    df <- arrow::read_parquet(file)
    tibble::tibble(
      file = basename(file),
      column = names(df),
      type = purrr::map_chr(df, ~ class(.x)[1]),
      example = purrr::map_chr(df, ~ {
        val <- .x[!is.na(.x)][1:min(n_examples, length(.x[!is.na(.x)]))]
        paste0(head(val, n_examples), collapse = ", ")
      })
    )
  })
}

#file_summary <- summarize_parquet_structure()
#data.table::fwrite(file_summary, "data/file_summary.csv")

# ---- Utilities added by request (non-breaking, black-box helpers) ----------------

#' Create a directory if it doesn't exist (idempotent)
#' @param path Character path to a directory.
#' @return The normalized path (invisibly).
#' @export
ensure_dir <- function(path) {
  if (is.na(path) || !nzchar(path)) stop("ensure_dir(): 'path' must be a non-empty string.")
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(normalizePath(path, mustWork = FALSE))
}

#' Safe Parquet writer (ensures parent dir exists)
#' @param x A data.frame / tibble / Arrow Table
#' @param path Output .parquet path
#' @param ... Additional args passed to arrow::write_parquet()
#' @return The output path (invisibly)
#' @export
write_parquet_safely <- function(x, path, ...) {
  if (missing(path)) stop("write_parquet_safely(): 'path' is required.")
  ensure_dir(dirname(path))
  arrow::write_parquet(x, path, ...)
  invisible(path)
}

#' Safe Parquet reader (returns NULL if file missing)
#' @param path .parquet file path
#' @return A data.frame/tibble (Arrow data.frame) or NULL if not found
#' @export
safe_read_parquet <- function(path) {
  if (!file.exists(path)) return(NULL)
  arrow::read_parquet(path)
}

#' Coerce to integer with NA on failure (silent)
#' @param x Vector to coerce
#' @return Integer vector
#' @export
coerce_int <- function(x) {
  suppressWarnings(as.integer(as.character(x)))
}

#' Coerce to Date with NA on failure (silent)
#' @param x Vector to coerce (character/factor/POSIXct/numeric/other)
#' @param format Optional format string for character input (e.g., "%Y-%m-%d")
#' @return Date vector
#' @export
coerce_date <- function(x, format = NULL) {
  if (inherits(x, "Date")) return(x)
  
  # Character/factor: only pass format if not NULL
  if (is.character(x) || is.factor(x)) {
    x_chr <- as.character(x)
    if (is.null(format)) {
      suppressWarnings(as.Date(x_chr))
    } else {
      suppressWarnings(as.Date(x_chr, format = format))
    }
  } else if (inherits(x, "POSIXct") || inherits(x, "POSIXt")) {
    as.Date(x)
  } else if (is.numeric(x)) {
    suppressWarnings(as.Date(x, origin = "1970-01-01"))
  } else {
    # last-resort attempt via character
    suppressWarnings(as.Date(as.character(x)))
  }
}

# ---- Path helpers (kept simple and repo-relative) --------------------------------

#' Build a path under data/raw
#' @param ... Path components under data/raw
#' @export
path_raw <- function(...) file.path("data", "raw", ...)

#' Build a path under data/processed
#' @param ... Path components under data/processed
#' @export
path_processed <- function(...) file.path("data", "processed", ...)

#' Build a path under data/staging
#' @param ... Path components under data/staging
#' @export
path_staging <- function(...) file.path("data", "staging", ...)

#' Build a path under data/for_database
#' @param ... Path components under data/for_database
#' @export
path_for_database <- function(...) file.path("data", "for_database", ...)

# ---- DB table guards (graceful when no connection is available) ------------------

#' Check if a table exists (optionally schema-qualified). Returns FALSE on any error.
#' Uses getOption('nfl_db_con') if no 'con' is supplied.
#' @param table Table name (character)
#' @param con DBI connection (optional)
#' @param schema Schema name (optional, e.g., "prod" or "stage")
#' @return TRUE/FALSE
#' @export
tbl_exists <- function(table, con = getOption("nfl_db_con", NULL), schema = NULL) {
  if (is.null(con)) return(FALSE)
  out <- FALSE
  try({
    if (!is.null(schema)) {
      out <- DBI::dbExistsTable(con, DBI::Id(schema = schema, table = table))
    } else {
      out <- DBI::dbExistsTable(con, table)
    }
  }, silent = TRUE)
  isTRUE(out)
}

#' Safely obtain a dplyr table handle; returns NULL if missing/invalid
#' Uses getOption('nfl_db_con') if no 'con' is supplied.
#' @param table Table name (character)
#' @param con DBI connection (optional)
#' @param schema Schema name (optional)
#' @return dplyr::tbl or NULL
#' @export
safe_tbl <- function(table, con = getOption("nfl_db_con", NULL), schema = NULL) {
  if (is.null(con)) return(NULL)
  if (!tbl_exists(table, con = con, schema = schema)) return(NULL)
  out <- NULL
  try({
    if (!is.null(schema)) {
      out <- dplyr::tbl(con, DBI::Id(schema = schema, table = table))
    } else {
      out <- dplyr::tbl(con, table)
    }
  }, silent = TRUE)
  out
}
