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

