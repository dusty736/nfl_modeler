# Helper: source utils and set up a clean temp space for side-effect-free tests.

# Load deps used in tests (does NOT change your code)
suppressPackageStartupMessages({
  library(testthat)
  library(withr)
  library(dplyr)
  library(arrow)
  library(here)
})

# Reproducibility
set.seed(42)

# Temp directory for any writes these tests perform
TEST_TMP_DIR <- withr::local_tempdir()
# Helper for building temp paths
tmp_path <- function(...) normalizePath(file.path(TEST_TMP_DIR, ...), mustWork = FALSE)

# Source your utils in the global env so existing codepaths remain untouched
UTILS_FILE <- here::here("etl", "R", "utils.R")

if (file.exists(UTILS_FILE)) {
  source(UTILS_FILE, chdir = TRUE)
} else {
  skip(paste0("etl/R/utils.R not found at: ", UTILS_FILE, " — are you running from the project root with nfl_modeler.Rproj?"))
}

# Convenience: check if a function exists before testing it
fn_exists <- function(name) isTRUE(exists(name, mode = "function"))
