# Step 2: id_map processing — black-box test.
# - Creates a temp data/raw/id_map.parquet with duplicates
# - Patches here::here to resolve under temp working dir
# - Sources your script unchanged
# - Asserts data/processed/id_map.parquet exists and equals distinct(raw)
# - Checks idempotency (second run is identical)

suppressPackageStartupMessages({
  library(testthat)
  library(withr)
  library(dplyr)
  library(tibble)
  library(arrow)
  library(here)
})

script_path <- here::here("etl", "R", "step2_process", "step2_idmap_process.R")
skip_if_not(file.exists(script_path), message = "etl/R/step2_process/step2_idmap_process.R not found.")

# Generic namespace patcher (useful when testing non-package projects)
# Patch/restore exported symbols in both the namespace and attached env
patch_pkg_symbol <- function(pkg, name, fn) {
  ns <- asNamespace(pkg)
  old_ns <- get(name, envir = ns, inherits = FALSE)
  unlockBinding(name, ns); assign(name, fn, envir = ns); lockBinding(name, ns)
  
  # If attached (package:pkg), patch there too
  attached_env_name <- paste0("package:", pkg)
  old_att <- NULL
  if (attached_env_name %in% search()) {
    att <- as.environment(attached_env_name)
    old_att <- get(name, envir = att, inherits = FALSE)
    unlockBinding(name, att); assign(name, fn, envir = att); lockBinding(name, att)
  }
  list(ns = old_ns, att = old_att)
}

restore_pkg_symbol <- function(pkg, name, olds) {
  ns <- asNamespace(pkg)
  unlockBinding(name, ns); assign(name, olds$ns, envir = ns); lockBinding(name, ns)
  
  attached_env_name <- paste0("package:", pkg)
  if (!is.null(olds$att) && attached_env_name %in% search()) {
    att <- as.environment(attached_env_name)
    unlockBinding(name, att); assign(name, olds$att, envir = att); lockBinding(name, att)
  }
}

test_that("step2_idmap_process writes distinct processed id_map and is idempotent", {
  tmp <- local_tempdir()
  local_dir(tmp)
  
  # Create temp raw + processed dirs
  dir.create(file.path("data", "raw"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path("data", "processed"), recursive = TRUE, showWarnings = FALSE)
  
  # Minimal raw with duplicates
  raw_id_map <- tibble::tibble(
    full_name  = c("Alice Able","Alice Able","Bob Baker"),
    first_name = c("Alice","Alice","Bob"),
    last_name  = c("Able","Able","Baker"),
    gsis_id    = c("00-0000001","00-0000001","00-0000002"),
    espn_id    = c("111","111","222")
  )
  arrow::write_parquet(raw_id_map, file.path("data","raw","id_map.parquet"))
  
  # Patch here() so here("data","...") → file.path(getwd(),"data","...")
  old_here <- patch_pkg_symbol("here", "here", function(...) file.path(getwd(), ...))
  defer(restore_pkg_symbol("here", "here", old_here))
  
  # Run unchanged
  source(script_path, chdir = FALSE)
  
  proc_path <- file.path("data","processed","id_map.parquet")
  expect_true(file.exists(proc_path))
  
  processed <- arrow::read_parquet(proc_path)
  
  # Must include our key name fields + at least our two id fields
  expect_true(all(c("full_name","first_name","last_name","gsis_id","espn_id") %in% names(processed)))
  
  # Compare only on columns common to both (script may emit extra id columns)
  common <- intersect(names(processed), names(raw_id_map))
  exp_sorted <- dplyr::arrange(dplyr::distinct(raw_id_map[common]), dplyr::across(dplyr::everything()))
  got_sorted <- dplyr::arrange(dplyr::distinct(processed[common]), dplyr::across(dplyr::everything()))
  
  # Coerce to plain data.frame to avoid tibble vs data.table chatter
  expect_equal(as.data.frame(got_sorted), as.data.frame(exp_sorted))
  
  # Idempotency: run again, same result on common columns
  source(script_path, chdir = FALSE)
  processed2 <- arrow::read_parquet(proc_path)
  got2_sorted <- dplyr::arrange(dplyr::distinct(processed2[common]), dplyr::across(dplyr::everything()))
  expect_equal(as.data.frame(got2_sorted), as.data.frame(exp_sorted))
})
