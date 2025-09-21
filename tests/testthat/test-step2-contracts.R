suppressPackageStartupMessages({
  library(testthat)
  library(withr)
  library(dplyr)
  library(tibble)
  library(arrow)
  library(here)
})

script_path <- here::here("etl","R","step2_process","step2_contracts_process.R")
skip_if_not(file.exists(script_path), "step2_contracts_process.R not found.")

# --- patch helpers ---
patch_pkg_symbol <- function(pkg, name, fn) {
  ns <- asNamespace(pkg)
  old_ns <- get(name, envir = ns, inherits = FALSE)
  unlockBinding(name, ns); assign(name, fn, envir = ns); lockBinding(name, ns)
  list(ns = old_ns)
}
restore_pkg_symbol <- function(pkg, name, olds) {
  ns <- asNamespace(pkg)
  unlockBinding(name, ns); assign(name, olds$ns, envir = ns); lockBinding(name, ns)
}

test_that("step2_contracts_process writes cap_pct and qb outputs, idempotent", {
  tmp <- local_tempdir()
  local_dir(tmp)
  
  # Ensure dirs exist
  dir.create(file.path("data","raw"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path("data","processed"), recursive = TRUE, showWarnings = FALSE)
  
  # Fixture contracts.parquet
  raw_contracts <- tibble::tibble(
    player = c("Alpha A","Alpha A","Bravo B","Charlie C"),
    position = c("QB","QB","WR","QB"),
    team = c("SEA","SF","SEA","SF"),
    is_active = c(TRUE, TRUE, FALSE, TRUE),
    year_signed = c(2020L,2023L,2021L,2025L),
    years = c(4L,2L,3L,1L),
    value = c(100,50,30,10),
    apy = c(25,25,10,10),
    guaranteed = c(80,40,20,8),
    apy_cap_pct = c(0.12,0.11,NA,0.05),
    inflated_value = c(110,52,32,11),
    inflated_apy = c(27.5,26,10.6,10.2),
    inflated_guaranteed = c(88,42,21,8.4),
    player_page = c("url1","url1","url2","url3"),
    otc_id = c("otc1","otc1","otc2","otc3"),
    gsis_id = c("00-1","00-1","00-2","00-3"),
    date_of_birth = c("1995-01-01","1995-01-01","1996-02-02","1994-03-03"),
    height = c("6-2","6-2","6-0","6-1"),
    weight = c(220,220,195,205),
    college = c("X","X","Y","Z"),
    draft_year = c(2018L,2018L,2019L,2017L),
    draft_round = c(1L,1L,2L,3L),
    draft_overall = c(5L,5L,45L,70L),
    draft_team = c("SEA","SEA","SEA","SF")
  )
  arrow::write_parquet(raw_contracts, file.path("data","raw","contracts.parquet"))
  
  # Patch here() so it resolves into the temp workspace
  old_here <- patch_pkg_symbol("here", "here", function(...) file.path(getwd(), ...))
  defer(restore_pkg_symbol("here", "here", old_here))
  
  # Run script
  source(script_path, chdir = FALSE)
  
  pos_path <- file.path("data","processed","contracts_position_cap_pct.parquet")
  qb_path  <- file.path("data","processed","contracts_qb.parquet")
  
  expect_true(file.exists(pos_path))
  expect_true(file.exists(qb_path))
  
  pos <- arrow::read_parquet(pos_path)
  qb  <- arrow::read_parquet(qb_path)
  
  # Sanity: cap_pct has expected grouping cols
  expect_true(all(c("position","year_signed","team","avg_apy_cap_pct","total_apy","count") %in% names(pos)))
  # Sanity: qb only has QBs
  expect_true(all(qb$position == "QB"))
  
  # Idempotency
  source(script_path, chdir = FALSE)
  pos2 <- arrow::read_parquet(pos_path)
  qb2  <- arrow::read_parquet(qb_path)
  expect_equal(as.data.frame(pos), as.data.frame(pos2))
  expect_equal(as.data.frame(qb), as.data.frame(qb2))
})
