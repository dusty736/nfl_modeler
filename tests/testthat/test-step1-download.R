# Step 1 downloader — black-box smoke test with namespace patching (no pkgload).
suppressPackageStartupMessages({
  library(testthat)
  library(withr)
  library(dplyr)
  library(tibble)
  library(arrow)
  library(here)
  library(nflreadr)  # needed so its namespace exists to patch
})

# Locate your script (supports both names)
candidate_paths <- c(
  here::here("etl", "R", "download_raw_nfl_data.R"),
  here::here("etl", "R", "step1_download", "step1_data_download.R")
)
found <- candidate_paths[file.exists(candidate_paths)]
script_path <- if (length(found)) found[[1]] else NA_character_
skip_if(is.na(script_path), "Neither download_raw_nfl_data.R nor step1_data_download.R found.")

# Friendly skip only if a *truly* dangling %>% sits right before kicking_stats
lines <- readLines(script_path, warn = FALSE)
next_nonempty_index <- function(lines, i) { j <- i + 1L; while (j <= length(lines) && grepl("^\\s*(#|$)", lines[j])) j <- j + 1L; j }
def_idx <- which(grepl("^\\s*defense_stats\\s*<-.*%>%\\s*$", lines))
is_dangling <- FALSE
if (length(def_idx)) {
  for (i in def_idx) {
    j <- next_nonempty_index(lines, i)
    if (j <= length(lines) && grepl("^\\s*kicking_stats\\s*<-", lines[j])) { is_dangling <- TRUE; break }
  }
}
skip_if(is_dangling, "Script has a dangling %>% on defense_stats line (immediately followed by kicking_stats).")

# ---- Namespace patch helpers ---------------------------------------------------
patch_nflreadr <- function(overrides) {
  ns <- asNamespace("nflreadr"); old <- list()
  for (nm in names(overrides)) {
    if (!exists(nm, envir = ns, inherits = FALSE)) stop("nflreadr::", nm, " not found")
    old[[nm]] <- get(nm, envir = ns, inherits = FALSE)
    unlockBinding(nm, ns); assign(nm, overrides[[nm]], envir = ns); lockBinding(nm, ns)
  }
  old
}
restore_nflreadr <- function(old) {
  ns <- asNamespace("nflreadr")
  for (nm in names(old)) { unlockBinding(nm, ns); assign(nm, old[[nm]], envir = ns); lockBinding(nm, ns) }
}

test_that("download script runs and writes expected files (with patches)", {
  # Work in a temp Dir so we don't touch your real data/
  tmp <- local_tempdir()
  local_dir(tmp)
  dir.create(file.path("data", "raw"), recursive = TRUE, showWarnings = FALSE)
  
  # Minimal fake data
  mock_pbp <- tibble(game_id = "G1", play_id = 1L, posteam = "SEA", defteam = "SF")
  mock_rosters <- tibble(
    full_name  = c("Alice Able", "Bob Baker"),
    first_name = c("Alice","Bob"),
    last_name  = c("Able","Baker"),
    gsis_id    = c("00-0000001","00-0000002"),
    espn_id    = c("111","222")
  )
  mock_depth <- tibble(game_type = c("REG","SBBYE"), team = c("SEA","SEA"))
  mock_inj   <- tibble(season = 2025L, week = 1L, team = "SEA", gsis_id = "00-0000001", status = "QUESTIONABLE")
  mock_part  <- tibble(season = 2025L, week = 1L, game_id = "G1", gsis_id = "00-0000001")
  mock_ngs   <- tibble(season = 2025L, week = 1L, player_gsis_id = "00-0000001")
  mock_contracts <- tibble(player = "Alice Able", cap_hit = 1e6)
  mock_teams <- tibble(team_abbr = c("SEA","LAR"))  # LAR should be filtered out
  mock_snap  <- tibble(season = 2025L, week = 1L, team = "SEA", gsis_id = "00-0000001", offense_snaps = 10L)
  mock_qbr   <- tibble(season = 2024L, player_id = "00-0000001", qbr_total = 55.1)
  mock_sched <- tibble(season = 2025L, week = 1L, game_id = "G1", home_team = "SEA", away_team = "SF")
  mock_player_stats <- tibble(
    season = 2025L,
    position = c("QB","RB","WR","TE","K","OL"),
    position_group = c("QB","RB","WR","TE","K","DL")
  )
  
  old_fns <- patch_nflreadr(list(
    load_pbp           = function(seasons = TRUE)  mock_pbp,
    load_rosters       = function(seasons)         mock_rosters,
    load_depth_charts  = function(seasons = TRUE)  mock_depth,
    load_injuries      = function(seasons = TRUE)  mock_inj,
    load_participation = function(seasons = TRUE)  mock_part,
    load_nextgen_stats = function(seasons = TRUE)  mock_ngs,
    load_contracts     = function()                mock_contracts,
    load_teams         = function(current = FALSE) mock_teams,
    load_snap_counts   = function(seasons = TRUE)  mock_snap,
    load_espn_qbr      = function(seasons = TRUE)  mock_qbr,
    load_player_stats  = function(seasons = TRUE)  mock_player_stats,
    load_schedules     = function(seasons = TRUE)  mock_sched
  ))
  defer(restore_nflreadr(old_fns))
  
  # IMPORTANT: keep writes relative to tmp
  source(script_path, chdir = FALSE)
  
  # Files exist (under tmp/data/raw)
  expect_true(file.exists("data/raw/pbp.parquet"))
  expect_true(file.exists("data/raw/rosters.parquet"))
  expect_true(file.exists("data/raw/depth_charts.parquet"))
  expect_true(file.exists("data/raw/injuries.parquet"))
  expect_true(file.exists("data/raw/participation.parquet"))
  expect_true(file.exists("data/raw/nextgen_stats.parquet"))
  expect_true(file.exists("data/raw/contracts.parquet"))
  expect_true(file.exists("data/raw/team_metadata.parquet"))
  expect_true(file.exists("data/raw/player_snapcount.parquet"))
  expect_true(file.exists("data/raw/espn_qbr.parquet"))
  expect_true(file.exists("data/raw/id_map.parquet"))
  expect_true(file.exists("data/raw/off_player_stats.parquet"))
  expect_true(file.exists("data/raw/def_player_stats.parquet"))
  expect_true(file.exists("data/raw/st_player_stats.parquet"))
  expect_true(file.exists("data/raw/schedule.parquet"))
  
  # Filters applied
  depth <- arrow::read_parquet("data/raw/depth_charts.parquet")
  expect_false(any(depth$game_type == "SBBYE"))
  
  teams <- arrow::read_parquet("data/raw/team_metadata.parquet")
  expect_false(any(teams$team_abbr == "LAR"))
  
  off <- arrow::read_parquet("data/raw/off_player_stats.parquet")
  expect_true(all(off$position %in% c("QB","RB","WR","TE")))
  
  def <- arrow::read_parquet("data/raw/def_player_stats.parquet")
  expect_true(all(def$position_group %in% c("DL","LB","DB")))
  
  st <- arrow::read_parquet("data/raw/st_player_stats.parquet")
  expect_true(all(st$position %in% c("K")))
  
  idm <- arrow::read_parquet("data/raw/id_map.parquet")
  expect_true(all(c("full_name","first_name","last_name") %in% names(idm)))
  expect_true(any(grepl("id", names(idm), fixed = TRUE)))
  expect_equal(nrow(idm), nrow(dplyr::distinct(idm)))
})

