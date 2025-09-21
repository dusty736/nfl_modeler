# tests/testthat/test-step2-injuries-process.R
# Context: step2_injuries_process — BASIC, ROBUST TESTS ONLY.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
})

# Resolve project root relative to this file (avoids here()-shenanigans in tmp dirs)
ROOT <- normalizePath(file.path(testthat::test_path(), "..", ".."))
FUN_PATH <- file.path(ROOT, "etl", "R", "step2_process", "step2_injuries_process_functions.R")
if (!file.exists(FUN_PATH)) {
  stop(paste0("Functions file not found at: ", FUN_PATH,
              " — are you running from the project root with nfl_modeler.Rproj?"))
}
source(FUN_PATH)

# ------------------------------------------------------------------------------
# Minimal raw fixture covering all branches
# ------------------------------------------------------------------------------
# Cases:
#  - A1: primary from REPORT; sec=FALSE; practice "Did Not Participate"
#  - A2: primary from PRACTICE (report NA); sec=FALSE
#  - A3: primary NA; secondary TRUE -> injury_reported TRUE via secondary
#  - A4: primary NA; secondary FALSE -> injury_reported FALSE
#  - A5/A6: two rows for same key; last status must be chosen
#  - A7: missing gsis_id -> UNK_* key is generated
inj_raw <- tibble::tibble(
  season = c(2024, 2024, 2024, 2024, 2024, 2024, 2024),
  week   = c(1,    1,    1,    2,    2,    2,    1),
  team   = c("SEA","SEA","SEA","SEA","SEA","SEA","SF"),
  gsis_id= c("00-000001", "00-000002", "00-000003", "00-000004","00-000004", NA, "00-000007"),
  full_name = c("Alpha A","Bravo B","Charlie C","Delta D","Delta D", "Echo E","Foxtrot F"),
  position  = c("QB","RB","WR","TE","TE","QB","RB"),
  
  # Report/practice injuries to exercise coalesces
  report_primary_injury   = c("Ankle", NA, NA, "Hamstring", NA, NA, NA),
  practice_primary_injury = c(NA, "Knee", NA, NA, NA, NA, NA),
  
  # Secondary flags (TRUE for A3 only)
  report_secondary_injury   = c(NA, NA, TRUE, NA, NA, NA, NA),
  practice_secondary_injury = c(NA, NA, NA,   NA, NA, NA, NA),
  
  # Statuses; A5/A6 share key — last status should win
  report_status   = c("Questionable","Questionable","Out","Doubtful","Out","Questionable","Probable"),
  practice_status = c("Did Not Participate", "Limited", "Limited", "Full", "Limited", "Limited", "Full")
)

# ------------------------------------------------------------------------------
test_that("process_injuries: shapes, keys, and flags behave", {
  wk <- process_injuries(inj_raw)
  
  # Columns exist
  expected_cols <- c(
    "season","week","team","gsis_id","full_name","position",
    "report_status","injury_reported","did_not_practice","injury_status",
    "practice_status","primary_injury","secondary_injury"
  )
  expect_true(all(expected_cols %in% names(wk)))
  
  # One row per (season, week, team, gsis_id)
  expect_equal(sum(duplicated(wk[c("season","week","team","gsis_id")])), 0L)
  
  # did_not_practice derived from practice_status text
  a1 <- wk %>% filter(gsis_id == "00-000001") %>% slice(1)
  expect_true(a1$did_not_practice)
  
  # Coalesce: primary from PRACTICE when report is NA
  a2 <- wk %>% filter(gsis_id == "00-000002") %>% slice(1)
  expect_identical(a2$primary_injury, "Knee")
  expect_true(a2$injury_reported)
  
  # Secondary-only still counts as reported
  a3 <- wk %>% filter(gsis_id == "00-000003") %>% slice(1)
  expect_true(is.na(a3$primary_injury))
  expect_true(a3$secondary_injury)
  expect_true(a3$injury_reported)
  
  # No primary + no secondary -> not reported
  a7 <- wk %>% filter(gsis_id == "00-000007") %>% slice(1)
  expect_false(a7$injury_reported)
  
  # Last status wins within a key (A5/A6)
  dd <- wk %>% filter(gsis_id == "00-000004", season == 2024, week == 2, team == "SEA")
  expect_identical(dd$report_status, "Out")
  expect_identical(dd$practice_status, "Limited")
  
  # Missing gsis_id gets a stable UNK_* surrogate
  unk <- wk %>% dplyr::filter(full_name == "Echo E")
  expect_true(all(grepl("^UNK_[A-Z0-9_]+_QB$", unk$gsis_id)))
})

# ------------------------------------------------------------------------------
test_that("position_injury_summary: counts and cumulative are sane", {
  wk <- process_injuries(inj_raw)
  pos <- position_injury_summary(wk)
  
  # Only reported injuries counted; positions non-empty
  expect_true(all(pos$position_injuries >= 0))
  expect_true(all(!is.na(pos$position)))
  expect_true(all(pos$position != ""))
  
  # Cumulative is non-decreasing within (season, team, position)
  chk <- pos %>%
    group_by(season, team, position) %>%
    summarise(ok = all(diff(c(0, cumulative_position_injuries)) >= 0), .groups = "drop")
  expect_true(all(chk$ok))
})

# ------------------------------------------------------------------------------
test_that("team_injury_summary: weekly sums match position totals; cumulative monotone", {
  wk  <- process_injuries(inj_raw)
  pos <- position_injury_summary(wk)
  tm  <- team_injury_summary(pos)
  
  # Weekly sums equal sum over positions
  comp <- pos %>%
    group_by(season, team, week) %>%
    summarise(pos_sum = sum(position_injuries), .groups = "drop") %>%
    inner_join(tm, by = c("season","team","week"))
  expect_equal(comp$weekly_injuries, comp$pos_sum)
  
  # Cumulative non-decreasing within (season, team)
  chk <- tm %>%
    group_by(season, team) %>%
    summarise(ok = all(diff(c(0, cumulative_injuries)) >= 0), .groups = "drop")
  expect_true(all(chk$ok))
})

# ------------------------------------------------------------------------------
test_that("season_injury_summary: season totals equal sum of weekly_injuries", {
  wk  <- process_injuries(inj_raw)
  pos <- position_injury_summary(wk)
  tm  <- team_injury_summary(pos)
  ssn <- season_injury_summary(tm)
  
  comp <- tm %>%
    group_by(season, team) %>%
    summarise(weekly_sum = sum(weekly_injuries), .groups = "drop") %>%
    inner_join(ssn, by = c("season","team"))
  
  expect_equal(comp$season_injuries, comp$weekly_sum)
})
