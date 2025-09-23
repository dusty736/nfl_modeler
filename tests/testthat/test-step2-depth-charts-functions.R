# tests/testthat/test-step2-depth-charts-functions.R

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(here)
  library(stringr)  # clean_position() uses stringr
  library(purrr)    # cumdistinct() uses purrr::accumulate
})

testthat::local_edition(3)

# ------------------------------------------------------------------------------
# Source functions under test (robust pathing)
# ------------------------------------------------------------------------------
fpath <- here::here("etl", "R", "step2_process", "step2_depth_charts_process_functions.R")
if (!file.exists(fpath)) {
  stop("Cannot find functions file: ", fpath)
}
source(fpath)

# ------------------------------------------------------------------------------
# Tiny fixtures
# ------------------------------------------------------------------------------

# Raw-ish depth chart rows (as read from parquet) with a mix of starter(1)/backup(2)
raw_depth <- tibble::tribble(
  ~depth_team, ~season, ~week, ~club_code, ~full_name,   ~depth_position, ~gsis_id,
  1L,          2024L,    1L,    "SEA",      "Alpha A",    "QB",            "00-AAA",
  2L,          2024L,    1L,    "SEA",      "Gamma G",    "QB",            "00-GGG",
  1L,          2024L,    2L,    "SEA",      "Alpha A",    "QB",            "00-AAA",
  1L,          2024L,    3L,    "SEA",      "Bravo B",    "QB",            "00-BBB",
  1L,          2024L,    1L,    "SEA",      "Wrynn W",    "WR1",           "00-WR1",
  1L,          2024L,    1L,    "SEA",      "Rin R",      "RWR",           "00-WR2",
  1L,          2024L,    2L,    "SEA",      "Wrynn W",    "WR1",           "00-WR1",
  1L,          2024L,    2L,    "SEA",      "Rin R",      "RWR",           "00-WR2",
  1L,          2024L,    3L,    "SEA",      "Wrynn W",    "WR1",           "00-WR1",
  1L,          2024L,    3L,    "SEA",      "Cora C",     "WR\\8",         "00-WR3",
  1L,          2024L,    1L,    "SEA",      "Snap S",     "K/KO",          "00-KKR"
)

# Convenience: starters like the script creates (filter, then clean positions)
make_starters <- function(df = raw_depth) {
  filter_depth_chart_starters(df) %>%
    mutate(position = clean_position(position))
}

# Helper used by lineup-stability test to match the pipeline’s grouping
add_position_group <- function(df) {
  df %>%
    mutate(
      position_group = dplyr::case_when(
        position %in% c("C", "LG", "LT", "OL", "RG", "RT") ~ "OL",
        position %in% c("QB") ~ "QB",
        position %in% c("WR", "TE") ~ "REC",
        position %in% c("RB", "FB") ~ "RB",
        position %in% c("CB", "DL", "DT", "EDGE", "FS", "MLB", "OLB", "SS") ~ "DEF",
        position %in% c("K") ~ "K",
        position %in% c("ST", "P") ~ "ST",
        TRUE ~ "OTHER"
      )
    )
}

# ------------------------------------------------------------------------------
# filter_depth_chart_starters()
# ------------------------------------------------------------------------------

test_that("filter_depth_chart_starters: filters to starters and reshapes columns", {
  starters <- filter_depth_chart_starters(raw_depth)
  
  # Only depth_team == 1 remain
  expect_true(all(starters$season == 2024L))
  expect_true(all(starters$week %in% c(1L, 2L, 3L)))
  expect_true(all(starters$team == "SEA"))
  expect_true(all(starters$player %in% raw_depth$full_name))
  expect_true(!"club_code" %in% names(starters))
  expect_true(!"depth_position" %in% names(starters))
  expect_identical(names(starters), c("season", "week", "team", "player", "position", "gsis_id"))
  
  # Should not include the backup QB (depth_team == 2)
  expect_false("Gamma G" %in% starters$player)
  
  # Zero-row pass-through (schema preserved)
  empty <- raw_depth[0, ]
  out <- filter_depth_chart_starters(empty)
  expect_identical(names(out), names(starters))
  expect_identical(nrow(out), 0L)
})

# ------------------------------------------------------------------------------
# clean_position()
# ------------------------------------------------------------------------------

test_that("clean_position: follows current fine-grained mapping and string cleaning", {
  src <- c(" lt ", "WR1", "TE\\N", "hb-te", "qb", "EDGE", "MLB", "NKL", "fs",
           "K/KO", "KR", "", NA)
  got <- clean_position(src)
  
  # Expectations match the CURRENT implementation:
  expect_equal(got[1],  "LT")     # " lt " -> LT (fine-grained)
  expect_equal(got[2],  "WR")     # WR1 -> WR
  expect_equal(got[3],  "OTHER")  # "TE\N" -> "TE/N" → not matched → OTHER
  expect_equal(got[4],  "RB")     # HB-TE -> RB (function collapses to RB)
  expect_equal(got[5],  "QB")     # qb -> QB
  expect_equal(got[6],  "EDGE")   # default de_to = "EDGE" → EDGE
  expect_equal(got[7],  "MLB")    # MLB stays MLB
  expect_equal(got[8],  "CB")     # NKL/NICK → CB bucket
  expect_equal(got[9],  "FS")     # fs -> FS (not S)
  expect_equal(got[10], "OTHER")  # "K/KO" not matched → OTHER
  expect_equal(got[11], "ST")     # KR -> ST
  expect_equal(got[12], "OTHER")  # "" -> OTHER
  expect_equal(got[13], "OTHER")  # NA -> OTHER (current function behavior)
})

# ------------------------------------------------------------------------------
# get_qb_start_stats()
# ------------------------------------------------------------------------------

test_that("get_qb_start_stats: counts distinct QB starters cumulatively by team/season", {
  starters <- make_starters(raw_depth)
  qb_stats <- get_qb_start_stats(starters)
  
  expect_identical(names(qb_stats), c("season", "week", "team", "distinct_qb_starters"))
  expect_true(all(qb_stats$team == "SEA"))
  expect_true(all(qb_stats$season == 2024L))
  expect_identical(qb_stats$week, sort(qb_stats$week))
  
  # Weeks 1-3: A, A, B -> cumulative distinct should be 1,1,2
  expected <- tibble::tibble(week = c(1L, 2L, 3L), distinct_qb_starters = c(1L, 1L, 2L))
  merged <- dplyr::left_join(qb_stats, expected, by = "week", suffix = c("", "_exp"))
  expect_equal(merged$distinct_qb_starters, merged$distinct_qb_starters_exp)
})

# ------------------------------------------------------------------------------
# get_player_start_totals()
# ------------------------------------------------------------------------------

test_that("get_player_start_totals: counts starts by season/team/position/gsis_id", {
  starters <- make_starters(raw_depth)
  
  totals <- get_player_start_totals(starters)
  expect_identical(
    names(totals),
    c("season", "team", "position", "gsis_id", "total_starts")
  )
  # Known counts from fixture:
  # Alpha A (QB): weeks 1,2 -> 2
  # Bravo B (QB): week 3    -> 1
  # Wrynn W (WR): weeks 1,2,3 -> 3
  # Rin R (WR): weeks 1,2   -> 2
  # Cora C (WR): week 3     -> 1
  # Snap S (K/KO): maps to OTHER in clean_position() → not asserted
  
  expect_equal(
    totals$total_starts[totals$gsis_id == "00-AAA" & totals$position == "QB"],
    2
  )
  expect_equal(
    totals$total_starts[totals$gsis_id == "00-BBB" & totals$position == "QB"],
    1
  )
  expect_equal(
    totals$total_starts[totals$gsis_id == "00-WR1" & totals$position == "WR"],
    3
  )
  expect_equal(
    totals$total_starts[totals$gsis_id == "00-WR2" & totals$position == "WR"],
    2
  )
  expect_equal(
    totals$total_starts[totals$gsis_id == "00-WR3" & totals$position == "WR"],
    1
  )
})

# ------------------------------------------------------------------------------
# get_starter_switches()
# ------------------------------------------------------------------------------

test_that("get_starter_switches: flags first starts after week 1 as new starters", {
  starters <- make_starters(raw_depth)
  
  switches <- get_starter_switches(starters)
  expect_true(all(c("is_new_starter", "player_starts") %in% names(switches)))
  
  # Alpha A first start at week 1 -> not new
  aa <- switches %>% filter(gsis_id == "00-AAA") %>% arrange(week)
  expect_false(any(aa$is_new_starter))
  
  # Bravo B first start at week 3 -> new starter TRUE at week 3
  bb <- switches %>% filter(gsis_id == "00-BBB") %>% arrange(week)
  expect_equal(bb$is_new_starter, c(TRUE))
  
  # WR Cora first start at week 3 -> also new starter TRUE
  cc <- switches %>% filter(gsis_id == "00-WR3") %>% arrange(week)
  expect_equal(cc$is_new_starter, c(TRUE))
})

# ------------------------------------------------------------------------------
# get_inseason_promotions()
# ------------------------------------------------------------------------------

test_that("get_inseason_promotions: detects a change of starter within team/position sequence", {
  starters <- make_starters(raw_depth)
  
  promos <- get_inseason_promotions(
    starters %>% filter(position == "WR") %>% arrange(week)
  )
  # Week1: Wrynn & Rin; Week2: Wrynn & Rin; Week3: Wrynn & Cora
  # Promotions: Cora appears at week 3 replacing Rin -> TRUE row for Cora at week 3
  expect_true(all(promos$position == "WR"))
  expect_true(any(promos$player == "Cora C" & promos$week == 3L))
  # Lag(week1) is NA -> should not flag week1
  expect_false(any(promos$week == 1L))
})

# ------------------------------------------------------------------------------
# get_lineup_stability_by_week()
# ------------------------------------------------------------------------------

test_that("get_lineup_stability_by_week: produces scores in [0,1] by position_group", {
  starters <- make_starters(raw_depth) %>% add_position_group()
  
  stab <- get_lineup_stability_by_week(starters)
  expect_identical(
    names(stab),
    c("season", "week", "team", "position_group", "position_group_score")
  )
  
  # WR rows are grouped as REC in position_group; fixture has only WRs in REC
  wr_stab <- stab %>% filter(position_group == "REC") %>% arrange(week)
  
  # WR logic from fixture:
  # Week1 WR: Wrynn(1), Rin(1) -> position_count=2; running=2; starts=1+1=2; score=2/2=1.000
  # Week2 WR: Wrynn(2), Rin(2) -> running=4; starts=2+2=4; score=4/4=1.000
  # Week3 WR: Wrynn(3), Cora(1) -> running=6; starts=3+1=4; score=4/6=0.667
  expect_equal(wr_stab$position_group_score, c(1.000, 1.000, 0.667), tolerance = 1e-3)
  
  # Scores are within [0,1]
  expect_true(all(wr_stab$position_group_score >= 0 & wr_stab$position_group_score <= 1))
})
