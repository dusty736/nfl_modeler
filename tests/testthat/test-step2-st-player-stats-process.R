# tests/testthat/test-step2-st-player-process.R
# Special teams tests, Hugh-Grant-level polite, mildly sassy, zero frills.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tidyr)   # your functions use replace_na(); I'm simply being helpful.
  library(tibble)
})

testthat::local_edition(3)

# --- Load functions under test ------------------------------------------------
suppressMessages({
  source(here::here("etl", "R", "step2_process", "step2_st_player_stats_process_functions.R"))
})

# --- Tiny raw ST stats --------------------------------------------------------
# Note the raw column names fg_made_60_ / fg_missed_60_ — your function maps them.
toy_raw <- tibble::tibble(
  season = c(2024L, 2024L, 2024L, 2024L, 2024L),
  week   = c(1L, 2L, 3L, 1L, 1L),
  season_type = c("REG","REG","REG","REG","REG"),
  player_id   = c("P1","P1","P1","P2","P2"),
  player_display_name = c("Kicker A","Kicker A","Kicker A","Kicker B","Kicker B"),
  team        = c("SEA","SEA",NA,"DAL","DAL"),
  position    = c("K","K","K","K","K"),
  
  fg_att     = c(3L, 2L, NA, 0L, NA),
  fg_made    = c(2L, 1L, NA, 0L, NA),
  fg_missed  = c(1L, 0L, NA, 0L, NA),
  fg_blocked = c(0L, 1L, NA, 0L, NA),
  fg_long    = c(52, 33, NA, NA, NA),
  fg_pct     = c(2/3, 1/2, NA, NA, NA),
  
  fg_made_0_19 = c(1L, 0L, NA, 0L, NA),
  fg_made_20_29 = c(0L, 0L, NA, 0L, NA),
  fg_made_30_39 = c(1L, 1L, NA, 0L, NA),
  fg_made_40_49 = c(0L, 0L, NA, 0L, NA),
  fg_made_50_59 = c(0L, 0L, NA, 0L, NA),
  fg_made_60_   = c(0L, 0L, NA, 0L, NA),
  
  fg_missed_0_19 = c(0L, 0L, NA, 0L, NA),
  fg_missed_20_29 = c(1L, 0L, NA, 0L, NA),
  fg_missed_30_39 = c(0L, 0L, NA, 0L, NA),
  fg_missed_40_49 = c(0L, 0L, NA, 0L, NA),
  fg_missed_50_59 = c(0L, 0L, NA, 0L, NA),
  fg_missed_60_   = c(0L, 0L, NA, 0L, NA),
  
  fg_made_distance    = c(52, 33, NA, NA, NA),
  fg_missed_distance  = c(28, NA, NA, NA, NA),
  fg_blocked_distance = c(NA, 48, NA, NA, NA),
  
  pat_att    = c(3L, 2L, NA, 0L, NA),
  pat_made   = c(3L, 1L, NA, 0L, NA),
  pat_missed = c(0L, 1L, NA, 0L, NA),
  pat_blocked= c(0L, 0L, NA, 0L, NA),
  pat_pct    = c(1, .5, NA, NA, NA),
  
  gwfg_att      = c(1L, 0L, NA, 0L, NA),
  gwfg_distance = c(42, NA, NA, NA, NA),
  gwfg_made     = c(1L, 0L, NA, 0L, NA),
  gwfg_missed   = c(0L, 0L, NA, 0L, NA),
  gwfg_blocked  = c(0L, 0L, NA, 0L, NA)
)

# ------------------------------------------------------------------------------
# process_special_teams_stats
# ------------------------------------------------------------------------------
test_that("process_special_teams_stats: keeps columns, types, and 60+ bins mapping", {
  cln <- process_special_teams_stats(toy_raw)
  
  # Required columns present
  expect_true(all(c(
    "season","week","season_type","player_id","player_name","team","position",
    "fg_att","fg_made","fg_missed","fg_blocked","fg_long","fg_pct",
    "fg_made_0_19","fg_made_20_29","fg_made_30_39","fg_made_40_49","fg_made_50_59","fg_made_60",
    "fg_missed_0_19","fg_missed_20_29","fg_missed_30_39","fg_missed_40_49","fg_missed_50_59","fg_missed_60",
    "fg_made_distance","fg_missed_distance","fg_blocked_distance",
    "pat_att","pat_made","pat_missed","pat_blocked","pat_pct",
    "gwfg_att","gwfg_distance","gwfg_made","gwfg_missed","gwfg_blocked"
  ) %in% names(cln)))
  
  # Type checks (spot)
  expect_type(cln$fg_att, "integer")
  expect_type(cln$fg_made, "integer")
  expect_type(cln$fg_blocked, "integer")
  expect_type(cln$fg_long, "double")
  expect_type(cln$fg_pct, "double")
  expect_type(cln$fg_made_60, "integer")
  expect_type(cln$fg_missed_60, "integer")
  
  # Mapping from *_60_ -> *_60 works
  p1_w1 <- cln %>% filter(player_id=="P1", week==1)
  expect_equal(p1_w1$fg_made_60, 0L)
  expect_equal(p1_w1$fg_missed_60, 0L)
})

# ------------------------------------------------------------------------------
# add_cumulative_special_teams_stats
# ------------------------------------------------------------------------------
test_that("add_cumulative_special_teams_stats: cumulative tallies and pct behave", {
  cln <- process_special_teams_stats(toy_raw)
  cum <- add_cumulative_special_teams_stats(cln)
  
  # One output row per input row (we’re not in a Wes Anderson montage)
  expect_equal(nrow(cum), nrow(cln))
  
  # Player P1 cumulative checks across weeks 1->2->3
  p1 <- cum %>% filter(player_id=="P1") %>% arrange(week)
  
  # Week 1
  expect_equal(p1$cumulative_fg_att[1], 3)
  expect_equal(p1$cumulative_fg_made[1], 2)
  expect_equal(round(p1$cumulative_fg_pct[1], 3), round(2/3, 3))
  expect_equal(p1$cumulative_pat_att[1], 3)
  expect_equal(p1$cumulative_pat_made[1], 3)
  expect_equal(p1$cumulative_pat_pct[1], 1)
  
  # Week 2 (adds 2 att, 1 made; adds 2 PAT att, 1 made)
  expect_equal(p1$cumulative_fg_att[2], 5)
  expect_equal(p1$cumulative_fg_made[2], 3)
  expect_equal(round(p1$cumulative_fg_pct[2], 3), round(3/5, 3))
  expect_equal(p1$cumulative_pat_att[2], 5)
  expect_equal(p1$cumulative_pat_made[2], 4)
  expect_equal(round(p1$cumulative_pat_pct[2], 3), round(4/5, 3))
  
  # Week 3 has NAs/zeros -> cumulative unchanged
  expect_equal(p1$cumulative_fg_att[3], 5)
  expect_equal(p1$cumulative_fg_made[3], 3)
  expect_equal(round(p1$cumulative_fg_pct[3], 3), round(3/5, 3))
  
  # Monotone non-decreasing on cumulative counts per player-season
  cum_cols <- grep("^cumulative_", names(cum), value = TRUE)
  cum_cols <- cum_cols[!grepl("_pct$", cum_cols)]  # counts only, darling
  
  mono <- cum %>%
    dplyr::group_by(season, player_id) %>%
    dplyr::summarize(
      ok = all(unlist(lapply(
        dplyr::across(dplyr::all_of(cum_cols)),
        function(x) {
          x <- x[!is.na(x)]
          length(x) <= 1 || all(diff(x) >= 0)
        }
      ))),
      .groups = "drop"
    )
  
  expect_true(all(mono$ok))
  
  # Player P2 had zero attempts → cumulative pct stays NA (no gaslighting)
  p2 <- cum %>% filter(player_id=="P2")
  expect_true(all(is.na(p2$cumulative_fg_pct)))
  expect_true(all(is.na(p2$cumulative_pat_pct)))
})

# ------------------------------------------------------------------------------
# summarize_special_teams_by_season
# ------------------------------------------------------------------------------
test_that("summarize_special_teams_by_season: season sums, max long, pct from totals", {
  cln <- process_special_teams_stats(toy_raw)
  sum_season <- summarize_special_teams_by_season(cln)
  
  # P1 totals across weeks: FG att=3+2+0=5, made=3, miss=1, blocked=1
  # fg_pct = 3/5 = 0.6, fg_long = max(52,33,NA) = 52
  # PAT att=3+2+0=5, made=4 -> pat_pct = 0.8
  # Games played = 3 (distinct weeks)
  p1 <- sum_season %>% filter(season==2024, player_id=="P1")
  expect_equal(p1$games_played, 3)
  expect_equal(p1$fg_att, 5)
  expect_equal(p1$fg_made, 3)
  expect_equal(p1$fg_missed, 1)
  expect_equal(p1$fg_blocked, 1)
  expect_equal(p1$fg_long, 52)
  expect_equal(round(p1$fg_pct, 3), round(3/5, 3))
  expect_equal(p1$pat_att, 5)
  expect_equal(p1$pat_made, 4)
  expect_equal(round(p1$pat_pct, 3), round(4/5, 3))
  
  # Bins and distances add up
  expect_equal(p1$fg_made_0_19, 1)
  expect_equal(p1$fg_made_30_39, 2)
  expect_equal(p1$fg_missed_20_29, 1)
  expect_equal(p1$fg_blocked_distance, 48)
  
  # P2 had only zeros/NAs → pct fields are NA; fg_long is NA when all are NA
  p2 <- sum_season %>% filter(season==2024, player_id=="P2")
  expect_true(is.na(p2$fg_pct))
  expect_true(is.na(p2$pat_pct))
  expect_true(is.na(p2$fg_long))
})
