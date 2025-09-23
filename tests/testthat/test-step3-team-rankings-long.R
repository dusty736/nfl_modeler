# tests/testthat/test-step3-team-rankings-long.R
# Basic, robust tests for rank_team_stats_weekly()

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(here)
})

# Load function under test
source(here::here("etl", "R", "step3_sql", "step3_team_rankings_long_functions.R"))

test_that("rank_team_stats_weekly: shapes, auto-derive def_*_allowed, sums vs means, invert, opp_rank", {
  # Two teams, two weeks, neat and tidy
  # Week 1 values -> used as "previous" for Week 2 ranks
  base <- tibble::tibble(
    team        = c("SEA","SF","SEA","SF","SEA","SF","SEA","SF","SEA","SF","SEA","SF"),
    opponent    = c("SF","SEA","SF","SEA","SF","SEA","SF","SEA","SF","SEA","SF","SEA"),
    season      = 2024L,
    season_type = "REG",
    week        = c(1L,1L, 1L,1L, 1L,1L, 2L,2L, 2L,2L, 2L,2L),
    stat_type   = "base",
    stat_name   = c(
      # Week 1 — offense (sum) + mean + invert
      "passing_yards","passing_yards",
      "passing_epa","passing_epa",
      "points_allowed","points_allowed",
      # Week 2 — offense (sum) + mean + invert
      "passing_yards","passing_yards",
      "passing_epa","passing_epa",
      "points_allowed","points_allowed"
    ),
    value       = c(
      # Week 1 values (feed Week 2 ranks)
      300, 150,
      0.20, 0.10,
      24, 31,
      # Week 2 (won’t affect Week 2 ranks)
      100, 400,
      0.00, 0.30,
      10, 7
    )
  ) %>%
    mutate(
      # Same game_id for both teams within a week; differs across weeks
      game_id = if_else(week == 1L, "2024_01_SEA_SF", "2024_02_SEA_SF")
    )
  
  # We intentionally DO NOT include any def_*_allowed rows;
  # the function should auto-derive them from opponent’s offense.
  ranking_stats <- c(
    "passing_yards",        # sum-type (higher = better)
    "passing_epa",          # mean-type (higher = better)
    "points_allowed",       # sum/invert (lower = better)
    "def_passing_yards_allowed" # derived from opponent offense; invert (lower = better)
  )
  
  out <- rank_team_stats_weekly(
    df = base,
    ranking_stats = ranking_stats,
    mean_stats = c("passing_epa"),
    invert_stats = c("points_allowed","def_passing_yards_allowed"),
    keep_agg_value = TRUE,
    stat_type_keep = "base"
  )
  
  # Basic columns present
  expect_true(all(c("season","week","team","opponent","stat_name","rank","n_teams") %in% names(out)))
  expect_true("agg_value_prev" %in% names(out))  # keep_agg_value = TRUE
  
  # n_teams == 2 everywhere
  expect_true(all(out$n_teams == 2L))
  
  # Focus assertions on WEEK 2 (ranks computed from WEEK 1 values)
  w2 <- out %>% filter(week == 2L, season == 2024L)
  
  # --- passing_yards (sum) ---
  # Week 1 sums: SEA=300, SF=150 => Week 2 rank: SEA=1, SF=2 (desc)
  py <- w2 %>% filter(stat_name == "passing_yards")
  expect_equal(py %>% arrange(team) %>% pull(agg_value_prev), c(300,150))  # SEA,SF order after arrange(team)
  expect_equal(py %>% filter(team == "SEA") %>% pull(rank), 1)
  expect_equal(py %>% filter(team == "SF")  %>% pull(rank), 2)
  
  # --- passing_epa (mean) ---
  # Week 1 means: SEA=0.20, SF=0.10 => Week 2 rank: SEA=1, SF=2 (desc)
  pe <- w2 %>% filter(stat_name == "passing_epa")
  # allow small numeric wiggle room
  expect_true(abs(pe %>% filter(team=="SEA") %>% pull(agg_value_prev) - 0.20) < 1e-9)
  expect_true(abs(pe %>% filter(team=="SF")  %>% pull(agg_value_prev) - 0.10) < 1e-9)
  expect_equal(pe %>% filter(team == "SEA") %>% pull(rank), 1)
  expect_equal(pe %>% filter(team == "SF")  %>% pull(rank), 2)
  
  # --- points_allowed (invert) ---
  # Week 1 sums: SEA=24, SF=31 => lower better => Week 2 rank: SEA=1, SF=2
  pa <- w2 %>% filter(stat_name == "points_allowed")
  expect_equal(pa %>% filter(team=="SEA") %>% pull(agg_value_prev), 24)
  expect_equal(pa %>% filter(team=="SF")  %>% pull(agg_value_prev), 31)
  expect_equal(pa %>% filter(team=="SEA") %>% pull(rank), 1)
  expect_equal(pa %>% filter(team=="SF")  %>% pull(rank), 2)
  
  # --- def_passing_yards_allowed (derived + invert) ---
  # Derived from opponent’s Week 1 offense:
  # SEA allowed = SF's wk1 passing_yards = 150; SF allowed = SEA's wk1 = 300
  dpy <- w2 %>% filter(stat_name == "def_passing_yards_allowed")
  # The function may create allowed rows internally; ranks should reflect lower=better
  # SEA should have 150 (rank 1), SF should have 300 (rank 2)
  # Note: We don't assert the *exact* presence pre-derivation; we check ranks & prev values.
  sea_allowed_prev <- dpy %>% filter(team=="SEA") %>% pull(agg_value_prev)
  sf_allowed_prev  <- dpy %>% filter(team=="SF")  %>% pull(agg_value_prev)
  expect_true(!is.na(sea_allowed_prev) && !is.na(sf_allowed_prev))
  expect_equal(unname(sea_allowed_prev), 150)
  expect_equal(unname(sf_allowed_prev),  300)
  expect_equal(dpy %>% filter(team=="SEA") %>% pull(rank), 1)
  expect_equal(dpy %>% filter(team=="SF")  %>% pull(rank), 2)
  
  # --- opponent rank join sanity: for Week 2 passing_yards, opp_rank should equal the other team’s rank
  py_w2 <- w2 %>% filter(stat_name == "passing_yards") %>% select(team, opponent, rank, opp_rank)
  expect_equal(py_w2 %>% filter(team=="SEA") %>% pull(opp_rank),
               py_w2 %>% filter(team=="SF")  %>% pull(rank))
  expect_equal(py_w2 %>% filter(team=="SF")  %>% pull(opp_rank),
               py_w2 %>% filter(team=="SEA") %>% pull(rank))
})
