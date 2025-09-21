# tests/testthat/test-step2-espn-qbr-functions.R

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(here)
  library(arrow)   # only for optional smoke test if parquet exists
})

testthat::local_edition(3)

# ------------------------------------------------------------------------------
# Source functions under test (robust pathing)
# ------------------------------------------------------------------------------
fpath <- here::here("etl", "R", "step2_process", "step2_espn_qbr_process_functions.R")
if (!file.exists(fpath)) {
  stop("Cannot find functions file: ", fpath)
}
source(fpath)

# ------------------------------------------------------------------------------
# Tiny fixtures (Season Total rows + a couple that should be filtered out)
# ------------------------------------------------------------------------------

toy_qbr <- tibble::tribble(
  ~season, ~season_type, ~game_week,      ~team_abb, ~team, ~player_id, ~name_display, ~qbr_total, ~qbr_raw, ~qb_plays, ~pts_added, ~epa_total, ~pass, ~run, ~sack, ~exp_sack, ~penalty, ~qualified,
  2022L,   "REG",        "Season Total",  "SEA",     "Seah",  1L,        "Alpha A",     70,         65,       400,       50,          45,         30,    10,    5,      2,         1,         TRUE,
  2023L,   "Regular",    "Season Total",  "SF",      "Niners",1L,        "Alpha A",     75,         70,       300,       40,          35,         25,    8,     4,      2,         0,         TRUE,
  2023L,   "POST",       "Season Total",  "SF",      "Niners",1L,        "Alpha A",     60,         58,       50,        6,           5,          4,     1,     0,      0,         0,         FALSE,
  2022L,   "REG",        "Week 5",        "SEA",     "Seah",  1L,        "Alpha A",     71,         66,       35,        5,           4,          3,     1,     0,      0,         0,         TRUE,     # should be filtered out
  2022L,   "POST",       "Season Total",  "KC",      "Chiefs",2L,        "Bravo B",     NA_real_,   50,       NA,        0,           0,          0,     0,     0,      0,         0,         NA,       # qb_plays NA -> 0; qbr_total stays NA
  2023L,   "Playoffs",   "Season Total",  "KC",      "Chiefs",2L,        "Bravo B",     55,         NA_real_, 0,         0,           0,          0,     0,     0,      0,         0,         FALSE    # qb_plays 0 -> weighted QBR NA
)

# Expected output columns from prepare()
prepare_cols <- c(
  "season","season_type","team_abb","team","player_id","name_display",
  "qbr_total","qbr_raw","qb_plays","pts_added","epa_total","pass","run","sack","exp_sack","penalty","qualified"
)

# ------------------------------------------------------------------------------
# prepare_espn_qbr_season_totals()
# ------------------------------------------------------------------------------

test_that("prepare_espn_qbr_season_totals: filters Season Total, normalizes types/values, returns expected columns", {
  got <- prepare_espn_qbr_season_totals(toy_qbr)
  
  # Filters out non-Season Total rows
  expect_true(all(got$season %in% c(2022L, 2023L)))
  expect_true(all(got$season_type %in% c("Regular","Playoffs")))  # REG/POST normalized
  expect_identical(names(got), prepare_cols)
  
  # Coalescing rules
  # - qb_plays NA -> 0
  expect_equal(
    got$qb_plays[got$player_id==2 & got$season==2022L & got$season_type=="Playoffs"],
    0
  )
  # - qbr_total, qbr_raw remain NA if missing (coalesced to NA_real_)
  expect_true(is.na(got$qbr_total[got$player_id==2 & got$season==2022L]))
  expect_true(is.na(got$qbr_raw  [got$player_id==2 & got$season==2023L]))
  
  # Zero-row behaviour
  empty <- toy_qbr[0, ]
  empty_out <- prepare_espn_qbr_season_totals(empty)
  expect_identical(names(empty_out), prepare_cols)
  expect_identical(nrow(empty_out), 0L)
})

test_that("prepare_espn_qbr_season_totals: missing required column yields helpful error", {
  bad <- dplyr::select(toy_qbr, -game_week)
  expect_error(prepare_espn_qbr_season_totals(bad), regexp = "game_week|object")
})

# ------------------------------------------------------------------------------
# summarize_espn_qbr_career_by_season_type()
# ------------------------------------------------------------------------------

test_that("summarize_espn_qbr_career_by_season_type: aggregates per (player, season_type) with weighted QBRs", {
  season_totals <- prepare_espn_qbr_season_totals(toy_qbr)
  out <- summarize_espn_qbr_career_by_season_type(season_totals)
  
  expect_s3_class(out, "tbl_df")
  expect_true(all(c("player_id","name_display","season_type") %in% names(out)))
  
  # Alpha A (player_id=1), Regular: two seasons (2022 SEA, 2023 SF)
  aa_reg <- out %>% filter(player_id==1, season_type=="Regular")
  expect_equal(nrow(aa_reg), 1L)
  expect_equal(aa_reg$first_season, 2022)
  expect_equal(aa_reg$last_season,  2023)
  expect_equal(aa_reg$seasons_played, 2)
  expect_equal(aa_reg$teams_played_for, 2)   # SEA + SF
  expect_equal(aa_reg$qb_plays, 700)         # 400 + 300
  expect_equal(aa_reg$pts_added, 90)         # 50 + 40
  expect_equal(aa_reg$epa, 80)               # 45 + 35
  
  # Weighted QBRs
  # qbr_total_w = (70*400 + 75*300) / (400+300) = (28000 + 22500)/700 = 72.142857...
  expect_equal(aa_reg$qbr_total_w, (70*400 + 75*300) / 700, tolerance = 1e-10)
  # qbr_raw_w   = (65*400 + 70*300) / 700 = (26000 + 21000)/700 = 67.142857...
  expect_equal(aa_reg$qbr_raw_w,   (65*400 + 70*300) / 700, tolerance = 1e-10)
  
  # Alpha A, Playoffs: one season (2023 SF)
  aa_post <- out %>% filter(player_id==1, season_type=="Playoffs")
  expect_equal(nrow(aa_post), 1L)
  expect_equal(aa_post$qb_plays, 50)
  expect_equal(aa_post$qbr_total_w, 60)
  expect_equal(aa_post$qbr_raw_w,   58)
  expect_equal(aa_post$qualified_seasons, 0)  # FALSE does not count
  
  # Bravo B (player_id=2), Regular: no Season Total rows? (only Post 2022 with NA plays + Post 2023 with 0 plays)
  # From toy data, there is no Regular Season Total for player 2; ensure absence is handled
  bb_reg <- out %>% filter(player_id==2, season_type=="Regular")
  expect_equal(nrow(bb_reg), 0L)
  
  # Bravo B, Playoffs: two rows (2022 NA plays->0; 2023 plays 0) -> qb_plays sum == 0 => weighted QBRs NA
  bb_post <- out %>% filter(player_id==2, season_type=="Playoffs")
  expect_equal(nrow(bb_post), 1L)
  expect_equal(bb_post$qb_plays, 0)
  expect_true(is.na(bb_post$qbr_total_w))
  expect_true(is.na(bb_post$qbr_raw_w))
  # qualified_seasons treats NA as FALSE
  expect_equal(bb_post$qualified_seasons, 0)
})

test_that("summarize_espn_qbr_career_by_season_type: zero-row input returns zero-row tibble", {
  empty <- prepare_espn_qbr_season_totals(toy_qbr[0, ])
  out <- summarize_espn_qbr_career_by_season_type(empty)
  expect_s3_class(out, "tbl_df")
  expect_identical(nrow(out), 0L)
})

# ------------------------------------------------------------------------------
# espn_qbr_career_by_season_type() wrapper
# ------------------------------------------------------------------------------

test_that("espn_qbr_career_by_season_type: equals prepare |> summarize with numeric rounding to 3dp", {
  season_totals <- prepare_espn_qbr_season_totals(toy_qbr)
  two_step <- summarize_espn_qbr_career_by_season_type(season_totals)
  
  # Round all numeric columns to 3 like the wrapper does
  two_step_round3 <- two_step %>%
    dplyr::mutate(dplyr::across(where(is.numeric), ~ round(.x, 3)))
  
  wrapped <- espn_qbr_career_by_season_type(toy_qbr)
  
  # Same rows / grouping keys
  expect_equal(
    dplyr::arrange(two_step_round3, name_display, season_type),
    dplyr::arrange(wrapped,        name_display, season_type)
  )
})

# ------------------------------------------------------------------------------
# Optional smoke test with real parquet (if present)
# ------------------------------------------------------------------------------

test_that("prepare/summarize run on real parquet slice (if present)", {
  raw_path <- here::here("data", "raw", "espn_qbr.parquet")
  skip_if_not(file.exists(raw_path), "espn_qbr.parquet not found; skipping smoke test")
  
  raw <- arrow::read_parquet(raw_path) %>% dplyr::slice_head(n = 2000)
  season_totals <- prepare_espn_qbr_season_totals(raw)
  out <- summarize_espn_qbr_career_by_season_type(season_totals)
  
  expect_s3_class(season_totals, "tbl_df")
  expect_s3_class(out, "tbl_df")
  expect_true(all(c("player_id","name_display","season_type") %in% names(out)))
})
