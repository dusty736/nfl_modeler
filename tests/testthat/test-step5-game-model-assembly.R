# tests/testthat/test-step5-game-model-assembly.R
# Simple tests for step 5 functions (no Postgres required)

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(here)
})

# Load functions under test
source(here::here("etl", "R", "step5_modeling_data", "step5_game_model_assembly_functions.R"))

# ------------------------------------------------------------------------------
# finalize_targets_and_clean ----------------------------------------------------
# ------------------------------------------------------------------------------
test_that("finalize_targets_and_clean computes margin/total and flips spread sign if needed", {
  # Construct a tiny toy set where spread_line is NEGATIVELY correlated with margin
  home_score  <- c(24, 17, 14, 21)
  away_score  <- c(20, 24, 21, 24)
  spread_line <- c(-4, 7, 7, 3)  # roughly -margin; negative correlation expected
  
  df <- tibble::tibble(
    game_id     = c("G1","G2","G3","G4"),
    home_score  = home_score,
    away_score  = away_score,
    spread_line = spread_line,
    # IMPORTANT for this function: provide home_win like the pipeline does upstream
    home_win    = ifelse(home_score > away_score, 1L,
                         ifelse(home_score < away_score, 0L, NA_integer_))
  )
  
  out <- finalize_targets_and_clean(df, push_as_met = TRUE)
  
  # Basic target math
  expect_equal(out$margin,       df$home_score - df$away_score)
  expect_equal(out$total_points, df$home_score + df$away_score)
  
  # Because corr(spread_line, margin) < 0, spread_home should be -spread_line
  expect_equal(out$spread_home, -df$spread_line)
  
  # With push_as_met=TRUE, (margin - spread_home) == 0 ⇒ covered (1)
  expect_true(all((out$margin - out$spread_home) >= 0))
  expect_true(all(out$spread_covered == 1L))
  
  # Targets relocated to the end in this order
  tail4 <- tail(names(out), 4)
  expect_identical(tail4, c("home_win", "margin", "spread_covered", "total_points"))
})

test_that("finalize_targets_and_clean respects push_as_met = FALSE (> 0 only)", {
  # Vary values so cor(spread_line, margin) is defined and positive
  home_score  <- c(30, 32)   # margins: 10, 12
  away_score  <- c(20, 20)
  spread_line <- c(10, 12)   # exactly equals margin -> diff == 0
  
  df <- tibble::tibble(
    game_id     = c("G1","G2"),
    home_score  = home_score,
    away_score  = away_score,
    spread_line = spread_line,
    home_win    = ifelse(home_score > away_score, 1L,
                         ifelse(home_score < away_score, 0L, NA_integer_))
  )
  
  out_strict <- finalize_targets_and_clean(df, push_as_met = FALSE)
  
  # corr > 0 => spread_home == spread_line (no flip)
  expect_equal(out_strict$spread_home, df$spread_line)
  
  # margin - spread_home == 0 in both rows
  expect_true(all(out_strict$margin - out_strict$spread_home == 0))
  
  # With strict rule (> 0), a push should NOT be covered
  expect_true(all(out_strict$spread_covered == 0L))
})

# ------------------------------------------------------------------------------
# add_team_rankings_features (Postgres via RPostgres) ---------------------------
# ------------------------------------------------------------------------------
test_that("add_team_rankings_features joins ranks, builds home/away and diff columns [RPostgres]", {
  skip_if_not_installed("RPostgres")
  skip_if_not_installed("dbplyr")
  
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    dbname   = "nfl",
    host     = "localhost",
    port     = 5432,
    user     = "nfl_user",
    password = "nfl_pass"
  )
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  
  # Unique temp table name in prod schema
  tmp_tbl <- paste0("team_weekly_rankings_tbl_tmp_", as.integer(runif(1, 1e8, 1e9)))
  
  # Minimal rankings data (one week, two stats)
  ranks <- dplyr::bind_rows(
    tibble::tibble(
      season    = 2024L,
      week      = 2L,
      team      = c("SEA", "SF", "LA", "LAC"),
      stat_name = "passing_yards",
      rank      = c(10, 5, 15, 20)
    ),
    tibble::tibble(
      season    = 2024L,
      week      = 2L,
      team      = c("SEA", "SF", "LA", "LAC"),
      stat_name = "def_passing_yards_allowed",
      rank      = c(8, 12, 7, 14)
    )
  )
  
  # Write to prod.<tmp_tbl>
  DBI::dbWriteTable(
    con,
    DBI::Id(schema = "prod", table = tmp_tbl),
    ranks,
    overwrite = TRUE
  )
  # Ensure cleanup
  withr::defer(
    try(DBI::dbExecute(con, sprintf('DROP TABLE IF EXISTS "prod"."%s";', tmp_tbl)), silent = TRUE),
    envir = parent.frame()
  )
  
  games <- tibble::tibble(
    game_id   = c("2024_02_SEA_SF", "2024_02_STL_SD"),
    season    = 2024L,
    week      = 2L,
    home_team = c("SEA", "STL"),   # STL -> LA
    away_team = c("SF",  "SD")     # SD  -> LAC
  )
  
  out <- add_team_rankings_features(
    con              = con,
    df               = games,
    seasons          = 2024L,
    schema           = "prod",
    rankings_table   = tmp_tbl,  # <- point to temp table
    ranking_stats    = c("passing_yards", "def_passing_yards_allowed"),
    include_pace_pairs = FALSE,
    verbose          = FALSE
  )
  
  # Expect wide columns
  expect_true(all(c(
    "home_passing_yards_rank",
    "away_passing_yards_rank",
    "home_def_passing_yards_allowed_rank",
    "away_def_passing_yards_allowed_rank"
  ) %in% names(out)))
  
  # Expect diff columns (generic + matchup)
  expect_true("diff_passing_yards_rank" %in% names(out))
  expect_true("diff_passing_yards_vs_def_passing_yards_allowed_rank" %in% names(out))
  
  # Row 1: SEA vs SF
  r1 <- out %>% dplyr::filter(game_id == "2024_02_SEA_SF")
  expect_equal(r1$home_passing_yards_rank, 10)
  expect_equal(r1$away_passing_yards_rank,  5)
  expect_equal(r1$diff_passing_yards_rank, 10 - 5)
  expect_equal(r1$`diff_passing_yards_vs_def_passing_yards_allowed_rank`, 10 - 12)
  
  # Row 2: STL->LA, SD->LAC
  r2 <- out %>% dplyr::filter(game_id == "2024_02_STL_SD")
  expect_equal(r2$home_passing_yards_rank, 15)  # LA
  expect_equal(r2$away_passing_yards_rank, 20)  # LAC
  expect_equal(r2$diff_passing_yards_rank, 15 - 20)
  expect_equal(r2$`diff_passing_yards_vs_def_passing_yards_allowed_rank`, 15 - 14)
  
  # diff_* columns relocated to the end
  diff_cols <- grep("^diff_.*_rank$", names(out), value = TRUE)
  expect_true(all(tail(names(out), length(diff_cols)) == diff_cols))
})
