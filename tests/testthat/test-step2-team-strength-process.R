# tests/testthat/test-step2-team-strength-process.R
# Context: step2_team_strength_process — BASIC, ROBUST TESTS ONLY.

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
})

# Keep deprecation chatter down
options(lifecycle_verbosity = "quiet")

# Resolve project root and source functions (mirror your calling script path)
ROOT <- normalizePath(file.path(testthat::test_path(), "..", ".."))
FUN_PATH <- file.path(ROOT, "etl", "R", "step2_process", "step2_team_strength_process_functions.R")
if (!file.exists(FUN_PATH)) {
  stop(paste0("Functions file not found at: ", FUN_PATH,
              " — are you running from the project root with nfl_modeler.Rproj?"))
}
source(FUN_PATH)

# -------------------------------------------------------------------
# Tiny helpers to build deterministic fixtures
# -------------------------------------------------------------------
mk_games <- function() {
  # Two REG-season games: SEA vs SF in weeks 1 and 2
  tibble::tibble(
    game_id   = c("g1","g2"),
    season    = c(2024L, 2024L),
    week      = c(1L, 2L),
    game_type = c("REG","REG"),
    home_team = c("SEA","SEA"),
    away_team = c("SF","SF")
  )
}

mk_pbp <- function() {
  # 24 plays per game (12 each offense), weights ~1.0, nice and reproducible
  # Week 1: SEA offense strong (+0.3), SF offense weak (-0.2)
  g1_sea_off <- tibble::tibble(
    game_id="g1", posteam="SEA", defteam="SF",
    epa = rep(0.3, 12), home_wp_post = 0.5, wpa = 0
  )
  g1_sf_off <- tibble::tibble(
    game_id="g1", posteam="SF", defteam="SEA",
    epa = rep(-0.2, 12), home_wp_post = 0.5, wpa = 0
  )
  # Week 2: SEA slight (+0.1), SF neutral (0.0)
  g2_sea_off <- tibble::tibble(
    game_id="g2", posteam="SEA", defteam="SF",
    epa = rep(0.1, 12), home_wp_post = 0.5, wpa = 0
  )
  g2_sf_off <- tibble::tibble(
    game_id="g2", posteam="SF", defteam="SEA",
    epa = rep(0.0, 12), home_wp_post = 0.5, wpa = 0
  )
  dplyr::bind_rows(g1_sea_off, g1_sf_off, g2_sea_off, g2_sf_off)
}

# -------------------------------------------------------------------
test_that(".ts_v01_attach_season_week attaches season/week and filters PRE", {
  games <- mk_games() %>%
    dplyr::bind_rows(tibble::tibble(
      game_id="gPRE", season=2024L, week=0L, game_type="PRE",
      home_team="SEA", away_team="SF"
    ))
  pbp <- tibble::tibble(
    game_id = c("g1","gPRE"),
    posteam = c("SEA","SEA"),
    defteam = c("SF","SF"),
    epa = c(0.1, 0.2),
    home_wp_post = c(0.5, 0.5),
    wpa = c(0, 0)
  )
  out <- suppressWarnings(.ts_v01_attach_season_week(pbp, games))
  expect_true(all(c("season","week") %in% names(out)))
  expect_true(!"gPRE" %in% out$game_id)   # PRE should be filtered out
})

test_that(".ts_v01_attach_season_week throws on missing required columns", {
  pbp_bad <- tibble::tibble(
    game_id="x", posteam="SEA", defteam="SF",
    epa=0.1, home_wp_post=0.5 # wpa missing
  )
  expect_error(.ts_v01_attach_season_week(pbp_bad, mk_games()),
               regexp = "missing required columns|pbp is missing")
})

# -------------------------------------------------------------------
test_that("build_team_strength_v01: returns expected columns and one row per week-team", {
  pbp   <- mk_pbp()
  games <- mk_games()
  
  ratings <- suppressWarnings(build_team_strength_v01(
    pbp, games = games, keep_components = FALSE
  ))
  
  expect_true(all(c("season","week","team","rating_net",
                    "net_epa_smooth","sos","n_plays_eff",
                    "params_version","run_timestamp") %in% names(ratings)))
  
  # One row per (season, week, team)
  expect_equal(sum(duplicated(ratings[c("season","week","team")])), 0L)
  
  # Exactly 4 rows: 2 teams x 2 weeks
  expect_equal(nrow(ratings), 4L)
})

# -------------------------------------------------------------------
test_that("schedule strength: week 1 sos = 0; week 2 sos = opponent smoothed week1", {
  pbp   <- mk_pbp()
  games <- mk_games()
  H     <- 4
  beta  <- 0.7
  
  ratings <- suppressWarnings(build_team_strength_v01(
    pbp, games = games, H = H, beta = beta, keep_components = FALSE
  ))
  
  # Week 1 neutral prior
  wk1 <- ratings %>% dplyr::filter(week == 1L)
  expect_true(all(abs(wk1$sos) < 1e-12))
  
  # Compute expected opponent smoothed week1 (equals net_week1 since first obs)
  # From construction: SEA week1 net = 0.3 + 0.2 = 0.5; SF week1 net = -0.5
  exp_opp_wk1 <- tibble::tibble(team = c("SEA","SF"),
                                exp_sos_wk2 = c(-0.5, 0.5))
  
  wk2 <- ratings %>%
    dplyr::filter(week == 2L) %>%
    dplyr::select(team, sos) %>%
    dplyr::inner_join(exp_opp_wk1, by = "team")
  
  expect_equal(wk2$sos, wk2$exp_sos_wk2, tolerance = 1e-12)
})

# -------------------------------------------------------------------
test_that("EWMA + adjustment produce sensible values; n_plays_eff is cumulative non-decreasing", {
  pbp   <- mk_pbp()
  games <- mk_games()
  H     <- 4
  beta  <- 0.7
  alpha <- 1 - 2^(-1 / H)
  
  ratings <- suppressWarnings(build_team_strength_v01(
    pbp, games = games, H = H, beta = beta, keep_components = FALSE
  ))
  
  # Expected SEA smoothing week2: alpha*0.1 + (1-alpha)*0.5
  exp_sea_net_smooth_w2 <- alpha * 0.1 + (1 - alpha) * 0.5
  sea_w2 <- ratings %>% dplyr::filter(team == "SEA", week == 2L) %>% dplyr::slice(1)
  expect_equal(sea_w2$net_epa_smooth, exp_sea_net_smooth_w2, tolerance = 1e-12)
  
  # Expected adj: adj = net_smooth - beta * sos; sos_w2 = -0.5 (opponent wk1)
  exp_sea_adj_w2 <- exp_sea_net_smooth_w2 - beta * (-0.5)
  
  tw_adj <- suppressWarnings(.ts_v01_schedule_adjust(
    .ts_v01_ewma_by_team_week(
      .ts_v01_team_week_from_team_game(
        .ts_v01_team_game_aggregates(
          .ts_v01_filter_weight_plays(.ts_v01_attach_season_week(pbp, games)),
          min_eff_plays = 20, keep_components = FALSE),
        games = games, keep_components = FALSE),
      alpha = alpha, keep_components = FALSE),
    tg = .ts_v01_team_game_aggregates(
      .ts_v01_filter_weight_plays(.ts_v01_attach_season_week(pbp, games)),
      min_eff_plays = 20, keep_components = FALSE),
    beta = beta, keep_components = FALSE))
  
  sea_adj_w2_actual <- tw_adj %>% dplyr::filter(team == "SEA", week == 2L) %>% dplyr::pull(adj_net)
  expect_equal(sea_adj_w2_actual, exp_sea_adj_w2, tolerance = 1e-12)
  
  # n_plays_eff must be non-decreasing by team over weeks
  chk <- ratings %>%
    dplyr::group_by(season, team) %>%
    dplyr::summarise(ok = all(diff(n_plays_eff) >= 0), .groups = "drop")
  expect_true(all(chk$ok))
})

# -------------------------------------------------------------------
test_that("Weekly z-scoring: within each week, mean ~ 0 and sd ~ 1", {
  pbp   <- mk_pbp()
  games <- mk_games()
  ratings <- suppressWarnings(build_team_strength_v01(
    pbp, games = games, keep_components = FALSE
  ))
  
  by_wk <- ratings %>%
    dplyr::group_by(season, week) %>%
    dplyr::summarise(
      m = mean(rating_net, na.rm = TRUE),
      s = stats::sd(rating_net, na.rm = TRUE),
      .groups = "drop"
    )
  expect_equal(by_wk$m, rep(0, nrow(by_wk)), tolerance = 1e-12)
  expect_equal(by_wk$s, rep(1, nrow(by_wk)), tolerance = 1e-12)
})

# -------------------------------------------------------------------
test_that("keep_components = TRUE adds columns and net ≈ off + def (smoothed)", {
  pbp   <- mk_pbp()
  games <- mk_games()
  H     <- 4
  alpha <- 1 - 2^(-1 / H)
  
  # Build with components
  ratings <- suppressWarnings(build_team_strength_v01(
    pbp, games = games, H = H, keep_components = TRUE
  ))
  expect_true(all(c("rating_off","rating_def") %in% names(ratings)))
  
  # Re-run internals to inspect smoothed components before scaling/selection
  tw_sm <- suppressWarnings(.ts_v01_ewma_by_team_week(
    .ts_v01_team_week_from_team_game(
      .ts_v01_team_game_aggregates(
        .ts_v01_filter_weight_plays(.ts_v01_attach_season_week(pbp, games)),
        min_eff_plays = 20, keep_components = TRUE),
      games = games, keep_components = TRUE),
    alpha = alpha, keep_components = TRUE))
  
  # Where components are finite, net_smooth ≈ off_smooth + def_smooth
  comp <- tw_sm %>%
    dplyr::filter(is.finite(off_epa_smooth), is.finite(def_epa_smooth), is.finite(net_epa_smooth)) %>%
    dplyr::mutate(diff = abs(net_epa_smooth - (off_epa_smooth + def_epa_smooth)))
  expect_true(all(comp$diff < 1e-12))
})
