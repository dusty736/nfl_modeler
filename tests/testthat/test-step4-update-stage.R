# tests/testthat/test-step4-weekly-upload-to-db.R
# Purpose: SIMPLE, non-flaky checks for Step 4 weekly upload to DB.
# Hugh Grant voice: quick look, approving nod, move along.

suppressPackageStartupMessages({
  library(testthat)
  library(here)
  library(stringr)
})

# --- Where might the script be? ------------------------------------------------
candidates <- c(
  here("etl", "R", "step4_update_db", "step4_weekly_upload_to_db.R"),
  here("etl", "R", "step4_update_db", "step4_weekly_upload_to_db.r")
)

script_path <- candidates[file.exists(candidates)][1]

test_that("weekly upload script exists in an expected location", {
  msg <- paste("Checked:\n- ", paste(candidates, collapse = "\n- "))
  expect_true(!is.na(script_path) && nzchar(script_path), info = msg)
})

skip_if(is.na(script_path), "Weekly upload script not found; skipping further checks.")

# Read whole file once
lines <- readLines(script_path, warn = FALSE)
txt   <- paste(lines, collapse = "\n")

# --- Parse-only (no execution) -------------------------------------------------
test_that("script is syntactically valid (parsed, not executed)", {
  expect_error(parse(file = script_path), NA)
})

# --- Sources the helpers you depend on -----------------------------------------
test_that("script sources the expected helper files", {
  expect_true(str_detect(txt, "step3_parquet_to_postgres_functions\\.R"))
  expect_true(str_detect(txt, "step3_database_file_prep_functions\\.R"))
  expect_true(str_detect(txt, "utils\\.R"))
})

# --- Stage schema lifecycle ----------------------------------------------------
test_that("script drops and (re)creates the stage schema", {
  expect_true(str_detect(txt, "drop_schema\\s*\\(\\s*con\\s*,\\s*schema\\s*=\\s*\"stage\"\\s*\\)"))
  expect_true(str_detect(txt, "create_schema\\s*\\(\\s*con\\s*,\\s*schema\\s*=\\s*\"stage\"\\s*\\)"))
})

# --- Base path for staging -----------------------------------------------------
test_that("script defines base_path as data/staging", {
  expect_true(str_detect(txt, 'base_path\\s*<-\\s*"data/staging"|base_path\\s*<-\\s*"data\\/staging"'))
})

# --- Spot-check a few formatter calls (structure present) ----------------------
test_that("script calls format/form helpers for key datasets", {
  expect_true(str_detect(txt, "format_game_for_sql\\s*\\("))
  expect_true(str_detect(txt, "format_weeks_for_sql\\s*\\("))
  expect_true(str_detect(txt, "format_roster_for_sql\\s*\\("))
  expect_true(str_detect(txt, "format_roster_summary_for_sql\\s*\\("))
})

# --- Must-have loads into stage (by table name) --------------------------------
test_that("script loads core tables into stage via load_parquet_to_postgres", {
  # Grab small windows after each load_parquet_to_postgres( occurrence.
  windows_after <- function(fun, span = 500L) {
    m <- stringr::str_locate_all(txt, paste0(fun, "\\s*\\("))[[1]]
    if (nrow(m) == 0) return(character(0))
    starts <- m[,1]
    ends   <- pmin(starts + span, nchar(txt))
    vapply(seq_along(starts), function(i) substr(txt, starts[i], ends[i]), FUN.VALUE = character(1))
  }
  
  loads <- windows_after("load_parquet_to_postgres")
  
  has_loader <- function(tbl) {
    if (!length(loads)) return(FALSE)
    any(stringr::str_detect(loads, paste0("[\"']", tbl, "[\"']")))
  }
  
  core_tables <- c(
    "pbp_tbl", "pbp_games_tbl", "games_tbl", "season_results_tbl", "weekly_results_tbl",
    "rosters_tbl", "weekly_stats_qb_tbl", "team_weekly_tbl", "player_weekly_tbl",
    "team_strength_tbl", "id_map_tbl", "team_weekly_rankings_tbl"
  )
  
  for (tbl in core_tables) {
    expect_true(has_loader(tbl), info = paste0("Expected a load_parquet_to_postgres(..., ", shQuote(tbl), " ) call"))
  }
})

# --- Indexing: spot-check id_cols and uniqueness on a few high-stakes tables ---
# helper: collect small windows after create_index( to avoid parenthesis hell
create_index_windows <- local({
  windows <- NULL
  function(span = 600L) {
    if (!is.null(windows)) return(windows)
    m <- stringr::str_locate_all(txt, "create_index\\s*\\(")[[1]]
    if (nrow(m) == 0) return(character(0))
    starts <- m[,1]
    ends   <- pmin(starts + span, nchar(txt))
    windows <<- vapply(seq_along(starts), function(i) substr(txt, starts[i], ends[i]), FUN.VALUE = character(1))
    windows
  }
})

test_that("script creates expected indexes on key tables", {
  has_index_loose <- function(table, cols, unique_flag) {
    wins <- create_index_windows()
    if (!length(wins)) return(FALSE)
    # must mention table='name' (single or double quotes)
    wins <- wins[stringr::str_detect(wins, paste0("table\\s*=\\s*[\"']", table, "[\"']"))]
    if (!length(wins)) return(FALSE)
    # must mention all columns somewhere and the unique flag
    any(vapply(wins, function(w) {
      cols_ok <- all(stringr::str_detect(w, paste0("[\"']", cols, "[\"']")))
      uniq_ok <- stringr::str_detect(w, paste0("unique\\s*=\\s*", unique_flag))
      cols_ok && uniq_ok
    }, logical(1)))
  }
  
  expect_true(has_index_loose("pbp_tbl", c("game_id","play_id"), "TRUE"))
  expect_true(has_index_loose("pbp_games_tbl", c("game_id","team"), "TRUE"))
  expect_true(has_index_loose("games_tbl", c("game_id"), "TRUE"))
  expect_true(has_index_loose("weekly_results_tbl", c("game_id","team_id"), "TRUE"))
  expect_true(has_index_loose("team_weekly_tbl", c("season","season_type","week","team","stat_name","stat_type"), "TRUE"))
  expect_true(has_index_loose("player_weekly_tbl", c("season","season_type","week","player_id","stat_name","stat_type"), "TRUE"))
  expect_true(has_index_loose("team_weekly_rankings_tbl", c("season","week","team","stat_name"), "TRUE"))
  expect_true(has_index_loose("team_strength_tbl", c("team","season","week"), "TRUE"))
})

test_that("script includes NGS/defense/contract/id_map tables with expected id_cols", {
  has_index_cols <- function(table, cols) {
    wins <- create_index_windows()
    if (!length(wins)) return(FALSE)
    wins <- wins[stringr::str_detect(wins, paste0("table\\s*=\\s*[\"']", table, "[\"']"))]
    if (!length(wins)) return(FALSE)
    any(vapply(wins, function(w) {
      all(stringr::str_detect(w, paste0("[\"']", cols, "[\"']")))
    }, logical(1)))
  }
  
  expect_true(has_index_cols("nextgen_stats_player_weekly_tbl", c("season","season_type","week","player_gsis_id")))
  expect_true(has_index_cols("def_player_stats_weekly_tbl",     c("player_id","team","season","week")))
  expect_true(has_index_cols("def_team_stats_week_tbl",         c("season","week","team")))
  expect_true(has_index_cols("contracts_qb_tbl",                c("gsis_id","team","year_signed")))
  expect_true(has_index_cols("contracts_position_cap_pct_tbl",  c("position","year_signed","team")))
  expect_true(has_index_cols("id_map_tbl",                      c("gsis_id","espn_id","full_name")))
})

test_that("script includes NGS/defense/contract/id_map tables with expected id_cols", {
  has_index_cols <- function(table, cols) {
    table_pat <- paste0("table\\s*=\\s*[\"']", table, "[\"']")
    cols_inner <- paste0("[\"']", cols, "[\"']")
    cols_pat <- paste0("id_cols\\s*=\\s*c\\s*\\(\\s*", paste0(cols_inner, collapse="\\s*,\\s*"), "\\s*\\)")
    pat <- paste0("(?s)create_index\\s*\\([^)]*", table_pat, "[^)]*", cols_pat)
    stringr::str_detect(txt, pat)
  }
  
  expect_true(has_index_cols("nextgen_stats_player_weekly_tbl", c("season","season_type","week","player_gsis_id")))
  expect_true(has_index_cols("def_player_stats_weekly_tbl",     c("player_id","team","season","week")))
  expect_true(has_index_cols("def_team_stats_week_tbl",         c("season","week","team")))
  expect_true(has_index_cols("contracts_qb_tbl",                c("gsis_id","team","year_signed")))
  expect_true(has_index_cols("contracts_position_cap_pct_tbl",  c("position","year_signed","team")))
  expect_true(has_index_cols("id_map_tbl",                      c("gsis_id","espn_id","full_name")))
})

# --- Next Gen Stats & Defensive tables appear with correct ids (tolerant) -----
test_that("script includes NGS and defense tables with expected id_cols (tolerant)", {
  # Reuse the create_index_windows() helper defined above in this file.
  has_index_cols_flex <- function(table, cols) {
    wins <- create_index_windows()
    if (!length(wins)) return(FALSE)
    # allow single OR double quotes around table=
    wins <- wins[stringr::str_detect(wins, paste0("table\\s*=\\s*['\\\"]", table, "['\\\"]"))]
    if (!length(wins)) return(FALSE)
    # pass if: id_cols = c(...) exists AND all column names appear (any order), any whitespace
    any(vapply(wins, function(w) {
      id_vec_present <- stringr::str_detect(w, "id_cols\\s*=\\s*c\\s*\\(")
      cols_present <- all(vapply(cols, function(cn) {
        stringr::str_detect(w, paste0("['\\\"]", cn, "['\\\"]"))
      }, logical(1)))
      id_vec_present && cols_present
    }, logical(1)))
  }
  
  expect_true(has_index_cols_flex("nextgen_stats_player_weekly_tbl",
                                  c("season","season_type","week","player_gsis_id")))
  expect_true(has_index_cols_flex("def_player_stats_weekly_tbl",
                                  c("player_id","team","season","week")))
  expect_true(has_index_cols_flex("def_team_stats_week_tbl",
                                  c("season","week","team")))
})

# --- Contracts & ID map sanity (tolerant) -------------------------------------
test_that("script includes contracts & id_map with expected keys (tolerant)", {
  has_index_cols_flex <- function(table, cols) {
    wins <- create_index_windows()
    if (!length(wins)) return(FALSE)
    wins <- wins[stringr::str_detect(wins, paste0("table\\s*=\\s*['\\\"]", table, "['\\\"]"))]
    if (!length(wins)) return(FALSE)
    any(vapply(wins, function(w) {
      id_vec_present <- stringr::str_detect(w, "id_cols\\s*=\\s*c\\s*\\(")
      cols_present <- all(vapply(cols, function(cn) {
        stringr::str_detect(w, paste0("['\\\"]", cn, "['\\\"]"))
      }, logical(1)))
      id_vec_present && cols_present
    }, logical(1)))
  }
  
  expect_true(has_index_cols_flex("contracts_qb_tbl",
                                  c("gsis_id","team","year_signed")))
  expect_true(has_index_cols_flex("contracts_position_cap_pct_tbl",
                                  c("position","year_signed","team")))
  expect_true(has_index_cols_flex("id_map_tbl",
                                  c("gsis_id","espn_id","full_name")))
})

