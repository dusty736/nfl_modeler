# tests/testthat/test-step2-contracts-functions.R

suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(tibble)
  library(here)   # <-- robust pathing from project root (nfl_modeler.Rproj)
})

testthat::local_edition(3)

# ---------------------------------------------------------------------------
# Source functions under test (robust to working directory)
# ---------------------------------------------------------------------------
contracts_fn <- here::here("etl", "R", "step2_process", "step2_contracts_process_functions.R")
if (!file.exists(contracts_fn)) {
  stop("Cannot find contracts functions file at: ", contracts_fn,
       "\nCheck that you're running within the project with nfl_modeler.Rproj.")
}
source(contracts_fn)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

expected_contract_cols <- c(
  "player", "position", "team", "is_active", "year_signed", "years",
  "value", "apy", "guaranteed", "apy_cap_pct",
  "inflated_value", "inflated_apy", "inflated_guaranteed",
  "player_page", "otc_id", "gsis_id", "date_of_birth", "height", "weight",
  "college", "draft_year", "draft_round", "draft_overall", "draft_team"
)

# Minimal happy-path raw contracts (mix of positions, some NAs)
raw_contracts_happy <- tibble::tibble(
  player = c("Alpha A", "Bravo B", "Charlie C"),
  position = c("QB", "WR", "QB"),
  team = c("SEA", "SEA", "SF"),
  is_active = c(TRUE, FALSE, TRUE),
  year_signed = c(2020L, 2021L, 2023L),
  years = c(4L, 3L, 2L),
  value = c(100, 30, 50),
  apy = c(25, 10, 25),
  guaranteed = c(80, 20, 40),
  apy_cap_pct = c(0.12, NA_real_, 0.11),
  inflated_value = c(110, 33, 55),
  inflated_apy = c(27.5, 11, 27.5),
  inflated_guaranteed = c(88, 22, 44),
  player_page = c("pA", "pB", "pC"),
  otc_id = c("otcA", "otcB", "otcC"),
  gsis_id = c("00-000001", "00-000002", "00-000003"),
  date_of_birth = c("1995-01-01", "1996-02-02", "1994-03-03"),
  height = c("6-2", "6-0", "6-3"),
  weight = c(220, 200, 225),
  college = c("Stanford", "UW", "Cal"),
  draft_year = c(2017L, 2018L, 2019L),
  draft_round = c(1L, 2L, 1L),
  draft_overall = c(3L, 40L, 10L),
  draft_team = c("SF", "SEA", "SF"),
  extra_col = c("drop_me", "drop_me", "drop_me")
)

# Duplicate QB entries across teams/years to test sequencing and metadata
raw_contracts_qb_dupes <- tibble::tibble(
  player = c("Alpha A", "Alpha A", "Bravo B", "Delta D"),
  position = c("QB", "QB", "QB", "WR"),
  team = c("SEA", "SF", "SEA", "SEA"),
  is_active = c(TRUE, TRUE, TRUE, TRUE),
  year_signed = c(2020L, 2023L, 2024L, 2022L),
  years = c(4L, 2L, NA_integer_, 1L),
  value = c(100, 50, 5, 1),
  apy = c(25, 25, 5, 1),
  guaranteed = c(80, 40, 4, 1),
  apy_cap_pct = c(0.12, 0.11, 0.02, 0.01),
  inflated_value = c(110, 55, 5, 1),
  inflated_apy = c(27.5, 27.5, 5, 1),
  inflated_guaranteed = c(88, 44, 4, 1),
  player_page = c("pA", "pA", "pB", "pD"),
  otc_id = c("otcA1", "otcA2", "otcB1", "otcD"),
  gsis_id = c("00-000001", "00-000001", "00-000002", "00-000004"),
  date_of_birth = c("1995-01-01", "1995-01-01", "1996-02-02", "1992-05-05"),
  height = c("6-2", "6-2", "6-1", "5-11"),
  weight = c(220, 220, 210, 195),
  college = c("Stanford", "Stanford", "UW", "WSU"),
  draft_year = c(2017L, 2017L, 2018L, 2015L),
  draft_round = c(1L, 1L, 2L, 6L),
  draft_overall = c(3L, 3L, 40L, 200L),
  draft_team = c("SF", "SF", "SEA", "SEA")
)

# Empty input with all required columns present (0 rows)
empty_raw_contracts <- {
  cols <- expected_contract_cols
  tibble::as_tibble(setNames(rep(list(logical(0)), length(cols)), cols))
}

# ---------------------------------------------------------------------------
# Tests: clean_contracts_data()
# ---------------------------------------------------------------------------

test_that("clean_contracts_data: schema and types are correct; extras dropped", {
  cleaned <- clean_contracts_data(raw_contracts_happy)
  
  expect_s3_class(cleaned, "tbl_df")
  expect_identical(names(cleaned), expected_contract_cols)
  
  # Types
  expect_type(cleaned$player, "character")
  expect_type(cleaned$position, "character")
  expect_type(cleaned$team, "character")
  expect_type(cleaned$is_active, "logical")
  expect_type(cleaned$year_signed, "integer")
  expect_type(cleaned$years, "integer")
  expect_type(cleaned$value, "double")
  expect_type(cleaned$apy, "double")
  expect_type(cleaned$guaranteed, "double")
  expect_type(cleaned$apy_cap_pct, "double")
  expect_type(cleaned$inflated_value, "double")
  expect_type(cleaned$inflated_apy, "double")
  expect_type(cleaned$inflated_guaranteed, "double")
  expect_type(cleaned$player_page, "character")
  expect_type(cleaned$otc_id, "character")
  expect_type(cleaned$gsis_id, "character")
  expect_type(cleaned$date_of_birth, "character")
  expect_type(cleaned$height, "character")
  expect_type(cleaned$weight, "double")
  expect_type(cleaned$college, "character")
  expect_type(cleaned$draft_year, "integer")
  expect_type(cleaned$draft_round, "integer")
  expect_type(cleaned$draft_overall, "integer")
  expect_type(cleaned$draft_team, "character")
  
  # NA handling preserved
  expect_true(is.na(cleaned$apy_cap_pct[2]))
  
  # Extra input column was dropped
  expect_false("extra_col" %in% names(cleaned))
})

test_that("clean_contracts_data: zero-row input returns zero-row with same schema", {
  cleaned <- clean_contracts_data(empty_raw_contracts)
  expect_s3_class(cleaned, "tbl_df")
  expect_identical(names(cleaned), expected_contract_cols)
  expect_identical(nrow(cleaned), 0L)
})

test_that("clean_contracts_data: missing required column yields informative error", {
  missing_year <- dplyr::select(raw_contracts_happy, -year_signed)
  expect_error(clean_contracts_data(missing_year), regexp = "year_signed")
})

# ---------------------------------------------------------------------------
# Tests: summarise_position_cap_pct()
# ---------------------------------------------------------------------------

test_that("summarise_position_cap_pct: groups and aggregates are correct", {
  cleaned <- clean_contracts_data(raw_contracts_happy)
  
  # Add an all-NA apy_cap_pct group for explicit NA-mean behaviour
  more <- cleaned %>%
    add_row(
      player = "Zeta Z", position = "WR", team = "SEA", is_active = TRUE,
      year_signed = 2022L, years = 1L, value = 1, apy = 1, guaranteed = 1,
      apy_cap_pct = NA_real_, inflated_value = 1, inflated_apy = 1,
      inflated_guaranteed = 1, player_page = "pZ", otc_id = "otcZ",
      gsis_id = "00-000999", date_of_birth = "1999-09-09", height = "6-0",
      weight = 190, college = "Nowhere", draft_year = 2020L,
      draft_round = 7L, draft_overall = 250L, draft_team = "SEA"
    ) %>%
    add_row(
      player = "Eta E", position = "WR", team = "SEA", is_active = TRUE,
      year_signed = 2022L, years = 1L, value = 2, apy = 2, guaranteed = 2,
      apy_cap_pct = NA_real_, inflated_value = 2, inflated_apy = 2,
      inflated_guaranteed = 2, player_page = "pE", otc_id = "otcE",
      gsis_id = "00-000998", date_of_birth = "1998-08-08", height = "6-1",
      weight = 195, college = "Somewhere", draft_year = 2020L,
      draft_round = 6L, draft_overall = 210L, draft_team = "SEA"
    )
  
  out <- summarise_position_cap_pct(more)
  
  # Number of groups equals number of unique (position, year_signed, team)
  n_groups <- nrow(distinct(more, position, year_signed, team))
  expect_equal(nrow(out), n_groups)
  
  # Check one known group (QB, 2020, SEA) from raw_contracts_happy
  g_qb_2020_sea <- out %>% filter(position == "QB", year_signed == 2020L, team == "SEA")
  expect_equal(nrow(g_qb_2020_sea), 1L)
  expect_equal(g_qb_2020_sea$total_apy, 25)      # apy was 25
  expect_equal(g_qb_2020_sea$count, 1L)
  expect_equal(g_qb_2020_sea$avg_apy_cap_pct, 0.12)
  
  # All-NA group: avg_apy_cap_pct should be NA/NaN (is.na covers both)
  g_wr_2022_sea <- out %>% filter(position == "WR", year_signed == 2022L, team == "SEA")
  expect_equal(nrow(g_wr_2022_sea), 1L)
  expect_true(is.na(g_wr_2022_sea$avg_apy_cap_pct))
  # total_apy sums the APY values (2 rows: 1 + 2 = 3)
  expect_equal(g_wr_2022_sea$total_apy, 3)
  expect_equal(g_wr_2022_sea$count, 2L)
  
  # Types
  expect_type(out$avg_apy_cap_pct, "double")
  expect_type(out$total_apy, "double")
  expect_type(out$count, "integer")
})

test_that("summarise_position_cap_pct: zero-row input returns zero-row with expected columns", {
  out <- summarise_position_cap_pct(clean_contracts_data(empty_raw_contracts))
  expect_s3_class(out, "tbl_df")
  expect_identical(nrow(out), 0L)
  expect_setequal(names(out), c("position", "year_signed", "team", "avg_apy_cap_pct", "total_apy", "count"))
})

# ---------------------------------------------------------------------------
# Tests: add_qb_contract_metadata()
# ---------------------------------------------------------------------------

test_that("add_qb_contract_metadata: filters to QBs and adds metadata correctly", {
  cleaned <- clean_contracts_data(raw_contracts_qb_dupes)
  qb_meta <- add_qb_contract_metadata(cleaned)
  
  # Only QBs present
  expect_true(all(qb_meta$position == "QB"))
  
  # Alpha A sequences and derived fields
  alpha <- qb_meta %>% filter(player == "Alpha A") %>% arrange(year_signed)
  expect_equal(alpha$year_signed, c(2020L, 2023L))
  expect_equal(alpha$contract_id, c(1L, 2L))
  # First contract ends at next contract's start (2023)
  expect_equal(alpha$contract_end[1], 2023L)
  # Final contract ends at year_signed + years (2023 + 2 = 2025)
  expect_equal(alpha$contract_end[2], 2025L)
  # Teams played for = 2 (SEA, SF)
  expect_equal(unique(alpha$teams_played_for), 2L)
  
  # Bravo B single-contract, years = NA -> contract_end should be NA
  bravo <- qb_meta %>% filter(player == "Bravo B")
  expect_equal(nrow(bravo), 1L)
  expect_true(is.na(bravo$years))
  expect_true(is.na(bravo$contract_end))
  expect_equal(unique(bravo$teams_played_for), 1L)
})

test_that("add_qb_contract_metadata: zero-QB input returns 0 rows and behaves", {
  # Feed only WR rows from the dupes fixture
  only_wr <- raw_contracts_qb_dupes %>% filter(position != "QB")
  cleaned <- clean_contracts_data(only_wr)
  qb_meta <- add_qb_contract_metadata(cleaned)
  
  expect_s3_class(qb_meta, "tbl_df")
  expect_identical(nrow(qb_meta), 0L)
  # teams_played_for should exist even if empty (mutate on empty tibble)
  expect_true("teams_played_for" %in% names(qb_meta))
})

test_that("add_qb_contract_metadata: idempotent when run twice", {
  cleaned <- clean_contracts_data(raw_contracts_qb_dupes)
  once <- add_qb_contract_metadata(cleaned)
  twice <- add_qb_contract_metadata(once)
  
  # Same rows, same values after a second pass
  key_cols <- c("player", "year_signed", "team")
  once_ord <- once %>% arrange(across(all_of(key_cols)))
  twice_ord <- twice %>% arrange(across(all_of(key_cols)))
  
  cols_to_check <- c(
    expected_contract_cols,
    "contract_start", "contract_end", "contract_id", "teams_played_for"
  )
  cols_to_check <- intersect(cols_to_check, names(once_ord))
  
  expect_equal(once_ord[, cols_to_check], twice_ord[, cols_to_check])
})

