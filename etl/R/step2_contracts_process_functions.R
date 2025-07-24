# step2_contracts_process_functions.R

library(dplyr)

#' Clean and normalize contract data
#'
#' @param df Raw contracts data from nflreadr::load_contracts()
#' @return Tibble with cleaned fields
clean_contracts_data <- function(df) {
  df %>%
    transmute(
      player = as.character(player),
      position = as.character(position),
      team = as.character(team),
      is_active = as.logical(is_active),
      year_signed = as.integer(year_signed),
      years = as.integer(years),
      value = as.numeric(value),
      apy = as.numeric(apy),
      guaranteed = as.numeric(guaranteed),
      apy_cap_pct = as.numeric(apy_cap_pct),
      inflated_value = as.numeric(inflated_value),
      inflated_apy = as.numeric(inflated_apy),
      inflated_guaranteed = as.numeric(inflated_guaranteed),
      player_page = as.character(player_page),
      otc_id = as.character(otc_id),
      gsis_id = as.character(gsis_id),
      date_of_birth = as.character(date_of_birth),
      height = as.character(height),
      weight = as.numeric(weight),
      college = as.character(college),
      draft_year = as.integer(draft_year),
      draft_round = as.integer(draft_round),
      draft_overall = as.integer(draft_overall),
      draft_team = as.character(draft_team)
    )
}

#' Summarize cap percentage by position, year, and team
#'
#' @param df Cleaned contracts data
#' @return Tibble with apy_cap_pct by position-year-team
summarise_position_cap_pct <- function(df) {
  df %>%
    group_by(position, year_signed, team) %>%
    summarise(
      avg_apy_cap_pct = mean(apy_cap_pct, na.rm = TRUE),
      total_apy = sum(apy, na.rm = TRUE),
      count = n(),
      .groups = "drop"
    )
}

#' Add quarterback-specific contract details
#'
#' @param df Cleaned contracts data
#' @return Tibble of quarterback contracts with metadata
add_qb_contract_metadata <- function(df) {
  df %>%
    filter(position == "QB") %>%
    arrange(player, year_signed) %>%
    group_by(player) %>%
    group_modify(~ {
      .x %>%
        mutate(
          contract_start = year_signed,
          contract_end = lead(year_signed, default = last(year_signed + years)),
          contract_id = row_number()
        )
    }) %>%
    ungroup() %>%
    group_by(player) %>%
    mutate(
      teams_played_for = n_distinct(team)
    ) %>%
    ungroup()
}
