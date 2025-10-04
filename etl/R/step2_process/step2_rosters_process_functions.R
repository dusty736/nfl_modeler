#' Process raw roster data into normalized player-season table
#'
#' This function takes raw roster data and returns a clean, normalized
#' player-season dataset with core attributes, including age as of
#' September 1 of the season year.
#'
#' @param roster_raw A data frame read from `nflreadr::load_rosters()` or the equivalent raw parquet file.
#'
#' @return A tibble with one row per player per season, containing clean metadata ready for database storage.
#' @export
#'
#' @examples
#' processed_rosters <- process_rosters(roster_raw)
process_rosters <- function(roster_raw) {
  require(dplyr)
  require(lubridate)
  require(stringr)
  
  roster_raw %>%
    filter(!is.na(gsis_id)) %>%
    mutate(
      player_id = gsis_id,
      age = if_else(
        !is.na(birth_date),
        floor(interval(start = birth_date, end = ymd(paste0(season, "-09-01"))) / years(1)),
        NA_real_
      )
    ) %>%
    transmute(
      season = season,
      week = week,
      player_id = player_id,
      full_name = full_name,
      first_name = first_name,
      last_name = last_name,
      position = position,
      team = team,
      status = status,
      age = age,
      height = as.integer(height),
      weight = as.integer(weight),
      college = college,
      years_exp = years_exp,
      rookie_year = rookie_year,
      entry_year = entry_year,
      headshot_url = headshot_url,
      esb_id = esb_id
    ) %>%
    arrange(season, player_id)
}

#' Summarize roster attributes by team and season
#'
#' Computes average age, height, weight, and experience per team per season,
#' excluding missing values. Useful for team-level analysis and QA.
#'
#' @param players A processed roster table, typically the output of `process_rosters()`.
#'
#' @return A tibble with one row per team-season combination, containing summary statistics.
#' @export
#'
#' @examples
#' team_roster_summary <- summarize_rosters_by_team_season(processed_rosters)
summarize_rosters_by_team_season <- function(players) {
  require(dplyr)
  
  players %>%
    group_by(season, team) %>%
    summarize(
      n_players = n_distinct(player_id),
      avg_age = mean(age, na.rm = TRUE),
      avg_height = mean(height, na.rm = TRUE),
      avg_weight = mean(weight, na.rm = TRUE),
      avg_exp = mean(years_exp, na.rm = TRUE),
      .groups = "drop"
    )
}

#' Summarize roster attributes by team, season, and position
#'
#' Computes average age, height, weight, and experience per team-position-season group.
#' Excludes missing values from averages and reports count of distinct players per group.
#'
#' @param players A processed roster table, typically the output of `process_rosters()`.
#'
#' @return A tibble with one row per season-team-position combination, containing summary statistics.
#' @export
#'
#' @examples
#' roster_summary <- summarize_rosters_by_team_position(processed_rosters)
summarize_rosters_by_team_position <- function(players) {
  require(dplyr)
  
  players %>%
    filter(!is.na(position)) %>%
    group_by(season, team, position) %>%
    summarize(
      n_players = n_distinct(player_id),
      avg_age = mean(age, na.rm = TRUE),
      avg_height = mean(height, na.rm = TRUE),
      avg_weight = mean(weight, na.rm = TRUE),
      avg_exp = mean(years_exp, na.rm = TRUE),
      .groups = "drop"
    )
}
