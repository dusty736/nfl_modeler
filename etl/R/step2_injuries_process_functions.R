#' Clean and Engineer Injury Table for ETL
#'
#' This function processes raw NFL injury report data to produce a clean,
#' PostgreSQL-ready table. It combines report and practice injury fields,
#' flags injury presence and participation status, and engineers summary
#' variables such as how often each player has appeared with injury data.
#'
#' @param injuries_raw A data frame or tibble containing raw injury data from `nflreadr::load_injuries()`.
#'
#' @return A tibble with one row per player per team-week containing injury fields,
#' cleaned text fields, logical indicators, and engineered per-player injury counts.
#'
#' @examples
#' \dontrun{
#' injuries_raw <- nflreadr::load_injuries()
#' injuries_clean <- process_injuries(injuries_raw)
#' }
#'
#' @import dplyr
#' @importFrom stringr str_detect
#' @export
process_injuries <- function(injuries_raw) {
  injuries_raw %>%
    dplyr::mutate(
      primary_injury   = dplyr::coalesce(report_primary_injury, practice_primary_injury),
      secondary_injury = !is.na(dplyr::coalesce(report_secondary_injury, practice_secondary_injury)),
      injury_reported  = !is.na(primary_injury) | !is.na(secondary_injury),
      did_not_practice = stringr::str_detect(practice_status %||% "", "Did Not Participate"),
      injury_status    = report_status %||% NA_character_,
      practice_status  = practice_status %||% NA_character_
    ) %>% 
    dplyr::select(
      season, week, team, gsis_id, full_name, position,
      report_status, injury_reported, did_not_practice, injury_status, 
      practice_status, primary_injury, secondary_injury
    )
}

#' Weekly Injury Summary by Team and Position
#'
#' Aggregates the number of injured players by team, week, season, and position.
#' Adds a running count of injuries over the course of the season.
#'
#' @param injuries A data frame containing cleaned injury data with at least
#'   the columns: `season`, `week`, `team`, `position`, and `injury_reported`.
#'
#' @return A tibble with one row per team-week-position containing the number of players
#'   listed with an injury and a running cumulative total across the season.
#'
#' @examples
#' \dontrun{
#' cleaned <- process_injuries(injuries_raw)
#' summary <- weekly_injury_summary(cleaned)
#' }
#'
#' @import dplyr
#' @export
position_injury_summary <- function(injuries) {
  injuries %>%
    dplyr::filter(!is.na(position), injury_reported, position != '') %>%
    dplyr::group_by(season, week, team, position) %>%
    dplyr::summarize(position_injuries = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(season, team, position, week) %>%
    dplyr::group_by(season, team, position) %>%
    dplyr::mutate(cumulative_position_injuries = cumsum(position_injuries)) %>%
    dplyr::ungroup() %>% 
    arrange(season, week, team, position)
}

#' Team-Level Weekly and Cumulative Injury Summary
#'
#' Aggregates weekly and cumulative injuries per team across positions.
#' Assumes the input data includes pre-computed `position_injuries` and `cumulative_position_injuries`
#' from a positional injury summary (e.g., output of `weekly_injury_summary()`).
#'
#' @param df A data frame containing columns `season`, `team`, `week`,
#'   `position`, `position_injuries`, and `cumulative_position_injuries`.
#'
#' @return A tibble with one row per team-week summarizing the total weekly
#'   injuries and cumulative season-to-date injuries.
#'
#' @examples
#' \dontrun{
#' weekly_summary <- weekly_injury_summary(cleaned)
#' team_summary <- team_injury_summary(weekly_summary)
#' }
#'
#' @import dplyr
#' @export
team_injury_summary <- function(df) {
  df %>%
    group_by(season, team, week) %>% 
    mutate(weekly_injuries = sum(position_injuries),
           cumulative_injuries = sum(cumulative_position_injuries)) %>% 
    dplyr::ungroup() %>% 
    arrange(season, week, team, position) %>% 
    dplyr::select(season, week, team, weekly_injuries, cumulative_injuries) %>% 
    distinct()
}


#' Season-Long Injury Totals by Team
#'
#' Aggregates total injuries per team over the course of a season.
#' Assumes input contains `weekly_injuries` from `team_injury_summary()`.
#'
#' @param df A data frame with at least `season`, `team`, and `weekly_injuries`.
#'
#' @return A tibble with one row per team-season containing the total number of injuries.
#'
#' @examples
#' \dontrun{
#' team_summary <- team_injury_summary(weekly_summary)
#' season_totals <- season_injury_summary(team_summary)
#' }
#'
#' @import dplyr
#' @export
season_injury_summary <- function(df) {
  df %>%
    group_by(season, team) %>% 
    summarize(season_injuries = sum(weekly_injuries)) %>% 
    dplyr::ungroup()
}



