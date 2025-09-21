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
  # pre-clean fields
  x <- injuries_raw %>%
    dplyr::mutate(
      primary_injury_raw   = dplyr::coalesce(report_primary_injury, practice_primary_injury),
      secondary_injury_raw = !is.na(dplyr::coalesce(report_secondary_injury, practice_secondary_injury)),
      did_not_practice_raw = stringr::str_detect(dplyr::coalesce(practice_status, ""), "Did Not Participate"),
      report_status_raw    = report_status,
      practice_status_raw  = practice_status
    )
  
  # build a robust player key for rows with missing gsis_id
  x <- x %>%
    dplyr::mutate(
      full_name_co = dplyr::coalesce(full_name, "UNKNOWN"),
      position_co  = dplyr::coalesce(position, "UNK"),
      gsis_id_norm = dplyr::if_else(!is.na(gsis_id) & nzchar(gsis_id),
                                    gsis_id,
                                    paste0(
                                      "UNK_",
                                      toupper(gsub("[^A-Z0-9]+", "_", full_name_co)),
                                      "_",
                                      toupper(gsub("[^A-Z0-9]+", "_", position_co))
                                    ))
    )
  
  # reduce to one row per season-week-team-player
  out <- x %>%
    dplyr::group_by(season, week, team, gsis_id_norm) %>%
    dplyr::summarise(
      full_name = {z <- stats::na.omit(full_name_co); if (length(z)) dplyr::first(z) else NA_character_},
      position  = {z <- stats::na.omit(position_co);  if (length(z)) dplyr::first(z) else NA_character_},
      
      report_status   = {z <- stats::na.omit(report_status_raw);   if (length(z)) dplyr::last(z) else NA_character_},
      practice_status = {z <- stats::na.omit(practice_status_raw); if (length(z)) dplyr::last(z) else NA_character_},
      
      primary_injury   = {z <- stats::na.omit(primary_injury_raw); if (length(z)) dplyr::first(z) else NA_character_},
      secondary_injury = any(secondary_injury_raw %in% TRUE, na.rm = TRUE),
      did_not_practice = any(did_not_practice_raw %in% TRUE,  na.rm = TRUE),
      .groups = "drop"
    ) %>%
    # final flags derived after reduction
    dplyr::mutate(
      injury_status   = report_status,
      injury_reported = (!is.na(primary_injury)) | secondary_injury,
      gsis_id         = gsis_id_norm
    ) %>%
    dplyr::select(
      season, week, team, gsis_id, full_name, position,
      report_status, injury_reported, did_not_practice, injury_status,
      practice_status, primary_injury, secondary_injury
    )
  
  # sanity: enforce uniqueness on the test key
  out %>%
    dplyr::arrange(season, week, team, gsis_id) %>%
    dplyr::distinct(season, week, team, gsis_id, .keep_all = TRUE)
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
    dplyr::filter(!is.na(position), injury_reported, position != "") %>%
    dplyr::group_by(season, week, team, position) %>%
    dplyr::summarise(position_injuries = dplyr::n(), .groups = "drop") %>%
    dplyr::arrange(season, team, position, week) %>%
    dplyr::group_by(season, team, position) %>%
    dplyr::mutate(cumulative_position_injuries = cumsum(position_injuries)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(season, week, team, position)
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
    dplyr::group_by(season, team, week) %>%
    dplyr::summarise(weekly_injuries = sum(position_injuries), .groups = "drop_last") %>%
    dplyr::arrange(season, team, week) %>%
    dplyr::group_by(season, team) %>%
    dplyr::mutate(cumulative_injuries = cumsum(weekly_injuries)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(season, week, team)
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
    dplyr::group_by(season, team) %>%
    dplyr::summarise(season_injuries = sum(weekly_injuries), .groups = "drop")
}
