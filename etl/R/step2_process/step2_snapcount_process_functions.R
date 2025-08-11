#' Summarize weekly snap counts to season level
#'
#' @description
#' Aggregates weekly snap count data to one row per player-team-season.
#' Totals are summed across games. Percentage columns (`*_pct`) are
#' simple arithmetic means across games where that phase appears.
#' If you later have team-level snap totals, switch to a weighted mean.
#'
#' @param snap_df A data frame like `nflreadr::load_snap_counts()` output,
#'   with columns: `game_id`, `season`, `game_type`, `week`, `player`,
#'   `pfr_player_id`, `position`, `team`, `opponent`,
#'   `offense_snaps`, `offense_pct`, `defense_snaps`, `defense_pct`,
#'   `st_snaps`, `st_pct`.
#' @param game_types Character vector of game types to include (e.g., c("REG","POST")).
#'   Default includes both Regular and Postseason.
#'
#' @return A tibble with one row per (season, team, player, pfr_player_id, position)
#'   containing summed snaps, mean percentages, and game counts.
#'
#' @examples
#' # season-level from weekly:
#' # season_snap <- summarize_snapcounts_season(snapcount, game_types = c("REG","POST"))
#'
#' @export
summarize_snapcounts_season <- function(snap_df) {
  snap_df %>%
    dplyr::mutate(
      offense_snaps = dplyr::coalesce(.data$offense_snaps, 0),
      defense_snaps = dplyr::coalesce(.data$defense_snaps, 0),
      st_snaps      = dplyr::coalesce(.data$st_snaps, 0),
      offense_pct   = dplyr::coalesce(.data$offense_pct, 0),
      defense_pct   = dplyr::coalesce(.data$defense_pct, 0),
      st_pct        = dplyr::coalesce(.data$st_pct, 0)
    ) %>%
    dplyr::group_by(
      .data$season, .data$team, .data$player, .data$pfr_player_id, .data$position
    ) %>%
    dplyr::summarise(
      games_played   = dplyr::n_distinct(.data$game_id[
        .data$offense_snaps + .data$defense_snaps + .data$st_snaps > 0
      ]),
      offense_games  = dplyr::n_distinct(.data$game_id[.data$offense_snaps > 0]),
      defense_games  = dplyr::n_distinct(.data$game_id[.data$defense_snaps > 0]),
      st_games       = dplyr::n_distinct(.data$game_id[.data$st_snaps > 0]),
      
      offense_snaps  = sum(.data$offense_snaps, na.rm = TRUE),
      defense_snaps  = sum(.data$defense_snaps, na.rm = TRUE),
      st_snaps       = sum(.data$st_snaps, na.rm = TRUE),
      
      offense_pct_mean = mean(.data$offense_pct, na.rm = TRUE),
      defense_pct_mean = mean(.data$defense_pct, na.rm = TRUE),
      st_pct_mean      = mean(.data$st_pct, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(.data$season, .data$team, .data$player) %>% 
    mutate_if(is.numeric, round, 3)
}

#' Summarize weekly snap counts to career level
#'
#' @description
#' Aggregates weekly snap count data to one row per player across all seasons.
#' Totals are summed across all games/seasons. Percentage columns are
#' simple arithmetic means across games. Adds helper fields like
#' `first_season`, `last_season`, and `teams_played_for`.
#'
#' @param snap_df A data frame like `nflreadr::load_snap_counts()` output.
#' @param game_types Character vector of game types to include (e.g., c("REG","POST")).
#'   Default includes both Regular and Postseason.
#'
#' @return A tibble with one row per (player, pfr_player_id, position_primary-ish)
#'   across all seasons, including summed snaps, mean percentages, counts of games,
#'   distinct seasons, and distinct teams.
#'
#' @examples
#' # career-level from weekly:
#' # career_snap <- summarize_snapcounts_career(snapcount)
#'
#' @export
summarize_snapcounts_career <- function(snap_df) {
  snap_df %>%
    dplyr::mutate(
      offense_snaps = dplyr::coalesce(.data$offense_snaps, 0),
      defense_snaps = dplyr::coalesce(.data$defense_snaps, 0),
      st_snaps      = dplyr::coalesce(.data$st_snaps, 0),
      offense_pct   = dplyr::coalesce(.data$offense_pct, 0),
      defense_pct   = dplyr::coalesce(.data$defense_pct, 0),
      st_pct        = dplyr::coalesce(.data$st_pct, 0)
    ) %>%
    dplyr::group_by(.data$player, .data$pfr_player_id) %>%
    dplyr::summarise(
      first_season   = min(.data$season, na.rm = TRUE),
      last_season    = max(.data$season, na.rm = TRUE),
      seasons_played = dplyr::n_distinct(.data$season),
      teams_played_for = dplyr::n_distinct(.data$team),
      
      games_played   = dplyr::n_distinct(.data$game_id[
        .data$offense_snaps + .data$defense_snaps + .data$st_snaps > 0
      ]),
      offense_games  = dplyr::n_distinct(.data$game_id[.data$offense_snaps > 0]),
      defense_games  = dplyr::n_distinct(.data$game_id[.data$defense_snaps > 0]),
      st_games       = dplyr::n_distinct(.data$game_id[.data$st_snaps > 0]),
      
      offense_snaps  = sum(.data$offense_snaps, na.rm = TRUE),
      defense_snaps  = sum(.data$defense_snaps, na.rm = TRUE),
      st_snaps       = sum(.data$st_snaps, na.rm = TRUE),
      
      offense_pct_mean = mean(.data$offense_pct, na.rm = TRUE),
      defense_pct_mean = mean(.data$defense_pct, na.rm = TRUE),
      st_pct_mean      = mean(.data$st_pct, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(.data$player, .data$first_season) %>% 
    mutate_if(is.numeric, round, 3)
}
