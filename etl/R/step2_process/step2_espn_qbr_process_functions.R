#' Prepare ESPN QBR season totals
#'
#' @description
#' Filters ESPN QBR data to season-level rows (game_week == "Season Total"),
#' keeps essential fields, and normalizes season_type.
#'
#' @param df A data frame like `nflreadr::load_espn_qbr(seasons=TRUE)`.
#'   Expected cols include: season, season_type, game_week, team_abb, team,
#'   player_id, name_display, qbr_total, qbr_raw, qb_plays, pts_added,
#'   epa_total, pass, run, sack, exp_sack, penalty, qualified.
#'
#' @return A tibble with one row per player-season-type team season total.
#' @export
prepare_espn_qbr_season_totals <- function(df) {
  df %>%
    dplyr::filter(.data$game_week == "Season Total") %>%
    dplyr::mutate(
      season_type = dplyr::case_when(
        .data$season_type %in% c("Regular", "REG") ~ "Regular",
        .data$season_type %in% c("Playoffs", "POST") ~ "Playoffs",
        TRUE ~ .data$season_type
      ),
      qb_plays  = dplyr::coalesce(.data$qb_plays, 0),
      qbr_total = dplyr::coalesce(.data$qbr_total, NA_real_),
      qbr_raw   = dplyr::coalesce(.data$qbr_raw, NA_real_),
      pts_added = dplyr::coalesce(.data$pts_added, 0),
      epa_total = dplyr::coalesce(.data$epa_total, 0),
      pass      = dplyr::coalesce(.data$pass, 0),
      run       = dplyr::coalesce(.data$run, 0),
      sack      = dplyr::coalesce(.data$sack, 0),
      exp_sack  = dplyr::coalesce(.data$exp_sack, 0),
      penalty   = dplyr::coalesce(.data$penalty, 0)
    ) %>%
    dplyr::select(
      season, season_type, team_abb, team,
      player_id, name_display,
      qbr_total, qbr_raw, qb_plays,
      pts_added, epa_total, pass, run, sack, exp_sack, penalty,
      qualified
    )
}

#' Career-level ESPN QBR aggregated by season type
#'
#' @description
#' Aggregates season totals across all seasons to one row per player_id
#' and season_type (Regular vs Playoffs).
#' QBR metrics are aggregated correctly using **play-weighted means**:
#'   qbr_total_w = sum(qbr_total * qb_plays) / sum(qb_plays)
#'   qbr_raw_w   = sum(qbr_raw   * qb_plays) / sum(qb_plays)
#' Additive metrics (pts_added, epa_total, pass, run, sack, exp_sack, penalty)
#' are summed. Also returns first/last season and counts.
#'
#' @param season_totals Output of [prepare_espn_qbr_season_totals()].
#'
#' @return A tibble with one row per (player_id, season_type) including:
#'   name_display, seasons_played, first_season, last_season, teams_played_for,
#'   qb_plays, qbr_total_w, qbr_raw_w, pts_added, epa_total, pass, run, sack,
#'   exp_sack, penalty.
#'
#' @examples
#' # season_totals <- prepare_espn_qbr_season_totals(espn_qbr_raw)
#' # qbr_career_by_st <- summarize_espn_qbr_career_by_season_type(season_totals)
#'
#' @export
summarize_espn_qbr_career_by_season_type <- function(season_totals) {
  season_totals %>%
    dplyr::group_by(.data$player_id, .data$name_display, .data$season_type) %>%
    dplyr::summarise(
      first_season     = min(.data$season, na.rm = TRUE),
      last_season      = max(.data$season, na.rm = TRUE),
      seasons_played   = dplyr::n_distinct(.data$season),
      teams_played_for = dplyr::n_distinct(.data$team_abb),
      
      qb_plays   = sum(.data$qb_plays, na.rm = TRUE),
      # Weighted QBRs (guard against zero plays)
      qbr_total_w = dplyr::if_else(
        qb_plays > 0,
        sum(.data$qbr_total * .data$qb_plays, na.rm = TRUE) / qb_plays,
        NA_real_
      ),
      qbr_raw_w = dplyr::if_else(
        qb_plays > 0,
        sum(.data$qbr_raw * .data$qb_plays, na.rm = TRUE) / qb_plays,
        NA_real_
      ),
      
      pts_added = sum(.data$pts_added, na.rm = TRUE),
      epa = sum(.data$epa_total, na.rm = TRUE),
      pass      = sum(.data$pass, na.rm = TRUE),
      run       = sum(.data$run, na.rm = TRUE),
      sack      = sum(.data$sack, na.rm = TRUE),
      exp_sack  = sum(.data$exp_sack, na.rm = TRUE),
      penalty   = sum(.data$penalty, na.rm = TRUE),
      
      qualified_seasons = sum(dplyr::coalesce(.data$qualified, FALSE)),
      .groups = "drop"
    ) %>%
    dplyr::arrange(.data$name_display, .data$season_type)
}

#' Convenience wrapper: from raw to career-by-season-type
#'
#' @description
#' Runs preparation and aggregation in one call.
#'
#' @param df Raw ESPN QBR data.
#' @return Tibble as in [summarize_espn_qbr_career_by_season_type()].
#' @export
espn_qbr_career_by_season_type <- function(df) {
  df %>%
    prepare_espn_qbr_season_totals() %>%
    summarize_espn_qbr_career_by_season_type() %>% 
    mutate_if(is.numeric, round, 3)
}
