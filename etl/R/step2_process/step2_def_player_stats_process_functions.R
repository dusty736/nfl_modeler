#' Clean and select columns from raw defensive player stats (game level)
#'
#' Filters to defensive players only (DL, LB, DB) and returns cleaned
#' player-week defensive stats.
#'
#' @param def_player_stats_raw A data frame from `nflreadr::load_defensive_stats()` or the equivalent raw parquet.
#'
#' @return A tibble with one row per defensive player per game week.
#' @export
#'
#' @examples
#' def_stats_clean <- process_defensive_player_stats(def_player_stats_raw)
process_defensive_player_stats <- function(def_player_stats_raw) {
  require(dplyr)
  
  def_player_stats_raw %>%
    filter(position_group %in% c("DL", "LB", "DB")) %>%
    transmute(
      season,
      week,
      season_type,
      player_id,
      player_name = player_display_name,
      team,
      position,
      position_group,
      def_tackles = as.integer(def_tackles),
      def_tackles_solo = as.integer(def_tackles_solo),
      def_tackle_assists = as.integer(def_tackle_assists),
      def_tackles_for_loss = as.integer(def_tackles_for_loss),
      def_tackles_for_loss_yards = as.numeric(def_tackles_for_loss_yards),
      def_fumbles_forced = as.integer(def_fumbles_forced),
      def_sacks = as.numeric(def_sacks),
      def_sack_yards = as.numeric(def_sack_yards),
      def_qb_hits = as.numeric(def_qb_hits),
      def_interceptions = as.numeric(def_interceptions),
      def_interception_yards = as.numeric(def_interception_yards),
      def_pass_defended = as.numeric(def_pass_defended),
      def_tds = as.numeric(def_tds),
      def_fumbles = as.numeric(def_fumbles),
      def_fumble_recovery_own = as.numeric(def_fumble_recovery_own),
      def_fumble_recovery_yards_own = as.numeric(def_fumble_recovery_yards_own),
      def_fumble_recovery_opp = as.numeric(def_fumble_recovery_opp),
      def_fumble_recovery_yards_opp = as.numeric(def_fumble_recovery_yards_opp),
      def_safety = as.integer(def_safety),
      def_penalty = as.numeric(def_penalty),
      def_penalty_yards = as.numeric(def_penalty_yards)
    ) %>% 
  arrange(season, player_id, week) %>%
    group_by(season, player_id) %>%
    mutate(
      cumulative_def_tackles = cumsum(replace_na(def_tackles, 0)),
      cumulative_def_tackles_solo = cumsum(replace_na(def_tackles_solo, 0)),
      cumulative_def_tackle_assists = cumsum(replace_na(def_tackle_assists, 0)),
      cumulative_def_tackles_for_loss = cumsum(replace_na(def_tackles_for_loss, 0)),
      cumulative_def_tackles_for_loss_yards = cumsum(replace_na(def_tackles_for_loss_yards, 0)),
      cumulative_def_fumbles_forced = cumsum(replace_na(def_fumbles_forced, 0)),
      cumulative_def_sacks = cumsum(replace_na(def_sacks, 0)),
      cumulative_def_sack_yards = cumsum(replace_na(def_sack_yards, 0)),
      cumulative_def_qb_hits = cumsum(replace_na(def_qb_hits, 0)),
      cumulative_def_interceptions = cumsum(replace_na(def_interceptions, 0)),
      cumulative_def_interception_yards = cumsum(replace_na(def_interception_yards, 0)),
      cumulative_def_pass_defended = cumsum(replace_na(def_pass_defended, 0)),
      cumulative_def_tds = cumsum(replace_na(def_tds, 0)),
      cumulative_def_fumbles = cumsum(replace_na(def_fumbles, 0)),
      cumulative_def_fumble_recovery_own = cumsum(replace_na(def_fumble_recovery_own, 0)),
      cumulative_def_fumble_recovery_yards_own = cumsum(replace_na(def_fumble_recovery_yards_own, 0)),
      cumulative_def_fumble_recovery_opp = cumsum(replace_na(def_fumble_recovery_opp, 0)),
      cumulative_def_fumble_recovery_yards_opp = cumsum(replace_na(def_fumble_recovery_yards_opp, 0)),
      cumulative_def_safety = cumsum(replace_na(def_safety, 0)),
      cumulative_def_penalty = cumsum(replace_na(def_penalty, 0)),
      cumulative_def_penalty_yards = cumsum(replace_na(def_penalty_yards, 0))
    ) %>%
    ungroup()
}

#' Summarize defensive stats by player and season
#'
#' Aggregates defensive player stats across the season to create a season-level
#' summary for each player. All numeric statistics are summed, and player metadata
#' (team, position, etc.) is taken from the most recent game of the season.
#'
#' @param def_stats_clean A cleaned and possibly cumulative player-week defensive stats table.
#'
#' @return A tibble with one row per player per season and summed defensive statistics.
#' @export
#'
#' @examples
#' def_stats_season <- summarize_defensive_player_stats_by_season(def_stats_clean)
summarize_defensive_player_stats_by_season <- function(def_stats_clean) {
  require(dplyr)
  
  def_stats_clean %>%
    group_by(season, player_id) %>%
    summarize(
      player_name = dplyr::last(na.omit(player_name)),
      team = dplyr::last(na.omit(team)),
      position = dplyr::last(na.omit(position)),
      position_group = dplyr::last(na.omit(position_group)),
      games_played = n_distinct(week),
      def_tackles = sum(def_tackles, na.rm = TRUE),
      def_tackles_solo = sum(def_tackles_solo, na.rm = TRUE),
      def_tackle_assists = sum(def_tackle_assists, na.rm = TRUE),
      def_tackles_for_loss = sum(def_tackles_for_loss, na.rm = TRUE),
      def_tackles_for_loss_yards = sum(def_tackles_for_loss_yards, na.rm = TRUE),
      def_fumbles_forced = sum(def_fumbles_forced, na.rm = TRUE),
      def_sacks = sum(def_sacks, na.rm = TRUE),
      def_sack_yards = sum(def_sack_yards, na.rm = TRUE),
      def_qb_hits = sum(def_qb_hits, na.rm = TRUE),
      def_interceptions = sum(def_interceptions, na.rm = TRUE),
      def_interception_yards = sum(def_interception_yards, na.rm = TRUE),
      def_pass_defended = sum(def_pass_defended, na.rm = TRUE),
      def_tds = sum(def_tds, na.rm = TRUE),
      def_fumbles = sum(def_fumbles, na.rm = TRUE),
      def_fumble_recovery_own = sum(def_fumble_recovery_own, na.rm = TRUE),
      def_fumble_recovery_yards_own = sum(def_fumble_recovery_yards_own, na.rm = TRUE),
      def_fumble_recovery_opp = sum(def_fumble_recovery_opp, na.rm = TRUE),
      def_fumble_recovery_yards_opp = sum(def_fumble_recovery_yards_opp, na.rm = TRUE),
      def_safety = sum(def_safety, na.rm = TRUE),
      def_penalty = sum(def_penalty, na.rm = TRUE),
      def_penalty_yards = sum(def_penalty_yards, na.rm = TRUE),
      .groups = "drop"
    )
}

#' Summarize team defensive stats by season
#'
#' Aggregates all defensive player stats by team and season,
#' summing numeric performance fields and counting distinct players.
#'
#' @param def_stats_clean A cleaned defensive stats table, typically player-week level.
#'
#' @return A tibble with one row per team per season, containing total defensive output.
#' @export
#'
#' @examples
#' team_def_summary <- summarize_defensive_stats_by_team_season(def_stats_clean)
summarize_defensive_stats_by_team_season <- function(def_stats_clean) {
  require(dplyr)
  
  def_stats_clean %>%
    group_by(season, team) %>%
    summarize(
      n_players = n_distinct(player_id),
      def_tackles = sum(def_tackles, na.rm = TRUE),
      def_tackles_solo = sum(def_tackles_solo, na.rm = TRUE),
      def_tackle_assists = sum(def_tackle_assists, na.rm = TRUE),
      def_tackles_for_loss = sum(def_tackles_for_loss, na.rm = TRUE),
      def_tackles_for_loss_yards = sum(def_tackles_for_loss_yards, na.rm = TRUE),
      def_fumbles_forced = sum(def_fumbles_forced, na.rm = TRUE),
      def_sacks = sum(def_sacks, na.rm = TRUE),
      def_sack_yards = sum(def_sack_yards, na.rm = TRUE),
      def_qb_hits = sum(def_qb_hits, na.rm = TRUE),
      def_interceptions = sum(def_interceptions, na.rm = TRUE),
      def_interception_yards = sum(def_interception_yards, na.rm = TRUE),
      def_pass_defended = sum(def_pass_defended, na.rm = TRUE),
      def_tds = sum(def_tds, na.rm = TRUE),
      def_fumbles = sum(def_fumbles, na.rm = TRUE),
      def_fumble_recovery_own = sum(def_fumble_recovery_own, na.rm = TRUE),
      def_fumble_recovery_yards_own = sum(def_fumble_recovery_yards_own, na.rm = TRUE),
      def_fumble_recovery_opp = sum(def_fumble_recovery_opp, na.rm = TRUE),
      def_fumble_recovery_yards_opp = sum(def_fumble_recovery_yards_opp, na.rm = TRUE),
      def_safety = sum(def_safety, na.rm = TRUE),
      def_penalty = sum(def_penalty, na.rm = TRUE),
      def_penalty_yards = sum(def_penalty_yards, na.rm = TRUE),
      .groups = "drop"
    )
}

#' Summarize team defensive stats by week
#'
#' Aggregates all defensive player stats by team and week,
#' summing numeric performance fields and counting distinct players.
#'
#' @param def_stats_clean A cleaned defensive stats table, typically player-week level.
#'
#' @return A tibble with one row per team per week, containing total defensive output.
#' @export
#'
#' @examples
#' team_def_summary <- summarize_defensive_stats_by_team_season(def_stats_clean)
summarize_defensive_stats_by_team_weekly <- function(def_stats_clean) {
  require(dplyr)
  
  def_stats_clean %>%
    group_by(season, week, team) %>%
    summarize(
      n_players = n_distinct(player_id),
      def_tackles = sum(def_tackles, na.rm = TRUE),
      def_tackles_solo = sum(def_tackles_solo, na.rm = TRUE),
      def_tackle_assists = sum(def_tackle_assists, na.rm = TRUE),
      def_tackles_for_loss = sum(def_tackles_for_loss, na.rm = TRUE),
      def_tackles_for_loss_yards = sum(def_tackles_for_loss_yards, na.rm = TRUE),
      def_fumbles_forced = sum(def_fumbles_forced, na.rm = TRUE),
      def_sacks = sum(def_sacks, na.rm = TRUE),
      def_sack_yards = sum(def_sack_yards, na.rm = TRUE),
      def_qb_hits = sum(def_qb_hits, na.rm = TRUE),
      def_interceptions = sum(def_interceptions, na.rm = TRUE),
      def_interception_yards = sum(def_interception_yards, na.rm = TRUE),
      def_pass_defended = sum(def_pass_defended, na.rm = TRUE),
      def_tds = sum(def_tds, na.rm = TRUE),
      def_fumbles = sum(def_fumbles, na.rm = TRUE),
      def_fumble_recovery_own = sum(def_fumble_recovery_own, na.rm = TRUE),
      def_fumble_recovery_yards_own = sum(def_fumble_recovery_yards_own, na.rm = TRUE),
      def_fumble_recovery_opp = sum(def_fumble_recovery_opp, na.rm = TRUE),
      def_fumble_recovery_yards_opp = sum(def_fumble_recovery_yards_opp, na.rm = TRUE),
      def_safety = sum(def_safety, na.rm = TRUE),
      def_penalty = sum(def_penalty, na.rm = TRUE),
      def_penalty_yards = sum(def_penalty_yards, na.rm = TRUE),
      .groups = "drop"
    )
}

#' Summarize career defensive stats per player
#'
#' Aggregates all available defensive stats across all seasons for each player.
#' Metadata like name, position, and team are taken from the latest appearance.
#'
#' @param def_stats_clean A cleaned defensive stats table, typically player-week level.
#'
#' @return A tibble with one row per player, representing total career defensive stats.
#' @export
#'
#' @examples
#' def_career_stats <- summarize_defensive_stats_by_player(def_stats_clean)
summarize_defensive_stats_by_player <- function(def_stats_clean) {
  require(dplyr)
  
  def_stats_clean %>%
    group_by(player_id) %>%
    summarize(
      player_name = dplyr::last(na.omit(player_name)),
      position = dplyr::last(na.omit(position)),
      position_group = dplyr::last(na.omit(position_group)),
      last_team = dplyr::last(na.omit(team)),
      seasons_played = n_distinct(season),
      games_played = n_distinct(paste0(season, "_", week)),
      def_tackles = sum(def_tackles, na.rm = TRUE),
      def_tackles_solo = sum(def_tackles_solo, na.rm = TRUE),
      def_tackle_assists = sum(def_tackle_assists, na.rm = TRUE),
      def_tackles_for_loss = sum(def_tackles_for_loss, na.rm = TRUE),
      def_tackles_for_loss_yards = sum(def_tackles_for_loss_yards, na.rm = TRUE),
      def_fumbles_forced = sum(def_fumbles_forced, na.rm = TRUE),
      def_sacks = sum(def_sacks, na.rm = TRUE),
      def_sack_yards = sum(def_sack_yards, na.rm = TRUE),
      def_qb_hits = sum(def_qb_hits, na.rm = TRUE),
      def_interceptions = sum(def_interceptions, na.rm = TRUE),
      def_interception_yards = sum(def_interception_yards, na.rm = TRUE),
      def_pass_defended = sum(def_pass_defended, na.rm = TRUE),
      def_tds = sum(def_tds, na.rm = TRUE),
      def_fumbles = sum(def_fumbles, na.rm = TRUE),
      def_fumble_recovery_own = sum(def_fumble_recovery_own, na.rm = TRUE),
      def_fumble_recovery_yards_own = sum(def_fumble_recovery_yards_own, na.rm = TRUE),
      def_fumble_recovery_opp = sum(def_fumble_recovery_opp, na.rm = TRUE),
      def_fumble_recovery_yards_opp = sum(def_fumble_recovery_yards_opp, na.rm = TRUE),
      def_safety = sum(def_safety, na.rm = TRUE),
      def_penalty = sum(def_penalty, na.rm = TRUE),
      def_penalty_yards = sum(def_penalty_yards, na.rm = TRUE),
      .groups = "drop"
    )
}
