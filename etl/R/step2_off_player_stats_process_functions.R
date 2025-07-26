#' Process Weekly Quarterback Stats with Cumulative Season Totals
#'
#' Filters and cleans offensive player stats for quarterbacks (QBs), retaining
#' relevant passing, rushing, EPA, and fantasy fields. Adds cumulative season-to-date
#' stats for each player. Returns one row per QB per game.
#'
#' @param off_stats A data frame returned by `nflreadr::load_offensive_stats()`.
#'
#' @return A tibble with quarterback statistics per player-week,
#'   including cumulative season-to-date values.
#'
#' @examples
#' \dontrun{
#' raw <- nflreadr::load_offensive_stats()
#' qb_stats <- process_qb_stats(raw)
#' }
#'
#' @import dplyr
#' @export
process_qb_stats <- function(off_stats) {
  off_stats %>%
    dplyr::filter(position_group == "QB") %>%
    dplyr::select(
      season, week, season_type,
      player_id, full_name = player_display_name,
      position, recent_team, opponent_team,
      
      # Passing stats
      completions, attempts, passing_yards, passing_tds,
      interceptions, sacks, sack_yards,
      sack_fumbles, sack_fumbles_lost,
      passing_air_yards, passing_yards_after_catch,
      passing_first_downs, passing_epa, passing_2pt_conversions,
      pacr, dakota,
      
      # Rushing stats
      carries, rushing_yards, rushing_tds,
      rushing_fumbles, rushing_fumbles_lost,
      rushing_first_downs, rushing_epa, rushing_2pt_conversions,
      
      # Fantasy
      fantasy_points, fantasy_points_ppr
    ) %>%
    dplyr::filter(!is.na(attempts) | !is.na(carries)) %>%
    dplyr::group_by(season, player_id) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(
      cumulative_completions = cumsum(coalesce(completions, 0)),
      cumulative_attempts = cumsum(coalesce(attempts, 0)),
      cumulative_passing_yards = cumsum(coalesce(passing_yards, 0)),
      cumulative_passing_tds = cumsum(coalesce(passing_tds, 0)),
      cumulative_interceptions = cumsum(coalesce(interceptions, 0)),
      cumulative_sacks = cumsum(coalesce(sacks, 0)),
      cumulative_sack_yards = cumsum(coalesce(sack_yards, 0)),
      cumulative_passing_epa = cumsum(coalesce(passing_epa, 0)),
      cumulative_rushing_yards = cumsum(coalesce(rushing_yards, 0)),
      cumulative_rushing_tds = cumsum(coalesce(rushing_tds, 0)),
      cumulative_rushing_epa = cumsum(coalesce(rushing_epa, 0)),
      cumulative_fantasy_points = cumsum(coalesce(fantasy_points, 0)),
      cumulative_fantasy_points_ppr = cumsum(coalesce(fantasy_points_ppr, 0))
    ) %>%
    dplyr::ungroup()
}

#' Aggregate Quarterback Stats to Player-Season Level
#'
#' Aggregates weekly quarterback stats to one row per player per season.
#' Volume stats are summed; rate stats are averaged. Cumulative columns are excluded.
#'
#' @param qb_stats A data frame from `process_qb_stats()` with one row per player-week.
#'
#' @return A tibble with one row per QB-season, including totals and average efficiency metrics.
#'
#' @examples
#' \dontrun{
#' raw <- nflreadr::load_offensive_stats()
#' qb_weekly <- process_qb_stats(raw)
#' qb_season <- aggregate_qb_season_stats(qb_weekly)
#' }
#'
#' @import dplyr
#' @export
aggregate_qb_season_stats <- function(qb_stats) {
  qb_stats %>%
    dplyr::select(
      -dplyr::starts_with("cumulative_")
    ) %>%
    dplyr::group_by(season, player_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      position = dplyr::first(position),
      recent_team = dplyr::last(recent_team),
      
      games_played = dplyr::n_distinct(week),
      
      # Volume totals
      completions = sum(completions, na.rm = TRUE),
      attempts = sum(attempts, na.rm = TRUE),
      passing_yards = sum(passing_yards, na.rm = TRUE),
      passing_tds = sum(passing_tds, na.rm = TRUE),
      interceptions = sum(interceptions, na.rm = TRUE),
      sacks = sum(sacks, na.rm = TRUE),
      sack_yards = sum(sack_yards, na.rm = TRUE),
      sack_fumbles = sum(sack_fumbles, na.rm = TRUE),
      sack_fumbles_lost = sum(sack_fumbles_lost, na.rm = TRUE),
      passing_air_yards = sum(passing_air_yards, na.rm = TRUE),
      passing_yards_after_catch = sum(passing_yards_after_catch, na.rm = TRUE),
      passing_first_downs = sum(passing_first_downs, na.rm = TRUE),
      passing_epa = sum(passing_epa, na.rm = TRUE),
      passing_2pt_conversions = sum(passing_2pt_conversions, na.rm = TRUE),
      
      carries = sum(carries, na.rm = TRUE),
      rushing_yards = sum(rushing_yards, na.rm = TRUE),
      rushing_tds = sum(rushing_tds, na.rm = TRUE),
      rushing_fumbles = sum(rushing_fumbles, na.rm = TRUE),
      rushing_fumbles_lost = sum(rushing_fumbles_lost, na.rm = TRUE),
      rushing_first_downs = sum(rushing_first_downs, na.rm = TRUE),
      rushing_epa = sum(rushing_epa, na.rm = TRUE),
      rushing_2pt_conversions = sum(rushing_2pt_conversions, na.rm = TRUE),
      
      fantasy_points = sum(fantasy_points, na.rm = TRUE),
      fantasy_points_ppr = sum(fantasy_points_ppr, na.rm = TRUE),
      
      # Rate stats (averaged)
      pacr = mean(pacr, na.rm = TRUE),
      dakota = mean(dakota, na.rm = TRUE),
      
      .groups = "drop"
    )
}

#' Aggregate Quarterback Stats to Career Level
#'
#' Aggregates weekly quarterback stats across all seasons, one row per player.
#' Sums all volume stats and averages key rate stats. Cumulative columns are excluded.
#'
#' @param qb_stats A data frame from `process_qb_stats()` with one row per player-week.
#'
#' @return A tibble with one row per quarterback containing career totals and average efficiency.
#'
#' @examples
#' \dontrun{
#' raw <- nflreadr::load_offensive_stats()
#' qb_weekly <- process_qb_stats(raw)
#' qb_career <- aggregate_qb_career_stats(qb_weekly)
#' }
#'
#' @import dplyr
#' @export
aggregate_qb_career_stats <- function(qb_stats) {
  qb_stats %>%
    dplyr::select(-dplyr::starts_with("cumulative_")) %>%
    dplyr::group_by(player_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      position = dplyr::first(position),
      recent_team = dplyr::last(recent_team),
      
      seasons_played = dplyr::n_distinct(season),
      games_played = dplyr::n_distinct(paste(season, week)),
      
      # Volume totals
      completions = sum(completions, na.rm = TRUE),
      attempts = sum(attempts, na.rm = TRUE),
      passing_yards = sum(passing_yards, na.rm = TRUE),
      passing_tds = sum(passing_tds, na.rm = TRUE),
      interceptions = sum(interceptions, na.rm = TRUE),
      sacks = sum(sacks, na.rm = TRUE),
      sack_yards = sum(sack_yards, na.rm = TRUE),
      sack_fumbles = sum(sack_fumbles, na.rm = TRUE),
      sack_fumbles_lost = sum(sack_fumbles_lost, na.rm = TRUE),
      passing_air_yards = sum(passing_air_yards, na.rm = TRUE),
      passing_yards_after_catch = sum(passing_yards_after_catch, na.rm = TRUE),
      passing_first_downs = sum(passing_first_downs, na.rm = TRUE),
      passing_epa = sum(passing_epa, na.rm = TRUE),
      passing_2pt_conversions = sum(passing_2pt_conversions, na.rm = TRUE),
      
      carries = sum(carries, na.rm = TRUE),
      rushing_yards = sum(rushing_yards, na.rm = TRUE),
      rushing_tds = sum(rushing_tds, na.rm = TRUE),
      rushing_fumbles = sum(rushing_fumbles, na.rm = TRUE),
      rushing_fumbles_lost = sum(rushing_fumbles_lost, na.rm = TRUE),
      rushing_first_downs = sum(rushing_first_downs, na.rm = TRUE),
      rushing_epa = sum(rushing_epa, na.rm = TRUE),
      rushing_2pt_conversions = sum(rushing_2pt_conversions, na.rm = TRUE),
      
      fantasy_points = sum(fantasy_points, na.rm = TRUE),
      fantasy_points_ppr = sum(fantasy_points_ppr, na.rm = TRUE),
      
      # Rate stats (averaged across games)
      pacr = mean(pacr, na.rm = TRUE),
      dakota = mean(dakota, na.rm = TRUE),
      
      .groups = "drop"
    )
}

#' Process Weekly Running Back Stats with Cumulative Season Totals
#'
#' Filters and cleans offensive player stats for running backs (RBs), retaining
#' rushing, receiving, EPA, and fantasy fields. Adds cumulative season-to-date
#' stats for each player. Returns one row per RB per game.
#'
#' @param off_stats A data frame returned by `nflreadr::load_offensive_stats()`.
#'
#' @return A tibble with running back statistics per player-week,
#'   including cumulative season-to-date values.
#'
#' @examples
#' \dontrun{
#' raw <- nflreadr::load_offensive_stats()
#' rb_stats <- process_rb_stats(raw)
#' }
#'
#' @import dplyr
#' @export
process_rb_stats <- function(off_stats) {
  off_stats %>%
    dplyr::filter(position_group == "RB") %>%
    dplyr::select(
      season, week, season_type,
      player_id, full_name = player_display_name,
      position, recent_team, opponent_team,
      
      # Rushing
      carries, rushing_yards, rushing_tds,
      rushing_fumbles, rushing_fumbles_lost,
      rushing_first_downs, rushing_epa, rushing_2pt_conversions,
      
      # Receiving
      targets, receptions, receiving_yards, receiving_tds,
      receiving_fumbles, receiving_fumbles_lost,
      receiving_air_yards, receiving_yards_after_catch,
      receiving_first_downs, receiving_epa, receiving_2pt_conversions,
      
      # Advanced
      racr, target_share, air_yards_share, wopr,
      
      # Fantasy
      fantasy_points, fantasy_points_ppr
    ) %>%
    dplyr::filter(
      !is.na(carries) | !is.na(targets)
    ) %>%
    dplyr::group_by(season, player_id) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(
      # Cumulative rushing
      cumulative_carries = cumsum(coalesce(carries, 0)),
      cumulative_rushing_yards = cumsum(coalesce(rushing_yards, 0)),
      cumulative_rushing_tds = cumsum(coalesce(rushing_tds, 0)),
      cumulative_rushing_epa = cumsum(coalesce(rushing_epa, 0)),
      
      # Cumulative receiving
      cumulative_targets = cumsum(coalesce(targets, 0)),
      cumulative_receptions = cumsum(coalesce(receptions, 0)),
      cumulative_receiving_yards = cumsum(coalesce(receiving_yards, 0)),
      cumulative_receiving_tds = cumsum(coalesce(receiving_tds, 0)),
      cumulative_receiving_epa = cumsum(coalesce(receiving_epa, 0)),
      
      # Cumulative fantasy
      cumulative_fantasy_points = cumsum(coalesce(fantasy_points, 0)),
      cumulative_fantasy_points_ppr = cumsum(coalesce(fantasy_points_ppr, 0))
    ) %>%
    dplyr::ungroup()
}

#' Aggregate Running Back Stats to Player-Season Level
#'
#' Aggregates weekly running back stats to one row per player per season.
#' Volume stats are summed; rate stats are averaged. Cumulative columns are excluded.
#'
#' @param rb_stats A data frame from `process_rb_stats()` with one row per player-week.
#'
#' @return A tibble with one row per RB-season, including totals and average efficiency metrics.
#'
#' @examples
#' \dontrun{
#' raw <- nflreadr::load_offensive_stats()
#' rb_weekly <- process_rb_stats(raw)
#' rb_season <- aggregate_rb_season_stats(rb_weekly)
#' }
#'
#' @import dplyr
#' @export
aggregate_rb_season_stats <- function(rb_stats) {
  rb_stats %>%
    dplyr::select(-dplyr::starts_with("cumulative_")) %>%
    dplyr::group_by(season, player_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      position = dplyr::first(position),
      recent_team = dplyr::last(recent_team),
      
      games_played = dplyr::n_distinct(week),
      
      # Rushing totals
      carries = sum(carries, na.rm = TRUE),
      rushing_yards = sum(rushing_yards, na.rm = TRUE),
      rushing_tds = sum(rushing_tds, na.rm = TRUE),
      rushing_epa = sum(rushing_epa, na.rm = TRUE),
      rushing_fumbles = sum(rushing_fumbles, na.rm = TRUE),
      rushing_fumbles_lost = sum(rushing_fumbles_lost, na.rm = TRUE),
      rushing_first_downs = sum(rushing_first_downs, na.rm = TRUE),
      rushing_2pt_conversions = sum(rushing_2pt_conversions, na.rm = TRUE),
      
      # Receiving totals
      targets = sum(targets, na.rm = TRUE),
      receptions = sum(receptions, na.rm = TRUE),
      receiving_yards = sum(receiving_yards, na.rm = TRUE),
      receiving_tds = sum(receiving_tds, na.rm = TRUE),
      receiving_epa = sum(receiving_epa, na.rm = TRUE),
      receiving_fumbles = sum(receiving_fumbles, na.rm = TRUE),
      receiving_fumbles_lost = sum(receiving_fumbles_lost, na.rm = TRUE),
      receiving_air_yards = sum(receiving_air_yards, na.rm = TRUE),
      receiving_yards_after_catch = sum(receiving_yards_after_catch, na.rm = TRUE),
      receiving_first_downs = sum(receiving_first_downs, na.rm = TRUE),
      receiving_2pt_conversions = sum(receiving_2pt_conversions, na.rm = TRUE),
      
      # Fantasy
      fantasy_points = sum(fantasy_points, na.rm = TRUE),
      fantasy_points_ppr = sum(fantasy_points_ppr, na.rm = TRUE),
      
      # Efficiency / rate stats (averaged)
      racr = mean(racr, na.rm = TRUE),
      target_share = mean(target_share, na.rm = TRUE),
      air_yards_share = mean(air_yards_share, na.rm = TRUE),
      wopr = mean(wopr, na.rm = TRUE),
      
      .groups = "drop"
    )
}

#' Aggregate Running Back Stats to Career Level
#'
#' Aggregates weekly running back stats across all seasons, one row per player.
#' Volume stats are summed, rate stats are averaged, and cumulative columns are excluded.
#'
#' @param rb_stats A data frame from `process_rb_stats()` with one row per player-week.
#'
#' @return A tibble with one row per running back containing career totals and average efficiency.
#'
#' @examples
#' \dontrun{
#' raw <- nflreadr::load_offensive_stats()
#' rb_weekly <- process_rb_stats(raw)
#' rb_career <- aggregate_rb_career_stats(rb_weekly)
#' }
#'
#' @import dplyr
#' @export
aggregate_rb_career_stats <- function(rb_stats) {
  rb_stats %>%
    dplyr::select(-dplyr::starts_with("cumulative_")) %>%
    dplyr::group_by(player_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      position = dplyr::first(position),
      recent_team = dplyr::last(recent_team),
      
      seasons_played = dplyr::n_distinct(season),
      games_played = dplyr::n_distinct(paste(season, week)),
      
      # Rushing totals
      carries = sum(carries, na.rm = TRUE),
      rushing_yards = sum(rushing_yards, na.rm = TRUE),
      rushing_tds = sum(rushing_tds, na.rm = TRUE),
      rushing_epa = sum(rushing_epa, na.rm = TRUE),
      rushing_fumbles = sum(rushing_fumbles, na.rm = TRUE),
      rushing_fumbles_lost = sum(rushing_fumbles_lost, na.rm = TRUE),
      rushing_first_downs = sum(rushing_first_downs, na.rm = TRUE),
      rushing_2pt_conversions = sum(rushing_2pt_conversions, na.rm = TRUE),
      
      # Receiving totals
      targets = sum(targets, na.rm = TRUE),
      receptions = sum(receptions, na.rm = TRUE),
      receiving_yards = sum(receiving_yards, na.rm = TRUE),
      receiving_tds = sum(receiving_tds, na.rm = TRUE),
      receiving_epa = sum(receiving_epa, na.rm = TRUE),
      receiving_fumbles = sum(receiving_fumbles, na.rm = TRUE),
      receiving_fumbles_lost = sum(receiving_fumbles_lost, na.rm = TRUE),
      receiving_air_yards = sum(receiving_air_yards, na.rm = TRUE),
      receiving_yards_after_catch = sum(receiving_yards_after_catch, na.rm = TRUE),
      receiving_first_downs = sum(receiving_first_downs, na.rm = TRUE),
      receiving_2pt_conversions = sum(receiving_2pt_conversions, na.rm = TRUE),
      
      # Fantasy
      fantasy_points = sum(fantasy_points, na.rm = TRUE),
      fantasy_points_ppr = sum(fantasy_points_ppr, na.rm = TRUE),
      
      # Efficiency / rate stats (averaged)
      racr = mean(racr, na.rm = TRUE),
      target_share = mean(target_share, na.rm = TRUE),
      air_yards_share = mean(air_yards_share, na.rm = TRUE),
      wopr = mean(wopr, na.rm = TRUE),
      
      .groups = "drop"
    )
}

#' Process Weekly Receiver Stats (WR/TE) with Cumulative Season Totals
#'
#' Filters and processes offensive stats for wide receivers (WR) or tight ends (TE),
#' returning per-week stats with cumulative season-to-date receiving totals.
#'
#' @param off_stats A data frame returned by `nflreadr::load_offensive_stats()`.
#' @param position_group Either `"WR"` or `"TE"`. Filters stats to that position group.
#'
#' @return A tibble with one row per player-week including receiving stats and cumulative totals.
#'
#' @examples
#' \dontrun{
#' raw <- nflreadr::load_offensive_stats()
#' wr_weekly <- process_receiver_stats(raw, "WR")
#' te_weekly <- process_receiver_stats(raw, "TE")
#' }
#'
#' @import dplyr
#' @export
process_receiver_stats <- function(off_stats, position_group = c("WR", "TE")) {
  position_group <- match.arg(position_group)
  
  off_stats %>%
    dplyr::filter(position_group == !!position_group) %>%
    dplyr::select(
      season, week, season_type,
      player_id, full_name = player_display_name,
      position, recent_team, opponent_team,
      
      # Receiving
      targets, receptions, receiving_yards, receiving_tds,
      receiving_fumbles, receiving_fumbles_lost,
      receiving_air_yards, receiving_yards_after_catch,
      receiving_first_downs, receiving_epa, receiving_2pt_conversions,
      
      # Advanced
      racr, target_share, air_yards_share, wopr,
      
      # Fantasy
      fantasy_points, fantasy_points_ppr
    ) %>%
    dplyr::filter(!is.na(targets)) %>%
    dplyr::group_by(season, player_id) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(
      cumulative_targets = cumsum(coalesce(targets, 0)),
      cumulative_receptions = cumsum(coalesce(receptions, 0)),
      cumulative_receiving_yards = cumsum(coalesce(receiving_yards, 0)),
      cumulative_receiving_tds = cumsum(coalesce(receiving_tds, 0)),
      cumulative_receiving_epa = cumsum(coalesce(receiving_epa, 0)),
      cumulative_fantasy_points = cumsum(coalesce(fantasy_points, 0)),
      cumulative_fantasy_points_ppr = cumsum(coalesce(fantasy_points_ppr, 0))
    ) %>%
    dplyr::ungroup()
}


#' Aggregate Receiver Stats to Player-Season Level (WR/TE)
#'
#' Aggregates weekly receiver stats to one row per player-season. Volume stats are summed;
#' rate stats are averaged. Works for both wide receivers and tight ends.
#'
#' @param receiver_stats A data frame from `process_receiver_stats()` with one row per player-week.
#'
#' @return A tibble with one row per player-season including receiving totals and efficiency metrics.
#'
#' @examples
#' \dontrun{
#' wr_weekly <- process_receiver_stats(raw, "WR")
#' wr_season <- aggregate_receiver_season_stats(wr_weekly)
#' }
#'
#' @import dplyr
#' @export
aggregate_receiver_season_stats <- function(receiver_stats) {
  receiver_stats %>%
    dplyr::select(-dplyr::starts_with("cumulative_")) %>%
    dplyr::group_by(season, player_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      position = dplyr::first(position),
      recent_team = dplyr::last(recent_team),
      
      games_played = dplyr::n_distinct(week),
      
      # Receiving totals
      targets = sum(targets, na.rm = TRUE),
      receptions = sum(receptions, na.rm = TRUE),
      receiving_yards = sum(receiving_yards, na.rm = TRUE),
      receiving_tds = sum(receiving_tds, na.rm = TRUE),
      receiving_fumbles = sum(receiving_fumbles, na.rm = TRUE),
      receiving_fumbles_lost = sum(receiving_fumbles_lost, na.rm = TRUE),
      receiving_air_yards = sum(receiving_air_yards, na.rm = TRUE),
      receiving_yards_after_catch = sum(receiving_yards_after_catch, na.rm = TRUE),
      receiving_first_downs = sum(receiving_first_downs, na.rm = TRUE),
      receiving_epa = sum(receiving_epa, na.rm = TRUE),
      receiving_2pt_conversions = sum(receiving_2pt_conversions, na.rm = TRUE),
      
      # Fantasy totals
      fantasy_points = sum(fantasy_points, na.rm = TRUE),
      fantasy_points_ppr = sum(fantasy_points_ppr, na.rm = TRUE),
      
      # Rate stats (averaged)
      racr = mean(racr, na.rm = TRUE),
      target_share = mean(target_share, na.rm = TRUE),
      air_yards_share = mean(air_yards_share, na.rm = TRUE),
      wopr = mean(wopr, na.rm = TRUE),
      
      .groups = "drop"
    )
}

#' Aggregate Receiver Stats to Career Level (WR/TE)
#'
#' Aggregates weekly receiver stats across all seasons into one row per player.
#' Sums volume stats and averages rate stats like RACR and WOPR. Works for both WR and TE.
#'
#' @param receiver_stats A data frame from `process_receiver_stats()` with one row per player-week.
#'
#' @return A tibble with one row per player including career totals and average efficiency metrics.
#'
#' @examples
#' \dontrun{
#' wr_weekly <- process_receiver_stats(raw, "WR")
#' wr_career <- aggregate_receiver_career_stats(wr_weekly)
#' }
#'
#' @import dplyr
#' @export
aggregate_receiver_career_stats <- function(receiver_stats) {
  receiver_stats %>%
    dplyr::select(-dplyr::starts_with("cumulative_")) %>%
    dplyr::group_by(player_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      position = dplyr::first(position),
      recent_team = dplyr::last(recent_team),
      
      seasons_played = dplyr::n_distinct(season),
      games_played = dplyr::n_distinct(paste(season, week)),
      
      # Receiving totals
      targets = sum(targets, na.rm = TRUE),
      receptions = sum(receptions, na.rm = TRUE),
      receiving_yards = sum(receiving_yards, na.rm = TRUE),
      receiving_tds = sum(receiving_tds, na.rm = TRUE),
      receiving_fumbles = sum(receiving_fumbles, na.rm = TRUE),
      receiving_fumbles_lost = sum(receiving_fumbles_lost, na.rm = TRUE),
      receiving_air_yards = sum(receiving_air_yards, na.rm = TRUE),
      receiving_yards_after_catch = sum(receiving_yards_after_catch, na.rm = TRUE),
      receiving_first_downs = sum(receiving_first_downs, na.rm = TRUE),
      receiving_epa = sum(receiving_epa, na.rm = TRUE),
      receiving_2pt_conversions = sum(receiving_2pt_conversions, na.rm = TRUE),
      
      # Fantasy totals
      fantasy_points = sum(fantasy_points, na.rm = TRUE),
      fantasy_points_ppr = sum(fantasy_points_ppr, na.rm = TRUE),
      
      # Rate stats (averaged)
      racr = mean(racr, na.rm = TRUE),
      target_share = mean(target_share, na.rm = TRUE),
      air_yards_share = mean(air_yards_share, na.rm = TRUE),
      wopr = mean(wopr, na.rm = TRUE),
      
      .groups = "drop"
    )
}

