#' Clean and select columns from raw special teams stats (no aggregation)
#'
#' Returns a validated version of the special teams stats, one row per player-week,
#' retaining all key field goal and PAT variables for modeling or reporting.
#'
#' @param st_stats_raw A data frame read from `nflreadr::load_special_teams_stats()` or the equivalent raw parquet file.
#'
#' @return A tibble with cleaned and typed columns; one row per player-week.
#' @export
#'
#' @examples
#' st_stats_clean <- process_special_teams_stats(st_stats_raw)
process_special_teams_stats <- function(st_stats_raw) {
  require(dplyr)
  
  st_stats_raw %>%
    transmute(
      season,
      week,
      season_type,
      player_id,
      player_name = player_display_name,
      team,
      position,
      fg_att = as.integer(fg_att),
      fg_made = as.integer(fg_made),
      fg_missed = as.integer(fg_missed),
      fg_blocked = as.integer(fg_blocked),
      fg_long = as.numeric(fg_long),
      fg_pct = as.numeric(fg_pct),
      fg_made_0_19 = as.integer(fg_made_0_19),
      fg_made_20_29 = as.integer(fg_made_20_29),
      fg_made_30_39 = as.integer(fg_made_30_39),
      fg_made_40_49 = as.integer(fg_made_40_49),
      fg_made_50_59 = as.integer(fg_made_50_59),
      fg_made_60 = as.integer(fg_made_60_),
      fg_missed_0_19 = as.integer(fg_missed_0_19),
      fg_missed_20_29 = as.integer(fg_missed_20_29),
      fg_missed_30_39 = as.integer(fg_missed_30_39),
      fg_missed_40_49 = as.integer(fg_missed_40_49),
      fg_missed_50_59 = as.integer(fg_missed_50_59),
      fg_missed_60 = as.integer(fg_missed_60_),
      fg_made_distance = as.numeric(fg_made_distance),
      fg_missed_distance = as.numeric(fg_missed_distance),
      fg_blocked_distance = as.numeric(fg_blocked_distance),
      pat_att = as.integer(pat_att),
      pat_made = as.integer(pat_made),
      pat_missed = as.integer(pat_missed),
      pat_blocked = as.integer(pat_blocked),
      pat_pct = as.numeric(pat_pct),
      gwfg_att = as.integer(gwfg_att),
      gwfg_distance = as.numeric(gwfg_distance),
      gwfg_made = as.integer(gwfg_made),
      gwfg_missed = as.integer(gwfg_missed),
      gwfg_blocked = as.integer(gwfg_blocked)
    )
}

#' Summarize special teams stats at the season level
#'
#' Aggregates weekly special teams data to one row per player per season,
#' summing all numeric performance metrics.
#'
#' @param st_clean A cleaned special teams data frame, typically the output of `process_special_teams_stats()`.
#'
#' @return A tibble with one row per player per season, with cumulative stats.
#' @export
#'
#' @examples
#' st_summary <- summarize_special_teams_by_season(st_stats_clean)
summarize_special_teams_by_season <- function(st_clean) {
  require(dplyr)
  
  st_clean %>%
    group_by(season, player_id) %>%
    summarize(
      player_name = dplyr::last(na.omit(player_name)),
      team = dplyr::last(na.omit(team)),
      position = dplyr::last(na.omit(position)),
      games_played = n_distinct(week),
      fg_att = sum(fg_att, na.rm = TRUE),
      fg_made = sum(fg_made, na.rm = TRUE),
      fg_missed = sum(fg_missed, na.rm = TRUE),
      fg_blocked = sum(fg_blocked, na.rm = TRUE),
      fg_pct = ifelse(fg_att > 0, fg_made / fg_att, NA_real_),
      fg_long = if (all(is.na(fg_long))) NA_real_ else max(fg_long, na.rm = TRUE),
      fg_made_0_19 = sum(fg_made_0_19, na.rm = TRUE),
      fg_made_20_29 = sum(fg_made_20_29, na.rm = TRUE),
      fg_made_30_39 = sum(fg_made_30_39, na.rm = TRUE),
      fg_made_40_49 = sum(fg_made_40_49, na.rm = TRUE),
      fg_made_50_59 = sum(fg_made_50_59, na.rm = TRUE),
      fg_made_60 = sum(fg_made_60, na.rm = TRUE),
      fg_missed_0_19 = sum(fg_missed_0_19, na.rm = TRUE),
      fg_missed_20_29 = sum(fg_missed_20_29, na.rm = TRUE),
      fg_missed_30_39 = sum(fg_missed_30_39, na.rm = TRUE),
      fg_missed_40_49 = sum(fg_missed_40_49, na.rm = TRUE),
      fg_missed_50_59 = sum(fg_missed_50_59, na.rm = TRUE),
      fg_missed_60 = sum(fg_missed_60, na.rm = TRUE),
      fg_made_distance = sum(fg_made_distance, na.rm = TRUE),
      fg_missed_distance = sum(fg_missed_distance, na.rm = TRUE),
      fg_blocked_distance = sum(fg_blocked_distance, na.rm = TRUE),
      pat_att = sum(pat_att, na.rm = TRUE),
      pat_made = sum(pat_made, na.rm = TRUE),
      pat_missed = sum(pat_missed, na.rm = TRUE),
      pat_blocked = sum(pat_blocked, na.rm = TRUE),
      pat_pct = ifelse(pat_att > 0, pat_made / pat_att, NA_real_),
      gwfg_att = sum(gwfg_att, na.rm = TRUE),
      gwfg_distance = sum(gwfg_distance, na.rm = TRUE),
      gwfg_made = sum(gwfg_made, na.rm = TRUE),
      gwfg_missed = sum(gwfg_missed, na.rm = TRUE),
      gwfg_blocked = sum(gwfg_blocked, na.rm = TRUE),
      .groups = "drop"
    )
}

#' Add cumulative stats to special teams data by player-week
#'
#' Computes cumulative versions of key special teams stats, including field goals made/missed by distance bin,
#' for each player-week within a season.
#'
#' @param st_clean A cleaned special teams data frame, typically the output of `process_special_teams_stats()`.
#'
#' @return A tibble with the original player-week rows and added cumulative stats.
#' @export
#'
#' @examples
#' st_cume <- add_cumulative_special_teams_stats(st_stats_clean)
add_cumulative_special_teams_stats <- function(st_clean) {
  require(dplyr)
  
  st_clean %>%
    arrange(season, player_id, week) %>%
    group_by(season, player_id) %>%
    mutate(
      # FG total stats
      cumulative_fg_att = cumsum(replace_na(fg_att, 0)),
      cumulative_fg_made = cumsum(replace_na(fg_made, 0)),
      cumulative_fg_missed = cumsum(replace_na(fg_missed, 0)),
      cumulative_fg_blocked = cumsum(replace_na(fg_blocked, 0)),
      cumulative_fg_pct = if_else(cumulative_fg_att > 0, cumulative_fg_made / cumulative_fg_att, NA_real_),
      
      # PAT stats
      cumulative_pat_att = cumsum(replace_na(pat_att, 0)),
      cumulative_pat_made = cumsum(replace_na(pat_made, 0)),
      cumulative_pat_missed = cumsum(replace_na(pat_missed, 0)),
      cumulative_pat_blocked = cumsum(replace_na(pat_blocked, 0)),
      cumulative_pat_pct = if_else(cumulative_pat_att > 0, cumulative_pat_made / cumulative_pat_att, NA_real_),
      
      # Game-winning FG stats
      cumulative_gwfg_att = cumsum(replace_na(gwfg_att, 0)),
      cumulative_gwfg_made = cumsum(replace_na(gwfg_made, 0)),
      cumulative_gwfg_missed = cumsum(replace_na(gwfg_missed, 0)),
      cumulative_gwfg_blocked = cumsum(replace_na(gwfg_blocked, 0)),
      
      # FG made by distance
      cumulative_fg_made_0_19 = cumsum(replace_na(fg_made_0_19, 0)),
      cumulative_fg_made_20_29 = cumsum(replace_na(fg_made_20_29, 0)),
      cumulative_fg_made_30_39 = cumsum(replace_na(fg_made_30_39, 0)),
      cumulative_fg_made_40_49 = cumsum(replace_na(fg_made_40_49, 0)),
      cumulative_fg_made_50_59 = cumsum(replace_na(fg_made_50_59, 0)),
      cumulative_fg_made_60 = cumsum(replace_na(fg_made_60, 0)),
      
      # FG missed by distance
      cumulative_fg_missed_0_19 = cumsum(replace_na(fg_missed_0_19, 0)),
      cumulative_fg_missed_20_29 = cumsum(replace_na(fg_missed_20_29, 0)),
      cumulative_fg_missed_30_39 = cumsum(replace_na(fg_missed_30_39, 0)),
      cumulative_fg_missed_40_49 = cumsum(replace_na(fg_missed_40_49, 0)),
      cumulative_fg_missed_50_59 = cumsum(replace_na(fg_missed_50_59, 0)),
      cumulative_fg_missed_60 = cumsum(replace_na(fg_missed_60, 0))
    ) %>%
    ungroup()
}


