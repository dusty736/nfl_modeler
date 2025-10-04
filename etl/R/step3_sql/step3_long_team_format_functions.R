#' Pivot team stats to long format
#'
#' Reads a team-level stats Parquet file, joins with an opponent lookup table,
#' and pivots all statistics into long format. Includes stat_type
#' (either "base" or "cumulative").
#'
#' @param file_path Character string. Path to the Parquet file containing weekly team stats.
#' @param opponent_df Data frame with columns `season`, `week`, `team`, `opponent`, and `season_type`
#'   used to add opponent and season type information.
#' @param team_col Character string. Name of the column in the parquet file that
#'   contains the team abbreviation (e.g., `"recent_team"` or `"team"`).
#'
#' @return A tibble with columns:
#'   \describe{
#'     \item{team}{Team abbreviation (e.g., "SEA")}
#'     \item{season}{Season year}
#'     \item{season_type}{Season type ("REG", "POST")}
#'     \item{week}{Week number}
#'     \item{opponent}{Opponent team abbreviation}
#'     \item{stat_type}{Either "base" or "cumulative"}
#'     \item{stat_name}{Name of statistic (e.g., "passing_yards")}
#'     \item{stat_value}{Value of the statistic}
#'   }
#'
#' @examples
#' \dontrun{
#' opponent_df <- readRDS("data/opponent_lookup.rds")
#' pivot_team_stats_long(
#'   "data/processed/team_offense.parquet",
#'   opponent_df,
#'   team_col = "recent_team"
#' )
#' }
#' @export
pivot_team_stats_long <- function(file_path='', data=NULL, opponent_df, team_col = "team") {
  if(file_path != '') {
    df <- arrow::read_parquet(file_path)
  } else {
    df <- data
  }
  
  # Standardize the team column
  df <- df %>%
    dplyr::rename(team = !!rlang::sym(team_col))
  
  id_cols <- c("team", "season", "week")
  
  df_long <- df %>%
    dplyr::left_join(opponent_df,
                     by = c("season", "week", "team")
    ) %>%
    tidyr::pivot_longer(
      cols = -dplyr::all_of(c(id_cols, "opponent", "season_type")),
      names_to = "stat_name",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      stat_type = ifelse(grepl("^cumulative_", stat_name), "cumulative", "base"),
      stat_name = gsub("^cumulative_", "", stat_name)
    ) %>%
    dplyr::select(
      team, season, season_type, week, opponent,
      stat_type, stat_name, value
    )
  
  return(df_long)
}

#' Pivot game results to long format
#'
#' Reads a game results Parquet file and pivots numeric statistics into long format.
#' Year-to-date (`*_ytd`) stats are marked as cumulative, while other numeric stats
#' are marked as base.
#'
#' @param file_path Character string. Path to the Parquet file containing game results.
#'
#' @return A tibble with columns:
#'   \describe{
#'     \item{game_id}{Unique game identifier (e.g., "1999_01_ARI_PHI")}
#'     \item{team}{Team abbreviation (e.g., "ARI")}
#'     \item{season}{Season year}
#'     \item{week}{Week number}
#'     \item{stat_type}{Either "base" or "cumulative"}
#'     \item{stat_name}{Name of statistic (e.g., "points_scored")}
#'     \item{stat_value}{Value of the statistic}
#'   }
#'
#' @examples
#' \dontrun{
#' pivot_game_results_long("data/processed/game_results.parquet")
#' }
#' @export
pivot_game_results_long <- function(file_path='', data=NULL) {
  if(file_path != '') {
    df <- arrow::read_parquet(file_path)
  } else {
    df <- data
  }
  
  # Identifier columns we must NOT pivot
  id_cols <- c(
    "season", "week", "season_type", "team_id", "game_id"
  )
  
  # Only pivot the numeric columns not in id_cols
  num_cols <- setdiff(names(dplyr::select(df, where(is.numeric))), id_cols)
  
  df_long <- df %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(num_cols),
      names_to = "stat_name",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      stat_type = ifelse(grepl("_ytd$", stat_name), "cumulative", "base"),
      stat_name = gsub("_ytd$", "", stat_name)
    ) %>%
    dplyr::select(
      game_id,
      team = team_id,
      season,
      week,
      stat_type,
      stat_name,
      value
    )
  
  return(df_long)
}

#' Pivot special teams player stats to team-level long format
#'
#' Aggregates player-level special teams stats to team level,
#' then pivots numeric columns to long format.
#'
#' @param file_path Character string. Path to the Parquet file with special teams player stats.
#'
#' @return A tibble with columns:
#'   \describe{
#'     \item{team}{Team abbreviation (e.g., "PHI")}
#'     \item{season}{Season year}
#'     \item{season_type}{Season type (REG, POST)}
#'     \item{week}{Week number}
#'     \item{stat_type}{Either "base" or "cumulative"}
#'     \item{stat_name}{Name of statistic (e.g., "fg_att")}
#'     \item{stat_value}{Aggregated value of the statistic}
#'   }
#'
#' @examples
#' \dontrun{
#' pivot_special_teams_long("data/processed/special_teams.parquet")
#' }
#' @export
pivot_special_teams_long <- function(file_path='', data=NULL) {
  if(file_path != '') {
    df <- arrow::read_parquet(file_path)
  } else {
    df <- data
  }
  
  # Columns to group by for team-level aggregation
  id_cols <- c("team", "season", "season_type", "week")
  
  # Aggregate to team-level first
  df_team <- df %>%
    dplyr::group_by(dplyr::across(all_of(id_cols))) %>%
    dplyr::summarise(dplyr::across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop")
  
  # Identify numeric columns for pivot
  num_cols <- setdiff(names(df_team), id_cols)
  
  df_long <- df_team %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(num_cols),
      names_to = "stat_name",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      stat_type = ifelse(grepl("^cumulative_", stat_name), "cumulative", "base"),
      stat_name = gsub("^cumulative_", "", stat_name)
    ) %>%
    dplyr::select(team, season, season_type, week, stat_type, stat_name, value)
  
  return(df_long)
}


#' Aggregate Weekly Team Stats to Season Level
#'
#' This function takes long-format weekly team stats and aggregates them
#' to season-level statistics according to a classification table
#' (sum, average, both, max, max_abs).
#'
#' @param weekly_df Data frame in long format with columns:
#'   team, season, season_type, week, stat_name, value.
#'
#' @return A tibble with columns:
#'   team, season, season_type (REG, POST, TOTAL),
#'   stat_name, stat_type (sum, avg, max, max_abs), value.
#' @export
#'
aggregate_team_season_stats <- function(weekly_df) {
  # ---- classification table ----
  stat_class <- tribble(
    ~stat_name,                  ~agg,
    "completions",               "both",
    "attempts",                  "both",
    "passing_yards",             "both",
    "passing_tds",               "both",
    "interceptions",             "both",
    "sacks",                     "both",
    "sack_yards",                "sum",
    "passing_air_yards",         "both",
    "passing_yards_after_catch", "both",
    "passing_first_downs",       "both",
    "passing_epa",               "avg",
    "passing_2pt_conversions",   "sum",
    "carries",                   "both",
    "rushing_yards",             "both",
    "rushing_tds",               "both",
    "rushing_fumbles",           "sum",
    "rushing_fumbles_lost",      "sum",
    "rushing_first_downs",       "both",
    "rushing_epa",               "avg",
    "rushing_2pt_conversions",   "sum",
    "receptions",                "both",
    "targets",                   "both",
    "receiving_yards",           "both",
    "receiving_tds",             "both",
    "receiving_fumbles",         "sum",
    "receiving_fumbles_lost",    "sum",
    "receiving_air_yards",       "both",
    "receiving_yards_after_catch","both",
    "receiving_first_downs",     "both",
    "receiving_epa",             "avg",
    "receiving_2pt_conversions", "sum",
    "pacr",                      "avg",
    "dakota",                    "avg",
    "racr",                      "avg",
    "wopr",                      "avg",
    "target_share",              "avg",
    "air_yards_share",           "avg",
    "n_players",                 "avg",
    "def_tackles",               "both",
    "def_tackles_solo",          "both",
    "def_tackle_assists",        "both",
    "def_tackles_for_loss",      "both",
    "def_tackles_for_loss_yards","sum",
    "def_fumbles_forced",        "sum",
    "def_sacks",                 "both",
    "def_sack_yards",            "sum",
    "def_qb_hits",               "both",
    "def_interceptions",         "both",
    "def_interception_yards",    "sum",
    "def_pass_defended",         "both",
    "def_tds",                   "sum",
    "def_fumbles",               "sum",
    "def_fumble_recovery_own",   "sum",
    "def_fumble_recovery_yards_own","sum",
    "def_fumble_recovery_opp",   "sum",
    "def_fumble_recovery_yards_opp","sum",
    "def_safety",                "sum",
    "def_penalty",               "sum",
    "def_penalty_yards",         "sum",
    "fg_att",                    "sum",
    "fg_made",                   "sum",
    "fg_missed",                 "sum",
    "fg_blocked",                "sum",
    "fg_long",                   "max",
    "fg_pct",                    "avg",
    "fg_made_0_19",              "sum",
    "fg_made_20_29",             "sum",
    "fg_made_30_39",             "sum",
    "fg_made_40_49",             "sum",
    "fg_made_50_59",             "sum",
    "fg_made_60",                "sum",
    "fg_missed_0_19",            "sum",
    "fg_missed_20_29",           "sum",
    "fg_missed_30_39",           "sum",
    "fg_missed_40_49",           "sum",
    "fg_missed_50_59",           "sum",
    "fg_missed_60",              "sum",
    "fg_made_distance",          "avg",
    "fg_missed_distance",        "avg",
    "fg_blocked_distance",       "avg",
    "pat_att",                   "sum",
    "pat_made",                  "sum",
    "pat_missed",                "sum",
    "pat_blocked",               "sum",
    "pat_pct",                   "avg",
    "gwfg_att",                  "sum",
    "gwfg_distance",             "avg",
    "gwfg_made",                 "sum",
    "gwfg_missed",               "sum",
    "gwfg_blocked",              "sum",
    "weekly_injuries",           "sum",
    "injuries",                  "sum",
    "points_scored",             "both",
    "points_allowed",            "both",
    "wins_entering",             "max",
    "losses_entering",           "max",
    "ties_entering",             "max",
    "point_diff",                "max_abs"
  ) %>%
    mutate(stat_name = recode(stat_name,
                              "wins_entering" = "wins",
                              "losses_entering" = "losses",
                              "ties_entering" = "ties"))
  
  # ---- join with classification ----
  df <- weekly_df %>%
    filter(stat_type == 'base') %>% 
    left_join(stat_class, by = "stat_name")
  
  # ---- aggregation ----
  agg_df <- df %>%
    group_by(team, season, season_type, stat_name, agg) %>%
    summarise(
      value = case_when(
        agg == "sum" ~ sum(value, na.rm = TRUE),
        agg == "avg" ~ mean(value, na.rm = TRUE),
        agg == "max" ~ max(value, na.rm = TRUE),
        agg == "max_abs" ~ max(abs(value), na.rm = TRUE),
        TRUE ~ NA_real_
      ),
      .groups = "drop"
    )
  
  # ---- expand "both" into two rows ----
  both_df <- df %>%
    filter(agg == "both") %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(
      sum_value = sum(value, na.rm = TRUE),
      avg_value = mean(value, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    pivot_longer(cols = c(sum_value, avg_value),
                 names_to = "agg",
                 values_to = "value") %>%
    mutate(
      agg = ifelse(agg == "sum_value", "sum", "avg")
    )
  
  # combine
  season_df <- bind_rows(
    agg_df,
    both_df
  ) %>%
    rename(agg_type = agg)
  
  # ---- add TOTAL season_type ----
  total_df <- season_df %>%
    group_by(team, season, stat_name, agg_type) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(season_type = "TOTAL")
  
  bind_rows(season_df, total_df) %>%
    arrange(team, season, season_type, stat_name, agg_type)
}

#' Aggregate Weekly Team Stats to Season Level
#'
#' Aggregates weekly team stats into season-level totals and averages,
#' using predefined stat classification vectors. Handles REG, POST,
#' and TOTAL season types.
#'
#' @param weekly_df Long-format weekly stats with cols:
#'   team, season, season_type, week, stat_name, value
#'
#' @return Tibble with team, season, season_type, stat_name,
#'   stat_type (sum|avg|max|max_abs), value
#' @export
#'
aggregate_team_season_stats <- function(weekly_df) {
  library(dplyr)
  library(tidyr)
  
  # ---- classification vectors ----
  # ---- classification vectors ----
  sum_and_avg <- c(
    # offense/defense/player volumes
    "completions","attempts","passing_yards","passing_tds","interceptions",
    "sacks","passing_air_yards","passing_yards_after_catch","passing_first_downs",
    "carries","rushing_yards","rushing_tds","rushing_first_downs",
    "receptions","targets","receiving_yards","receiving_tds",
    "receiving_air_yards","receiving_yards_after_catch","receiving_first_downs",
    "def_tackles","def_tackles_solo","def_tackle_assists","def_tackles_for_loss",
    "def_sacks","def_qb_hits","def_interceptions","def_pass_defended",
    # scoring / injuries
    "points_scored","points_allowed","weekly_injuries","injuries",
    # --- NEW: team-game volumes you’ll want totals AND per-game means for ---
    "drives","plays_total","red_zone_trips","red_zone_scores","td_drives",
    "quality_drives","three_and_outs","short_turnovers_leq3",
    "three_and_out_or_short_turnover","early_plays","early_successes", "rating_net",
    "net_epa_smooth", "sos", "n_plays_eff"
  )
  
  sum_only <- c(
    "sack_yards","passing_2pt_conversions",
    "rushing_fumbles","rushing_fumbles_lost","rushing_2pt_conversions",
    "receiving_fumbles","receiving_fumbles_lost","receiving_2pt_conversions",
    "def_tackles_for_loss_yards","def_fumbles_forced","def_sack_yards",
    "def_interception_yards","def_tds","def_fumbles",
    "def_fumble_recovery_own","def_fumble_recovery_yards_own",
    "def_fumble_recovery_opp","def_fumble_recovery_yards_opp",
    "def_safety","def_penalty","def_penalty_yards",
    "fg_att","fg_made","fg_missed","fg_blocked",
    "fg_made_0_19","fg_made_20_29","fg_made_30_39","fg_made_40_49","fg_made_50_59","fg_made_60",
    "fg_missed_0_19","fg_missed_20_29","fg_missed_30_39","fg_missed_40_49","fg_missed_50_59","fg_missed_60",
    "pat_att","pat_made","pat_missed","pat_blocked",
    "gwfg_att","gwfg_made","gwfg_missed","gwfg_blocked",
    "fg_made_distance","fg_missed_distance","fg_blocked_distance","gwfg_distance",
    # --- NEW: additive “totals” only ---
    "epa_total","wpa_total","early_epa_total","pass_oe_sum",
    "short_turnovers_raw","three_and_outs_raw"
  )
  
  avg_only <- c(
    # player-level rates/means
    "passing_epa","rushing_epa","receiving_epa",
    "pacr","dakota","racr","wopr","target_share","air_yards_share",
    "fg_pct","pat_pct","n_players",
    # --- NEW: team-game rates/means only ---
    "points_per_drive","epa_per_play","success_rate","explosive_rate",
    "pass_rate","rush_rate","sacks_per_drive","interceptions_per_drive",
    "red_zone_trip_rate","red_zone_score_rate","td_rate_per_drive",
    "quality_drive_rate","three_and_out_rate","short_turnover_leq3_rate",
    "three_and_out_or_short_turnover_rate","early_epa_per_play","early_success_rate",
    "pass_oe_mean","avg_start_yardline_100","avg_drive_depth_into_opp",
    "avg_drive_plays","avg_drive_time_seconds"
  )
  
  max_only <- c("fg_long","wins_entering","losses_entering","ties_entering")
  max_abs_only <- c("point_diff")
  
  # ---- rename identifiers ----
  weekly_df <- weekly_df %>%
    filter(stat_type == 'base') %>% 
    mutate(stat_name = recode(stat_name,
                              "wins_entering" = "wins",
                              "losses_entering" = "losses",
                              "ties_entering" = "ties"))
  
  # ---- SUM + AVG ----
  both_df <- weekly_df %>%
    filter(stat_name %in% sum_and_avg) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(sum = sum(value, na.rm = TRUE),
              avg = mean(value, na.rm = TRUE),
              .groups = "drop") %>%
    pivot_longer(cols = c(sum, avg), names_to = "stat_type", values_to = "value")
  
  # ---- SUM only ----
  sum_df <- weekly_df %>%
    filter(stat_name %in% sum_only) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(stat_type = "sum")
  
  # ---- AVG only ----
  avg_df <- weekly_df %>%
    filter(stat_name %in% avg_only) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(stat_type = "avg")
  
  # ---- MAX ----
  max_df <- weekly_df %>%
    filter(stat_name %in% max_only) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(value = suppressWarnings(max(value, na.rm = TRUE)), .groups = "drop") %>%
    mutate(stat_type = "max")
  
  # ---- MAX ABS ----
  max_abs_df <- weekly_df %>%
    filter(stat_name %in% max_abs_only) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(value = suppressWarnings(max(abs(value), na.rm = TRUE)), .groups = "drop") %>%
    mutate(stat_type = "max_abs")
  
  # ---- combine ----
  season_df <- bind_rows(both_df, sum_df, avg_df, max_df, max_abs_df)
  
  # ---- TOTAL season type ----
  total_df <- season_df %>%
    group_by(team, season, stat_name, stat_type) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(season_type = "TOTAL")
  
  # ---- REG + POST (normal) ----
  season_df <- bind_rows(both_df, sum_df, avg_df, max_df, max_abs_df)
  
  # ---- TOTAL computed directly from weekly data ----
  weekly_total <- weekly_df %>%
    mutate(season_type = "TOTAL")
  
  # Re-run the same pipelines on TOTAL
  both_total <- weekly_total %>%
    filter(stat_name %in% sum_and_avg) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(sum = sum(value, na.rm = TRUE),
              avg = mean(value, na.rm = TRUE),
              .groups = "drop") %>%
    tidyr::pivot_longer(cols = c(sum, avg), names_to = "stat_type", values_to = "value")
  
  sum_total <- weekly_total %>%
    filter(stat_name %in% sum_only) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(stat_type = "sum")
  
  avg_total <- weekly_total %>%
    filter(stat_name %in% avg_only) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(stat_type = "avg")
  
  max_total <- weekly_total %>%
    filter(stat_name %in% max_only) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(value = suppressWarnings(max(value, na.rm = TRUE)), .groups = "drop") %>%
    mutate(stat_type = "max")
  
  max_abs_total <- weekly_total %>%
    filter(stat_name %in% max_abs_only) %>%
    group_by(team, season, season_type, stat_name) %>%
    summarise(value = suppressWarnings(max(abs(value), na.rm = TRUE)), .groups = "drop") %>%
    mutate(stat_type = "max_abs")
  
  total_df <- bind_rows(both_total, sum_total, avg_total, max_total, max_abs_total)
  
  # ---- final ----
  final <- bind_rows(season_df, total_df) %>%
    arrange(team, season, season_type, stat_name, stat_type)
  
  final
}


#' Aggregate Weekly Team Stats to Season Level
#'
#' Aggregates weekly team stats into season-level totals and averages,
#' using predefined stat classification vectors. Handles REG, POST,
#' and TOTAL season types.
#'
#' @param weekly_df Long-format weekly stats with cols:
#'   team, season, season_type, week, stat_name, value
#'
#' @return Tibble with team, season, season_type, stat_name,
#'   stat_type (sum|avg|max|max_abs), value
#' @export
#'
aggregate_team_alltime_stats <- function(weekly_df) {
  library(dplyr)
  library(tidyr)
  
  # ---- classification vectors ----
  # ---- classification vectors ----
  sum_and_avg <- c(
    "completions","attempts","passing_yards","passing_tds","interceptions",
    "sacks","passing_air_yards","passing_yards_after_catch","passing_first_downs",
    "carries","rushing_yards","rushing_tds","rushing_first_downs",
    "receptions","targets","receiving_yards","receiving_tds",
    "receiving_air_yards","receiving_yards_after_catch","receiving_first_downs",
    "def_tackles","def_tackles_solo","def_tackle_assists","def_tackles_for_loss",
    "def_sacks","def_qb_hits","def_interceptions","def_pass_defended",
    "points_scored","points_allowed","weekly_injuries","injuries",
    # NEW (totals + per-game means both useful)
    "drives","plays_total","red_zone_trips","red_zone_scores","td_drives",
    "quality_drives","three_and_outs","short_turnovers_leq3",
    "three_and_out_or_short_turnover","early_plays","early_successes", "rating_net",
    "net_epa_smooth", "sos", "n_plays_eff"
  )
  
  sum_only <- c(
    "sack_yards","passing_2pt_conversions",
    "rushing_fumbles","rushing_fumbles_lost","rushing_2pt_conversions",
    "receiving_fumbles","receiving_fumbles_lost","receiving_2pt_conversions",
    "def_tackles_for_loss_yards","def_fumbles_forced","def_sack_yards",
    "def_interception_yards","def_tds","def_fumbles",
    "def_fumble_recovery_own","def_fumble_recovery_yards_own",
    "def_fumble_recovery_opp","def_fumble_recovery_yards_opp",
    "def_safety","def_penalty","def_penalty_yards",
    "fg_att","fg_made","fg_missed","fg_blocked",
    "fg_made_0_19","fg_made_20_29","fg_made_30_39","fg_made_40_49","fg_made_50_59","fg_made_60",
    "fg_missed_0_19","fg_missed_20_29","fg_missed_30_39","fg_missed_40_49","fg_missed_50_59","fg_missed_60",
    "pat_att","pat_made","pat_missed","pat_blocked",
    "gwfg_att","gwfg_made","gwfg_missed","gwfg_blocked",
    "fg_made_distance","fg_missed_distance","fg_blocked_distance","gwfg_distance",
    # NEW (purely additive)
    "epa_total","wpa_total","early_epa_total","pass_oe_sum",
    "short_turnovers_raw","three_and_outs_raw"
  )
  
  avg_only <- c(
    "passing_epa","rushing_epa","receiving_epa",
    "pacr","dakota","racr","wopr","target_share","air_yards_share",
    "fg_pct","pat_pct","n_players",
    # NEW (rates/means)
    "points_per_drive","epa_per_play","success_rate","explosive_rate",
    "pass_rate","rush_rate","sacks_per_drive","interceptions_per_drive",
    "red_zone_trip_rate","red_zone_score_rate","td_rate_per_drive","quality_drive_rate",
    "three_and_out_rate","short_turnover_leq3_rate","three_and_out_or_short_turnover_rate",
    "early_epa_per_play","early_success_rate","pass_oe_mean",
    "avg_start_yardline_100","avg_drive_depth_into_opp","avg_drive_plays","avg_drive_time_seconds"
  )
  
  max_only <- c("fg_long","wins_entering","losses_entering","ties_entering")
  max_abs_only <- c("point_diff")
  
  # ---- rename identifiers ----
  weekly_df <- weekly_df %>%
    filter(stat_type == 'base') %>% 
    mutate(stat_name = recode(stat_name,
                              "wins_entering" = "wins",
                              "losses_entering" = "losses",
                              "ties_entering" = "ties"))
  
  # ---- SUM + AVG ----
  both_df <- weekly_df %>%
    filter(stat_name %in% sum_and_avg) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(sum = sum(value, na.rm = TRUE),
              avg = mean(value, na.rm = TRUE),
              .groups = "drop") %>%
    pivot_longer(cols = c(sum, avg), names_to = "stat_type", values_to = "value")
  
  # ---- SUM only ----
  sum_df <- weekly_df %>%
    filter(stat_name %in% sum_only) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(stat_type = "sum")
  
  # ---- AVG only ----
  avg_df <- weekly_df %>%
    filter(stat_name %in% avg_only) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(stat_type = "avg")
  
  # ---- MAX ----
  max_df <- weekly_df %>%
    filter(stat_name %in% max_only) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(value = suppressWarnings(max(value, na.rm = TRUE)), .groups = "drop") %>%
    mutate(stat_type = "max")
  
  # ---- MAX ABS ----
  max_abs_df <- weekly_df %>%
    filter(stat_name %in% max_abs_only) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(value = suppressWarnings(max(abs(value), na.rm = TRUE)), .groups = "drop") %>%
    mutate(stat_type = "max_abs")
  
  # ---- REG + POST (normal) ----
  season_df <- bind_rows(both_df, sum_df, avg_df, max_df, max_abs_df)
  
  # ---- TOTAL computed directly from weekly data ----
  weekly_total <- weekly_df %>%
    mutate(season_type = "TOTAL")
  
  # Re-run the same pipelines on TOTAL
  both_total <- weekly_total %>%
    filter(stat_name %in% sum_and_avg) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(sum = sum(value, na.rm = TRUE),
              avg = mean(value, na.rm = TRUE),
              .groups = "drop") %>%
    tidyr::pivot_longer(cols = c(sum, avg), names_to = "stat_type", values_to = "value")
  
  sum_total <- weekly_total %>%
    filter(stat_name %in% sum_only) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(stat_type = "sum")
  
  avg_total <- weekly_total %>%
    filter(stat_name %in% avg_only) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(stat_type = "avg")
  
  max_total <- weekly_total %>%
    filter(stat_name %in% max_only) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(value = suppressWarnings(max(value, na.rm = TRUE)), .groups = "drop") %>%
    mutate(stat_type = "max")
  
  max_abs_total <- weekly_total %>%
    filter(stat_name %in% max_abs_only) %>%
    group_by(team, season_type, stat_name) %>%
    summarise(value = suppressWarnings(max(abs(value), na.rm = TRUE)), .groups = "drop") %>%
    mutate(stat_type = "max_abs")
  
  total_df <- bind_rows(both_total, sum_total, avg_total, max_total, max_abs_total)
  
  # ---- final ----
  final <- bind_rows(season_df, total_df) %>%
    arrange(team, season_type, stat_name, stat_type)
  
  final
}

#' Derive defensive "allowed" stats from opponent offense (use `opponent` column)
#'
#' @description
#' For each offensive stat row, create a corresponding defensive-allowed row for
#' the opponent (swap `team` and `opponent`, keep `value`), then append it to the
#' original long table. Uses the provided `opponent` column (no `game_id` parsing).
#' Duplicates are prevented via an anti-join on exact keys. Offense rows are never
#' removed or overwritten.
#'
#' @param df Long tibble/data.frame with columns:
#'   `team, season, season_type, week, opponent, stat_type, stat_name, value, game_id`.
#' @param stat_type_keep Which `stat_type` to transform (default "base").
#'
#' @return A tibble with original rows **plus** derived defensive-allowed rows,
#'   sorted by `season, week, team, stat_name`.
#'
#' @details
#' Mapping (offense → defense-allowed):
#' - `passing_yards`            → `def_passing_yards_allowed`
#' - `passing_tds`              → `def_passing_tds_allowed`
#' - `passing_first_downs`      → `def_passing_first_downs_allowed`
#' - `passing_epa`              → `def_pass_epa_allowed`
#' - `avg_drive_depth_into_opp` → `def_avg_drive_depth_allow`
#' - `drives`                   → `def_drives_allowed`
#' - `carries`                  → `def_carries_allowed`
#' - `rushing_yards`            → `def_rushing_yards_allowed`
#' - `rushing_tds`              → `def_rushing_tds_allowed`
#' - `rushing_first_downs`      → `def_rushing_first_downs_allowed`
#' - `rushing_epa`              → `def_rushing_epa_allowed`
#'
#' @importFrom dplyr filter transmute select distinct anti_join bind_rows arrange mutate
#' @export
derive_defense_allowed_stats <- function(
    df,
    stat_type_keep = "base"
) {
  # ---- Column checks ----
  needed <- c("team","season","season_type","week","opponent",
              "stat_type","stat_name","value","game_id")
  miss <- setdiff(needed, names(df))
  if (length(miss) > 0) stop(sprintf("Missing required columns: %s", paste(miss, collapse = ", ")))
  
  # ---- Offense → Defense-Allowed map ----
  stat_map <- c(
    "passing_yards"            = "def_passing_yards_allowed",
    "passing_tds"              = "def_passing_tds_allowed",
    "passing_first_downs"      = "def_passing_first_downs_allowed",
    "passing_epa"              = "def_pass_epa_allowed",
    "avg_drive_depth_into_opp" = "def_avg_drive_depth_allow",
    "drives"                   = "def_drives_allowed",
    "carries"                  = "def_carries_allowed",
    "rushing_yards"            = "def_rushing_yards_allowed",
    "rushing_tds"              = "def_rushing_tds_allowed",
    "rushing_first_downs"      = "def_rushing_first_downs_allowed",
    "rushing_epa"              = "def_rushing_epa_allowed"
  )
  
  present_off <- intersect(names(stat_map), unique(df$stat_name))
  if (length(present_off) == 0L) {
    return(df |> dplyr::arrange(.data$season, .data$week, .data$team, .data$stat_name))
  }
  
  # ---- Build mirrored rows using the opponent column ----
  src <- df |>
    dplyr::filter(
      .data$stat_type == stat_type_keep,
      .data$stat_name %in% present_off,
      !is.na(.data$opponent), .data$opponent != "",
      .data$team != .data$opponent
    )
  
  def_allowed <- src |>
    dplyr::transmute(
      team        = .data$opponent,              # mirror to opponent
      season      = .data$season,
      season_type = .data$season_type,           # kept for lineage
      week        = .data$week,
      opponent    = .data$team,                  # original team becomes opponent
      stat_type   = .data$stat_type,             # "base"
      stat_name   = unname(stat_map[.data$stat_name]),
      value       = .data$value,
      game_id     = .data$game_id
    )
  
  # ---- Prevent duplicates (exact key) ----
  key_cols <- c("season","week","team","stat_type","stat_name","game_id")
  existing_keys <- df |>
    dplyr::select(dplyr::all_of(key_cols)) |>
    dplyr::distinct()
  
  def_allowed_new <- def_allowed |>
    dplyr::anti_join(existing_keys, by = key_cols) |>
    dplyr::filter(.data$team != .data$opponent)   # final guardrail
  
  # ---- Append & sort ----
  dplyr::bind_rows(df, def_allowed_new) |>
    dplyr::arrange(.data$season, .data$week, .data$team, .data$stat_name)
}
