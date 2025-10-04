#' Pivot player statistics (Parquet) to long format
#'
#' Reads a Parquet file of wide player statistics and reshapes it
#' into a tidy long format, distinguishing base vs cumulative stats.
#'
#' @param file_path Character string; path to the Parquet file containing
#'   player-level statistics in wide format.
#'
#' @return A tibble with columns:
#'   \itemize{
#'     \item \code{player_id} Player unique ID
#'     \item \code{full_name} Player name
#'     \item \code{position} Position (QB, RB, WR, etc.)
#'     \item \code{season} Season year
#'     \item \code{season_type} Regular or Postseason
#'     \item \code{week} Week number
#'     \item \code{team} Recent team abbreviation
#'     \item \code{opponent} Opponent team abbreviation
#'     \item \code{stat_type} "base" or "cumulative"
#'     \item \code{stat_name} Statistic name
#'     \item \code{value} Statistic value (numeric)
#'   }
#'
#' @examples
#' \dontrun{
#' tidy_stats <- pivot_player_stats_long("data/player_weekly.parquet")
#' }
#'
#' @importFrom arrow read_parquet
#' @importFrom dplyr mutate select rename all_of if_else
#' @importFrom tidyr pivot_longer
#' @export
pivot_player_stats_long <- function(file_path="", data=NULL) {
  # read parquet file
  if(file_path != '') {
    df <- arrow::read_parquet(file_path)
  } else {
    df <- data
  }
  
  id_cols <- c(
    "player_id", "full_name", "position",
    "season", "season_type", "week",
    "recent_team", "opponent_team"
  )
  
  # check for missing identifier columns
  missing_cols <- setdiff(id_cols, names(df))
  if (length(missing_cols) > 0) {
    stop(
      sprintf(
        "Missing required columns in %s: %s",
        file_path, paste(missing_cols, collapse = ", ")
      )
    )
  }
  
  df_long <- df %>%
    tidyr::pivot_longer(
      cols = -dplyr::all_of(id_cols),
      names_to = "stat_name",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      stat_type = dplyr::if_else(
        grepl("^cumulative_", stat_name),
        "cumulative",
        "base"
      ),
      stat_name = gsub("^cumulative_", "", stat_name)
    ) %>%
    dplyr::rename(
      team = recent_team,
      opponent = opponent_team
    ) %>%
    dplyr::select(
      player_id, name = full_name, position,
      season, season_type, week,
      team, opponent,
      stat_type, stat_name, value
    )
  
  return(df_long)
}

#' Pivot Next Gen Stats player data to long format
#'
#' Reads a Parquet file of wide Next Gen Stats (NGS) player data and reshapes
#' into tidy long format. Joins in opponent info from another dataset and
#' prefixes all stat names with "ng_".
#'
#' @param file_path Character string; path to the Parquet file containing
#'   Next Gen Stats player-level data in wide format.
#' @param opponent_df A data.frame/tibble containing opponent information,
#'   with columns \code{season}, \code{season_type}, \code{week},
#'   \code{player_id}, and \code{opponent}.
#'
#' @return A tibble with columns:
#'   \itemize{
#'     \item \code{player_id} Player unique ID
#'     \item \code{full_name} Player name
#'     \item \code{position} Position (QB, WR, etc.)
#'     \item \code{season} Season year
#'     \item \code{season_type} Regular or Postseason
#'     \item \code{week} Week number
#'     \item \code{team} Recent team abbreviation
#'     \item \code{opponent} Opponent team abbreviation
#'     \item \code{stat_type} "base" or "cumulative"
#'     \item \code{stat_name} Statistic name (prefixed with "ng_")
#'     \item \code{value} Statistic value (numeric)
#'   }
#'
#' @examples
#' \dontrun{
#' tidy_ngs <- pivot_ngs_player_stats_long(
#'   file_path = "data/ngs_qb_weekly.parquet",
#'   opponent_df = qb_weekly_long
#' )
#' }
#'
#' @importFrom arrow read_parquet
#' @importFrom dplyr mutate select rename left_join all_of if_else
#' @importFrom tidyr pivot_longer
#' @export
pivot_ngs_player_stats_long <- function(file_path='', data = NULL, opponent_df) {
  # read parquet
  # read parquet file
  if(file_path != '') {
    df <- arrow::read_parquet(file_path)
  } else {
    df <- data
  }
  
  # normalize identifier names
  df <- df %>%
    dplyr::rename(
      player_id = player_gsis_id,
      position = player_position,
      team = team_abbr
    )
  
  id_cols <- c(
    "player_id", "full_name", "position",
    "season", "season_type", "week", "team"
  )
  
  # join opponent info
  df <- df %>%
    dplyr::left_join(
      opponent_df %>%
        dplyr::select(season, season_type, week, player_id, opponent),
      by = c("season", "season_type", "week", "player_id")
    )
  
  # pivot stats long
  df_long <- df %>%
    tidyr::pivot_longer(
      cols = -dplyr::all_of(c(id_cols, "opponent")),
      names_to = "stat_name",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      stat_type = dplyr::if_else(
        grepl("^cumulative_", stat_name),
        "cumulative",
        "base"
      ),
      stat_name = gsub("^cumulative_", "", stat_name),
      stat_name = paste0("ng_", stat_name)
    ) %>%
    dplyr::select(
      player_id, name = full_name, position,
      season, season_type, week,
      team, opponent,
      stat_type, stat_name, value
    )
  
  return(df_long)
}

#' Pivot defensive player statistics to long format
#'
#' Reads a Parquet file of wide defensive player statistics and reshapes
#' into tidy long format. Joins in opponent info at the team level.
#'
#' @param file_path Character string; path to the Parquet file containing
#'   defensive player statistics in wide format.
#' @param opponent_df A data.frame/tibble with columns
#'   \code{season}, \code{season_type}, \code{week}, \code{team}, and \code{opponent}.
#'
#' @return A tibble with columns:
#'   \itemize{
#'     \item \code{player_id} Player unique ID
#'     \item \code{player_name} Player name
#'     \item \code{position} Position (e.g., CB, LB, DL)
#'     \item \code{position_group} Position group (e.g., DB, LB, DL)
#'     \item \code{season} Season year
#'     \item \code{season_type} Regular or Postseason
#'     \item \code{week} Week number
#'     \item \code{team} Player’s team abbreviation
#'     \item \code{opponent} Opponent team abbreviation
#'     \item \code{stat_type} "base" or "cumulative"
#'     \item \code{stat_name} Statistic name (without "cumulative_" prefix)
#'     \item \code{value} Statistic value (numeric)
#'   }
#'
#' @examples
#' \dontrun{
#' tidy_def <- pivot_def_player_stats_long(
#'   file_path = "data/defense_player_weekly.parquet",
#'   opponent_df = weekly_opponents
#' )
#' }
#'
#' @importFrom arrow read_parquet
#' @importFrom dplyr mutate select left_join all_of if_else
#' @importFrom tidyr pivot_longer
#' @export
pivot_def_player_stats_long <- function(file_path='', data=NULL, opponent_df) {
  # read parquet file
  # read parquet file
  if(file_path != '') {
    df <- arrow::read_parquet(file_path)
  } else {
    df <- data
  }
  
  id_cols <- c(
    "player_id", "player_name", "position", "position_group",
    "season", "season_type", "week", "team"
  )
  
  # join opponent info
  df <- df %>%
    dplyr::left_join(
      opponent_df,
      by = c("season", "season_type", "week", "team")
    )
  
  # pivot stats long
  df_long <- df %>%
    tidyr::pivot_longer(
      cols = -dplyr::all_of(c(id_cols, "opponent")),
      names_to = "stat_name",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      stat_type = dplyr::if_else(
        grepl("^cumulative_", stat_name),
        "cumulative",
        "base"
      ),
      stat_name = gsub("^cumulative_", "", stat_name)
    ) %>%
    dplyr::select(
      player_id, name = player_name, position,
      season, season_type, week,
      team, opponent,
      stat_type, stat_name, value
    )
  
  return(df_long)
}

#' Aggregate weekly player stats to season level (REG, POST, TOTAL)
#'
#' This function aggregates weekly long-format player stats into season-level
#' summaries. It produces separate Regular (REG), Postseason (POST), and TOTAL
#' season aggregates. Each stat is aggregated according to predefined rules:
#' some are summed, some averaged, and some both.
#'
#' @param weekly_df A tibble or data.frame in long format, with at least the
#'   following columns: \code{player_id}, \code{name}, \code{position},
#'   \code{season}, \code{season_type}, \code{week}, \code{stat_type},
#'   \code{stat_name}, \code{value}.
#'
#' @return A tibble with columns:
#'   \itemize{
#'     \item \code{player_id} Player unique ID
#'     \item \code{name} Player name
#'     \item \code{position} Player position
#'     \item \code{season} Season year
#'     \item \code{season_type} REG, POST, or TOTAL
#'     \item \code{agg_type} "sum" or "average"
#'     \item \code{stat_name} Statistic name
#'     \item \code{value} Aggregated value
#'   }
#'
#' @examples
#' \dontrun{
#' season_stats <- create_season_stats(weekly_player_long)
#' }
#'
#' @importFrom dplyr filter group_by summarise bind_rows mutate select
#' @export
create_season_stats <- function(weekly_df) {
  # mapping table: stat_name → aggregation rule
  # Averages only (rates/means; summing would be nonsense)
  avg_only <- c(
    # PBP game/team rates
    "points_per_drive","epa_per_play","success_rate","explosive_rate",
    "pass_rate","rush_rate","sacks_per_drive","interceptions_per_drive",
    "red_zone_trip_rate","red_zone_score_rate","td_rate_per_drive",
    "three_and_out_rate","short_turnover_leq3_rate","three_and_out_or_short_turnover_rate",
    "early_epa_per_play","early_success_rate","pass_oe_mean",
    # positional + advanced player means
    "passing_epa","rushing_epa","receiving_epa","pacr","dakota","racr",
    "target_share","air_yards_share","wopr",
    # next-gen passer/receiver means
    "ng_avg_time_to_throw","ng_avg_completed_air_yards","ng_avg_intended_air_yards",
    "ng_avg_air_yards_differential","ng_aggressiveness","ng_max_completed_air_distance",
    "ng_avg_air_yards_to_sticks","ng_passer_rating","ng_completion_percentage",
    "ng_expected_completion_percentage","ng_completion_percentage_above_expectation",
    "ng_avg_air_distance","ng_max_air_distance",
    # useful field/tempo means from game context
    "avg_start_yardline_100","avg_drive_depth_into_opp",
    "avg_drive_plays","avg_drive_time_seconds"
  )
  
  # Both total and mean (volumes where both season totals and per-game means are handy)
  sum_and_avg <- c(
    # your original player volumes
    "completions","attempts","passing_yards","passing_tds","interceptions",
    "sacks","sack_yards","sack_fumbles","sack_fumbles_lost","passing_air_yards",
    "passing_yards_after_catch","passing_first_downs","passing_2pt_conversions",
    "carries","rushing_yards","rushing_tds","rushing_fumbles","rushing_fumbles_lost",
    "rushing_first_downs","rushing_2pt_conversions","fantasy_points","fantasy_points_ppr",
    "ng_attempts","ng_pass_yards","ng_pass_touchdowns","ng_interceptions","ng_completions",
    "targets","receptions","receiving_yards","receiving_tds","receiving_fumbles",
    "receiving_fumbles_lost","receiving_air_yards","receiving_yards_after_catch",
    "receiving_first_downs","receiving_2pt_conversions",
    "def_tackles","def_tackles_solo","def_tackle_assists","def_tackles_for_loss",
    "def_tackles_for_loss_yards","def_fumbles_forced","def_sacks","def_sack_yards",
    "def_qb_hits","def_interceptions","def_interception_yards","def_pass_defended",
    "def_tds","def_fumbles","def_fumble_recovery_own","def_fumble_recovery_yards_own",
    "def_fumble_recovery_opp","def_fumble_recovery_yards_opp","def_safety",
    "def_penalty","def_penalty_yards",
    # team/game PBP volumes
    "drives","plays_total","points_scored","red_zone_trips","red_zone_scores",
    "td_drives","three_and_outs","short_turnovers_leq3","three_and_out_or_short_turnover",
    "early_plays","early_successes",
    # raw additive EPA/WPA sums
    "epa_total","wpa_total","early_epa_total","pass_oe_sum"
  )
  
  # filter base stats only
  df <- dplyr::filter(weekly_df, stat_type == "base")
  
  # --- SUM + AVERAGE by REG/POST ---
  df_sum <- df %>%
    dplyr::filter(stat_name %in% sum_and_avg) %>%
    dplyr::group_by(player_id, name, position, season, season_type, stat_name) %>%
    dplyr::summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(agg_type = "sum")
  
  df_avg <- df %>%
    dplyr::filter(stat_name %in% c(sum_and_avg, avg_only)) %>%
    dplyr::group_by(player_id, name, position, season, season_type, stat_name) %>%
    dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(agg_type = "average")
  
  # --- TOTAL across REG + POST ---
  df_total_sum <- df %>%
    dplyr::filter(stat_name %in% sum_and_avg) %>%
    dplyr::group_by(player_id, name, position, season, stat_name) %>%
    dplyr::summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(agg_type = "sum", season_type = "TOTAL")
  
  df_total_avg <- df %>%
    dplyr::filter(stat_name %in% c(sum_and_avg, avg_only)) %>%
    dplyr::group_by(player_id, name, position, season, stat_name) %>%
    dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(agg_type = "average", season_type = "TOTAL")
  
  # combine all
  out <- dplyr::bind_rows(df_sum, df_avg, df_total_sum, df_total_avg)
  
  return(out)
}

#' Aggregate weekly player stats to career level (REG, POST, TOTAL)
#'
#' This function aggregates weekly long-format player stats into season-level
#' summaries. It produces separate Regular (REG), Postseason (POST), and TOTAL
#' season aggregates. Each stat is aggregated according to predefined rules:
#' some are summed, some averaged, and some both.
#'
#' @param weekly_df A tibble or data.frame in long format, with at least the
#'   following columns: \code{player_id}, \code{name}, \code{position},
#'   \code{season_type}, \code{week}, \code{stat_type},
#'   \code{stat_name}, \code{value}.
#'
#' @return A tibble with columns:
#'   \itemize{
#'     \item \code{player_id} Player unique ID
#'     \item \code{name} Player name
#'     \item \code{position} Player position
#'     \item \code{season_type} REG, POST, or TOTAL
#'     \item \code{agg_type} "sum" or "average"
#'     \item \code{stat_name} Statistic name
#'     \item \code{value} Aggregated value
#'   }
#'
#' @examples
#' \dontrun{
#' season_stats <- create_career_stats(weekly_player_long)
#' }
#'
#' @importFrom dplyr filter group_by summarise bind_rows mutate select
#' @export
create_career_stats <- function(weekly_df) {
  # mapping table: stat_name → aggregation rule
  avg_only <- c(
    # PBP game/team rates
    "points_per_drive","epa_per_play","success_rate","explosive_rate",
    "pass_rate","rush_rate","sacks_per_drive","interceptions_per_drive",
    "red_zone_trip_rate","red_zone_score_rate","td_rate_per_drive",
    "three_and_out_rate","short_turnover_leq3_rate","three_and_out_or_short_turnover_rate",
    "early_epa_per_play","early_success_rate","pass_oe_mean",
    # positional + advanced player means
    "passing_epa","rushing_epa","receiving_epa","pacr","dakota","racr",
    "target_share","air_yards_share","wopr",
    # next-gen passer/receiver means
    "ng_avg_time_to_throw","ng_avg_completed_air_yards","ng_avg_intended_air_yards",
    "ng_avg_air_yards_differential","ng_aggressiveness","ng_max_completed_air_distance",
    "ng_avg_air_yards_to_sticks","ng_passer_rating","ng_completion_percentage",
    "ng_expected_completion_percentage","ng_completion_percentage_above_expectation",
    "ng_avg_air_distance","ng_max_air_distance",
    # useful field/tempo means from game context
    "avg_start_yardline_100","avg_drive_depth_into_opp",
    "avg_drive_plays","avg_drive_time_seconds"
  )
  
  # Both total and mean (volumes where both season totals and per-game means are handy)
  sum_and_avg <- c(
    # your original player volumes
    "completions","attempts","passing_yards","passing_tds","interceptions",
    "sacks","sack_yards","sack_fumbles","sack_fumbles_lost","passing_air_yards",
    "passing_yards_after_catch","passing_first_downs","passing_2pt_conversions",
    "carries","rushing_yards","rushing_tds","rushing_fumbles","rushing_fumbles_lost",
    "rushing_first_downs","rushing_2pt_conversions","fantasy_points","fantasy_points_ppr",
    "ng_attempts","ng_pass_yards","ng_pass_touchdowns","ng_interceptions","ng_completions",
    "targets","receptions","receiving_yards","receiving_tds","receiving_fumbles",
    "receiving_fumbles_lost","receiving_air_yards","receiving_yards_after_catch",
    "receiving_first_downs","receiving_2pt_conversions",
    "def_tackles","def_tackles_solo","def_tackle_assists","def_tackles_for_loss",
    "def_tackles_for_loss_yards","def_fumbles_forced","def_sacks","def_sack_yards",
    "def_qb_hits","def_interceptions","def_interception_yards","def_pass_defended",
    "def_tds","def_fumbles","def_fumble_recovery_own","def_fumble_recovery_yards_own",
    "def_fumble_recovery_opp","def_fumble_recovery_yards_opp","def_safety",
    "def_penalty","def_penalty_yards",
    # team/game PBP volumes
    "drives","plays_total","points_scored","red_zone_trips","red_zone_scores",
    "td_drives","three_and_outs","short_turnovers_leq3","three_and_out_or_short_turnover",
    "early_plays","early_successes",
    # raw additive EPA/WPA sums
    "epa_total","wpa_total","early_epa_total","pass_oe_sum"
  )
  
  # filter base stats only
  df <- dplyr::filter(weekly_df, stat_type == "base")
  
  # --- SUM + AVERAGE by REG/POST ---
  df_sum <- df %>%
    dplyr::filter(stat_name %in% sum_and_avg) %>%
    dplyr::group_by(player_id, name, position, season_type, stat_name) %>%
    dplyr::summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(agg_type = "sum")
  
  df_avg <- df %>%
    dplyr::filter(stat_name %in% c(sum_and_avg, avg_only)) %>%
    dplyr::group_by(player_id, name, position, season_type, stat_name) %>%
    dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(agg_type = "average")
  
  # --- TOTAL across REG + POST ---
  df_total_sum <- df %>%
    dplyr::filter(stat_name %in% sum_and_avg) %>%
    dplyr::group_by(player_id, name, position, stat_name) %>%
    dplyr::summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(agg_type = "sum", season_type = "TOTAL")
  
  df_total_avg <- df %>%
    dplyr::filter(stat_name %in% c(sum_and_avg, avg_only)) %>%
    dplyr::group_by(player_id, name, position, stat_name) %>%
    dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(agg_type = "average", season_type = "TOTAL")
  
  # combine all
  out <- dplyr::bind_rows(df_sum, df_avg, df_total_sum, df_total_avg)
  
  return(out)
}

#' Pivot PBP team-game features to long format
#'
#' Reads a wide team-game Parquet and reshapes to tidy long format.
#' - Parses season/week from game_id (e.g., "1999_01_ARI_PHI")
#' - Derives season_type = "REG" (week <= 17) else "POST"
#' - Joins opponent by pairing teams within each game_id
#' - Pivots all numeric feature columns to (stat_name, value)
#'
#' @param input_path Path to the team-game wide Parquet (e.g., "data/staging/pbp_games.parquet")
#' @return Long tibble with columns:
#'   season, season_type, week, team, opponent, stat_type="base", stat_name, value
#' @export
pivot_pbp_game_stats_long <- function(input_path='', data=NULL) {
  # read parquet file
  if(input_path != '') {
    df <- arrow::read_parquet(input_path)
  } else {
    df <- data
  }
  
  # Ensure expected id cols exist
  if (!all(c("game_id", "posteam") %in% names(df))) {
    stop("Expected columns `game_id` and `posteam` not found.")
  }
  
  # --- derive season/week/season_type from game_id ---
  # game_id like "1999_01_ARI_PHI"
  parts <- strsplit(df$game_id, "_", fixed = TRUE)
  season_chr <- vapply(parts, `[[`, character(1), 1)
  week_chr   <- vapply(parts, `[[`, character(1), 2)
  
  # basic numeric week parse; keep it KISS
  week_num <- suppressWarnings(as.integer(week_chr))
  
  df <- df %>%
    dplyr::mutate(
      season      = as.integer(season_chr),
      week        = week_num,
      season_type = dplyr::if_else(!is.na(week) & week <= 17L, "REG", "POST")
    )
  
  # --- opponent pairing inside each game_id ---
  team_pairs <- df %>%
    dplyr::distinct(game_id, team = posteam) %>%
    dplyr::inner_join(., ., by = "game_id", suffix = c("", "_opp")) %>%
    dplyr::filter(team != team_opp) %>%
    dplyr::transmute(game_id, team, opponent = team_opp) %>%
    dplyr::distinct()
  
  df2 <- df %>%
    dplyr::rename(team = posteam) %>%
    dplyr::left_join(team_pairs, by = c("game_id","team"))
  
  # --- choose columns to pivot: all numeric stats; keep id columns out ---
  id_cols <- c("game_id", "season", "season_type", "week", "team", "opponent")
  stat_cols <- df2 %>%
    dplyr::select(-dplyr::all_of(id_cols)) %>%
    dplyr::select(where(is.numeric)) %>%
    names()
  
  if (length(stat_cols) == 0) {
    stop("No numeric stat columns found to pivot.")
  }
  
  out <- df2 %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(stat_cols),
      names_to = "stat_name",
      values_to = "value"
    ) %>%
    dplyr::transmute(
      season       = as.integer(season),
      season_type  = as.character(season_type),
      week         = as.integer(week),
      team         = as.character(team),
      opponent     = as.character(opponent),
      stat_type    = "base",
      stat_name    = as.character(stat_name),
      value        = as.numeric(value)
    ) %>%
    dplyr::arrange(season, week, team, stat_name)
  
  out
}

