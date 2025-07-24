# step2_depth_charts_process_functions.R

library(dplyr)
library(tidyr)

#' Filter to only players listed as depth_position 1 (i.e., starters)
#' @param df Raw depth chart data
#' @return Filtered tibble of starters only
filter_depth_chart_starters <- function(df) {
  df %>%
    filter(as.integer(depth_team) == 1) %>%  # Corrected
    mutate(
      season = as.integer(season),
      week = as.integer(week),
      team = toupper(club_code),
      player = full_name,
      position = toupper(depth_position)
    ) %>%
    select(season, week, team, player, position, gsis_id)
}

#' Compute cumulative QB start statistics by season, week, and team
#' @param df Starter-only depth chart data
#' @return DataFrame with total QB starts and distinct QBs through each week
get_qb_start_stats <- function(df) {
  df %>%
    filter(position == "QB") %>%
    arrange(season, team, week) %>%
    group_by(season, team, week) %>%
    summarise(qb_starter = unique(player), .groups = "drop") %>%
    group_by(season, team) %>%
    arrange(week) %>%
    mutate(
      total_qb_starts = row_number(),  # total weekly starts
      distinct_qb_starters = cumdistinct(qb_starter)
    ) %>%
    select(season, week, team, distinct_qb_starters) %>% 
    arrange(season, week, team)
}

# Helper: running count of distinct values
cumdistinct <- function(x) {
  purrr::accumulate(x, ~ if (.y %in% .x) .x else c(.x, .y)) %>% lengths()
}

#' Compute player-level total starts across positions/seasons
#' @param df Starter-only depth chart data
#' @return DataFrame with number of starts by player/position/season/team
get_player_start_totals <- function(df) {
  df %>% 
    group_by(season, team, position, player) %>% 
    summarise(
      total_starts = n(),
      .groups = "drop"
    )
}

#' Detect in-season starter changes (new starters by team/position/week)
#' @param df Starter-only depth chart data
#' @return DataFrame with flags for new starters by team-position-week
get_starter_switches <- function(df) {
  df %>% 
    arrange(season, week) %>% 
    group_by(season, team, position) %>% 
    mutate(
      is_new_starter = player != lag(player)
    ) %>% 
    ungroup()
}

#' Identify players who became new starters (were not starters the previous week)
#' @param starters_df Cleaned starters-only dataframe
#' @return DataFrame of promotions with week flagged
get_inseason_promotions <- function(starters_df) {
  starters_df %>%
    arrange(season, team, position, week) %>%
    group_by(season, team, position) %>%
    mutate(
      was_starter_last_week = lag(player),
      promoted_to_starter = player != was_starter_last_week
    ) %>%
    ungroup() %>%
    filter(promoted_to_starter)
}

clean_position <- function(pos) {
  pos_clean <- stringr::str_trim(pos) |> stringr::str_to_upper()
  
  dplyr::case_when(
    # Offensive Line
    pos_clean %in% c("LT", "LG", "C", "RG", "RT", "OL", "LOT", "ROT", "OC", "T", "G") ~ "OL",
    
    # Wide Receiver
    pos_clean %in% c("WR", "WR1", "WR2", "WR\\8", "LWR", "RWR", "WRE") ~ "WR",
    
    # Tight End
    pos_clean %in% c("TE", "TE\\N", "LTE", "RTE", "HB-TE") ~ "TE",
    
    # Quarterback
    pos_clean %in% c("QB", "JACK") ~ "QB",
    
    # Running Back & Fullback
    pos_clean %in% c("RB", "FB", "HB", "H-B", "RB86", "F", "H") ~ "RB",
    
    # Defensive Line
    pos_clean %in% c(
      "DL", "DE", "DT", "NT", "LDE", "RDE", "RDT", "LDT", "DL44",
      "LE", "RE", "EDGE", "DPR", "NDT", "RUSH"
    ) ~ "DL",
    
    # Linebackers
    pos_clean %in% c(
      "LB", "ILB", "OLB", "MLB", "WILL", "SAM", "MIKE", "SLB", "WLB",
      "ROLB", "LOLB", "LILB", "RILB", "0LB", "$LB", "MIL", "\nMLB", "WIL", "LEO",
      "LILBI"
    ) ~ "LB",
    
    # Cornerbacks
    pos_clean %in% c(
      "CB", "LCB", "RCB", "NB", "NICKE", "NKL", "NICK", "NDB", "MCB", "NCB", "LCR"
    ) ~ "CB",
    
    # Safeties
    pos_clean %in% c("S", "SS", "FS", "WS", "S47") ~ "S",
    
    # Kickers & Punters & LS
    pos_clean %in% c("K", "P", "PK", "KO", "K/KO", "KOS", "P/H", "PH", "PF", "HLS", "LS") ~ "K",
    
    # Returners (map to ST)
    pos_clean %in% c("KR", "KOR", "PR", "RS") ~ "ST",
    
    # Special Teams, Long Snappers, etc.
    pos_clean %in% c("ST", "UT", "PK", "KOS", "P.", "OC") ~ "ST",
    
    # Unknown or ambiguous
    pos_clean %in% c("", "\n", "19", "21", "6", "J", "N", "DL/OL", "OTTO", 
                     "7") ~ "UNK",
    
    # Default: keep as is for manual follow-up
    TRUE ~ pos_clean
  )
}


#' Compute weekly depth chart stability by position/team
#' @param df Starter-only depth chart data (with position_clean)
#' @return Table with weekly top starter share by team/position
get_lineup_stability_by_week <- function(starters_df) {
  starters_df %>%
    group_by(season, week, team, player, position_clean, position_group) %>%
    summarise(
      has_started = 1L,
      .groups = "drop"
    ) %>%
    # Cumulative number of weeks each player has started up to each week
    group_by(season, team, player, position_clean, position_group) %>%
    arrange(week) %>%
    mutate(weeks_started_to_date = cumsum(has_started)) %>%
    ungroup() %>%
    
    # Focus on lineup for that week
    group_by(season, week, team, position_group) %>%
    summarise(
      total_starts_to_date = sum(weeks_started_to_date),
      num_starters_this_week = n(),
      max_possible_starts = n() * week,
      lineup_stability_score = total_starts_to_date / max_possible_starts,
      .groups = "drop"
    )
}

#' Join all derived depth chart statistics
#' @param raw_df Raw depth chart data
#' @return A list of relevant depth chart features
process_depth_chart_features <- function(raw_df) {
  starters <- get_all_starters(raw_df)
  
  list(
    all_starters = starters,
    qb_stats = get_qb_start_stats(starters),
    player_start_totals = get_player_start_totals(starters),
    starter_switches = get_starter_switches(starters),
    promotions = get_inseason_promotions(raw_df),
    lineup_stability = get_lineup_stability(starters)
  )
}

#' Example usage to derive and assign outputs from depth chart functions
#' @param depth_charts_raw Raw depth chart data
#' @return Multiple objects in environment
run_depth_chart_analysis <- function(depth_charts_raw) {
  depth_chart_results <- process_depth_chart_features(depth_charts_raw)
  
  all_starters <<- depth_chart_results$all_starters
  qb_stats <<- depth_chart_results$qb_stats
  player_start_totals <<- depth_chart_results$player_start_totals
  starter_switches <<- depth_chart_results$starter_switches
  promotions <<- depth_chart_results$promotions
  lineup_stability <<- depth_chart_results$lineup_stability
}
