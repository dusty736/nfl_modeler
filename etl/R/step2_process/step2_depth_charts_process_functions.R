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
    group_by(season, team, position, gsis_id) %>% 
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
    distinct() %>% 
    group_by(season, team, position, gsis_id) %>% 
    mutate(player_starts = row_number()) %>% 
    mutate(
      is_new_starter = player_starts == 1 & week != 1
    ) %>% 
    ungroup()
}

#' Identify players who became new starters (were not starters the previous week)
#' @param starters_df Cleaned starters-only dataframe
#' @return DataFrame of promotions with week flagged
#' Identify players promoted to starters relative to the *previous week*
#' (no Week 1 flags; compares week t starters to week t-1 starters)
get_inseason_promotions <- function(starters_df) {
  starters_df %>%
    # one row per player per week per slot
    dplyr::distinct(season, team, position, week, player, .keep_all = TRUE) %>%
    dplyr::arrange(season, team, position, week) %>%
    # build weekly starter sets
    dplyr::group_by(season, team, position, week) %>%
    dplyr::summarise(starters = list(unique(player)), .groups = "drop_last") %>%
    dplyr::arrange(season, team, position, week) %>%
    dplyr::group_by(season, team, position) %>%
    dplyr::mutate(
      prev_starters = dplyr::lag(starters),
      is_first_week = dplyr::row_number() == 1L
    ) %>%
    dplyr::ungroup() %>%
    # only compare weeks that have a previous week
    dplyr::filter(!is_first_week) %>%
    dplyr::mutate(
      promoted_players = purrr::map2(
        starters, prev_starters,
        ~ setdiff(.x, if (is.null(.y)) character(0) else .y)
      )
    ) %>%
    tidyr::unnest_longer(promoted_players, values_to = "player") %>%
    dplyr::filter(!is.na(player)) %>%
    dplyr::transmute(
      season, team, position, week,
      player,
      promoted_to_starter = TRUE
    ) %>%
    dplyr::arrange(season, team, position, week, player)
}

library(dplyr)
library(stringr)

clean_position <- function(x,
                           de_to   = c("EDGE", "DL"),   # where to send DE/LE/RE/etc.
                           h_as    = c("WR", "TE"),     # where to send H / H-back
                           s_generic_to = c("FS","SS")  # where to send bare "S"
) {
  de_to        <- match.arg(de_to)
  h_as         <- match.arg(h_as)
  s_generic_to <- match.arg(s_generic_to)
  
  x0 <- toupper(trimws(as.character(x)))
  x0 <- str_replace_all(x0, "\\s+", "")
  x0 <- str_replace_all(x0, "\\\\", "/")
  x0 <- str_replace_all(x0, "\\n", "")
  
  case_when(
    # Offense skill
    str_detect(x0, "^QB$") ~ "QB",
    str_detect(x0, "^(RB|HB|RB/TE|HB/TE|HB-TE|RB\\d+|RBC)$") ~ "RB",
    str_detect(x0, "^(FB|FB/TE|H-B)$") ~ "FB",
    str_detect(x0, "^(WR|LWR|RWR|WR1|WR2|WRE|WE|SE|FL|SL|WR/\\d+|WR\\d+)$") ~ "WR",
    str_detect(x0, "^(H)$") ~ h_as,  # H-back → WR (or TE if you set h_as="TE")
    str_detect(x0, "^(TE|TE/FB|TE/HB|LTE|RTE|TE/LS|LS/TE)$") ~ "TE",
    
    # Offensive line
    str_detect(x0, "^(LT|LOT)$") ~ "LT",
    str_detect(x0, "^RT$") ~ "RT",
    str_detect(x0, "^LG$") ~ "LG",
    str_detect(x0, "^RG$") ~ "RG",
    str_detect(x0, "^(C|OC)$") ~ "C",
    str_detect(x0, "^(OL|OLB?|OT|G|T|LS)$") ~ "OL",  # generic/ambiguous OL
    
    # Interior DL
    str_detect(x0, "^(DT|NT|NG|NDT|NOSE|UT)$") ~ "DT",
    
    # Edge / Ends (choice: EDGE or DL)
    str_detect(x0, "^(EDGE|DE/LB|LEO|DPR|RUSH|JACK)$") ~ "EDGE",
    str_detect(x0, "^(DE|LDE|RDE|LE|RE|END|OE|DDE)$") ~ ifelse(de_to=="EDGE","EDGE","DL"),
    
    # Generic DL
    str_detect(x0, "^(DL|DL44|DL/OL)$") ~ "DL",
    
    # Linebackers
    str_detect(x0, "^(MLB|MIKE|ILB|MILB|WILB|RILB|LILB|MO|ML|LB$)$") ~ "MLB",
    str_detect(x0, "^(OLB|LOLB|ROLB|SAM|WILL|WLB|SLB|OTTO|BLB)$") ~ "OLB",
    
    # Secondary
    str_detect(x0, "^(CB|LCB|RCB|NCB|MCB|NB|NICK|NICKE|NKL)$") ~ "CB",
    str_detect(x0, "^(SS|WS)$") ~ "SS",
    str_detect(x0, "^(FS)$") ~ "FS",
    str_detect(x0, "^(S|DS|CS)$") ~ s_generic_to,   # bare S → FS (or SS)
    
    x0 == "K" ~ "K",
    x0 == "P" ~ "P",
    
    str_detect(x0, "^(KO|KR|PR|HLS)") ~ "ST",
    
    TRUE ~ "OTHER"
  )
}

# Maps raw depth-chart positions to these exact labels (your set):
# C FB FS H KR LCB LDE LDT LG LILB LS LT MLB NB NT P PK PR QB RB RCB RDE RDT RG RILB RT SLB SS TE WLB WR
# Anything else -> "OTHER".
# Notes:
# - Bare "CB" -> NB (change default_cb if you prefer LCB/RCB).
# - Bare "DE" -> default_de (RDE by default).
# - Bare "DT" -> default_dt (NT by default).
# - Bare "ILB" -> MLB; "LILB"/"RILB" map to those sides.
# - Bare "OLB" -> default_olb (WLB by default).
# - Bare "S"  -> s_generic_to (FS by default).
# - "H" here is the holder (kept as "H"). If you use H-back, set h_as = "TE" or "WR".

clean_position_update <- function(x,
                           de_to   = c("EDGE", "DL"),   # where to send DE/LE/RE/etc.
                           h_as    = c("WR", "TE"),     # where to send H / H-back
                           s_generic_to = c("FS","SS")  # where to send bare "S"
) {
  de_to        <- match.arg(de_to)
  h_as         <- match.arg(h_as)
  s_generic_to <- match.arg(s_generic_to)
  
  x0 <- toupper(trimws(as.character(x)))
  x0 <- str_replace_all(x0, "\\s+", "")
  x0 <- str_replace_all(x0, "\\\\", "/")
  x0 <- str_replace_all(x0, "\\n", "")
  
  dplyr::case_when(
    # Offense skill
    str_detect(x0, "^QB$") ~ "QB",
    str_detect(x0, "^(RB|HB|RB/TE|HB/TE|HB-TE|RB\\d+|RBC)$") ~ "RB",
    str_detect(x0, "^(FB|FB/TE|H-B)$") ~ "FB",
    str_detect(x0, "^(WR|LWR|RWR|WR1|WR2|WRE|WE|SE|FL|SL|WR/\\d+|WR\\d+)$") ~ "WR",
    str_detect(x0, "^(H)$") ~ h_as,  # H-back → WR (or TE)
    str_detect(x0, "^(TE|TE/FB|TE/HB|LTE|RTE|TE/LS|LS/TE)$") ~ "TE",
    
    # Offensive line
    str_detect(x0, "^(LT|LOT)$") ~ "LT",
    str_detect(x0, "^RT$") ~ "RT",
    str_detect(x0, "^LG$") ~ "LG",
    str_detect(x0, "^RG$") ~ "RG",
    str_detect(x0, "^(C|OC)$") ~ "C",
    str_detect(x0, "^(OL|OLB?|OT|G|T|LS)$") ~ "OL",  # generic/ambiguous OL
    
    # Interior DL
    str_detect(x0, "^(DT|NT|NG|NDT|NOSE|UT)$") ~ "DT",
    
    # Edge / Ends (choice: EDGE or DL)
    str_detect(x0, "^(EDGE|DE/LB|LEO|DPR|RUSH|JACK)$") ~ "EDGE",
    str_detect(x0, "^(DE|LDE|RDE|LE|RE|END|OE|DDE)$") ~ ifelse(de_to == "EDGE", "EDGE", "DL"),
    
    # Generic DL
    str_detect(x0, "^(DL|DL44|DL/OL)$") ~ "DL",
    
    # Linebackers
    str_detect(x0, "^(MLB|MIKE|ILB|MILB|WILB|RILB|LILB|MO|ML|LB$)$") ~ "MLB",
    str_detect(x0, "^(OLB|LOLB|ROLB|SAM|WILL|WLB|SLB|OTTO|BLB)$") ~ "OLB",
    
    # Secondary
    str_detect(x0, "^(CB|LCB|RCB|NCB|MCB|NB|NICK|NICKE|NKL)$") ~ "CB",
    str_detect(x0, "^(SS|WS)$") ~ "SS",
    str_detect(x0, "^(FS)$") ~ "FS",
    str_detect(x0, "^(S|DS|CS)$") ~ s_generic_to,   # bare S → FS (or SS)
    
    x0 == "K" ~ "K",
    x0 == "P" ~ "P",
    
    str_detect(x0, "^(KO|KR|PR|HLS)$") ~ "ST",
    
    TRUE ~ "OTHER"
  )
}

#' Compute weekly depth chart stability by position/team
#' @param starters_df Starter-only depth chart data (with position)
#' @return Table with weekly top starter share by team/position
get_lineup_stability_by_week <- function(starters_df) {
  # 1. Summarize number of position entries per team-week
  position_games <- starters_df %>% 
    distinct() %>% 
    group_by(season, week, team, position_group) %>% 
    summarize(position_count = n()) %>% 
    arrange(team, position_group, season, week) %>% 
    group_by(season, team, position_group) %>% 
    mutate(running_position_count = cumsum(position_count))
  
  position_players <- starters_df %>% 
    arrange(season, week, team, position_group, player) %>% 
    distinct() %>% 
    group_by(season, team, position_group, player) %>% 
    mutate(start_count = row_number()) %>% 
    arrange(team, position_group, player, season, week)
  
  stability_by_week <- position_games %>% 
    left_join(., position_players, by=c('team', 'position_group', 'season', 'week')) %>% 
    group_by(season, week, team, position_group, running_position_count) %>% 
    summarize(starts = sum(start_count)) %>% 
    ungroup(.) %>% 
    mutate(position_group_score = round(starts / running_position_count, 3)) %>% 
    dplyr::select(season, week, team, position_group, position_group_score) %>% 
    filter(position_group != "OTHER")
  
  return(stability_by_week)
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
