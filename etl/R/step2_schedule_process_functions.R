# step2_schedule_process_functions.R

library(dplyr)
library(lubridate)
library(stringr)

#' Clean and normalize schedule data
#'
#' @param df Raw nflverse schedule dataset
#' @return Normalized games tibble ready for SQL import
clean_schedule_data <- function(df) {
  df %>%
    transmute(
      game_id = as.character(game_id),
      season = as.integer(season),
      week = as.integer(week),
      game_type = as.character(game_type),
      kickoff = parse_kickoff_time(gameday, gametime),
      weekday = as.character(weekday),
      
      home_team = str_to_upper(home_team),
      away_team = str_to_upper(away_team),
      home_score = as.integer(home_score),
      away_score = as.integer(away_score),
      result = compute_result(home_score, away_score),
      favored_team = compute_favored_team(home_team, away_team, spread_line),
      overtime = as.logical(overtime),
      
      stadium = as.character(stadium),
      stadium_id = as.character(stadium_id),
      roof = as.character(roof),
      surface = as.character(surface),
      temp = as.integer(temp),
      wind = as.integer(wind),
      
      referee = as.character(referee),
      
      home_qb_id = as.character(home_qb_id),
      away_qb_id = as.character(away_qb_id),
      home_qb_name = as.character(home_qb_name),
      away_qb_name = as.character(away_qb_name),
      
      home_coach = as.character(home_coach),
      away_coach = as.character(away_coach),
      
      div_game = as.logical(div_game),
      
      # Betting lines
      spread_line = as.numeric(spread_line),
      total_line = as.numeric(total_line),
      away_moneyline = as.integer(away_moneyline),
      home_moneyline = as.integer(home_moneyline),
      
      away_spread_odds = as.integer(away_spread_odds),
      home_spread_odds = as.integer(home_spread_odds),
      under_odds = as.integer(under_odds),
      over_odds = as.integer(over_odds),
      
      # External identifiers
      old_game_id = as.character(old_game_id),
      gsis = as.character(gsis),
      nfl_detail_id = as.character(nfl_detail_id),
      pfr = as.character(pfr),
      pff = as.character(pff),
      espn = as.character(espn),
      ftn = as.character(ftn)
    )
}

#' Parse kickoff timestamp
parse_kickoff_time <- function(date_col, time_col) {
  dt <- as.POSIXct(
    paste(date_col, time_col),
    format = "%Y-%m-%d %H:%M",
    tz = "UTC"
  )
  dt
}

#' Determine game result: HOME, AWAY, or TIE
compute_result <- function(home_score, away_score) {
  case_when(
    is.na(home_score) | is.na(away_score) ~ NA_character_,
    home_score > away_score ~ "HOME",
    home_score < away_score ~ "AWAY",
    home_score == away_score ~ "TIE"
  )
}

#' Determine favored team based on spread_line
compute_favored_team <- function(home_team, away_team, spread_line) {
  case_when(
    is.na(spread_line) ~ NA_character_,
    spread_line < 0 ~ home_team,
    spread_line > 0 ~ away_team,
    spread_line == 0 ~ "EVEN"
  )
}
