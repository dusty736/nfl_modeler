# step2_pbp_process_functions.R

library(dplyr)

#' Clean and normalize play-by-play data for modeling and dashboards
#'
#' @param df Raw pbp data from nflreadr::load_pbp()
#' @return Tibble with filtered, cleaned play-level data
clean_pbp_data <- function(df) {
  df %>%
    filter(!is.na(play_type), play_type != "no_play") %>%
    transmute(
      # Identifiers
      game_id = as.character(game_id),
      play_id = as.integer(play_id),
      qtr = as.integer(qtr),
      down = as.integer(down),
      ydstogo = as.integer(ydstogo),
      yardline_100 = as.integer(yardline_100),
      goal_to_go = as.logical(goal_to_go),
      
      # Team info
      posteam = as.character(posteam),
      defteam = as.character(defteam),
      home_team = as.character(home_team),
      away_team = as.character(away_team),
      
      # Clock and game state
      quarter_seconds_remaining = as.numeric(quarter_seconds_remaining),
      half_seconds_remaining = as.numeric(half_seconds_remaining),
      game_seconds_remaining = as.numeric(game_seconds_remaining),
      time = as.character(time),
      play_clock = as.character(play_clock),
      
      # Scoreboard
      home_score = as.integer(home_score),
      away_score = as.integer(away_score),
      posteam_score = as.integer(posteam_score),
      defteam_score = as.integer(defteam_score),
      total_home_score = as.integer(total_home_score),
      total_away_score = as.integer(total_away_score),
      score_differential = as.integer(score_differential),
      score_differential_post = as.integer(score_differential_post),
      
      # Outcomes
      yards_gained = as.integer(yards_gained),
      epa = as.numeric(epa),
      wpa = as.numeric(wpa),
      wp = as.numeric(wp),
      home_wp_post = as.numeric(home_wp_post),
      away_wp_post = as.numeric(away_wp_post),
      vegas_wp = as.numeric(vegas_wp),
      vegas_home_wp = as.numeric(vegas_home_wp),
      
      # Play details
      play_type = as.character(play_type),
      desc = as.character(desc),
      success = as.logical(success),
      penalty = as.logical(penalty),
      timeout = as.logical(timeout),
      aborted_play = as.logical(aborted_play),
      play_deleted = as.logical(play_deleted),
      
      # Modeling features
      air_yards = as.numeric(air_yards),
      yards_after_catch = as.numeric(yards_after_catch),
      pass_length = as.character(pass_length),
      pass_location = as.character(pass_location),
      rush_attempt = as.logical(rush_attempt == 1),
      pass_attempt = as.logical(pass_attempt == 1),
      sack = as.logical(sack == 1),
      touchdown = as.logical(touchdown == 1),
      interception = as.logical(interception == 1),
      
      # Probabilistic inputs
      xpass = as.numeric(xpass),
      pass_oe = as.numeric(pass_oe),
      
      # Drive and series context
      series = as.integer(series),
      series_result = as.character(series_result),
      drive = as.integer(drive),
      drive_play_count = as.integer(drive_play_count),
      drive_ended_with_score = as.logical(drive_ended_with_score == 1),
      drive_time_of_possession = as.character(drive_time_of_possession)
    )
}

