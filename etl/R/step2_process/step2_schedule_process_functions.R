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

get_weekly_season_table <- function(schedule_raw) {
  # Create long format: one row per team per game
  games_long <- dplyr::bind_rows(
    schedule_raw %>%
      dplyr::select(game_id, season, week, season_type = game_type, date = gameday,
                    team_id = home_team, opponent = away_team,
                    points_scored = home_score, points_allowed = away_score) %>%
      dplyr::mutate(location = "home"),
    
    schedule_raw %>%
      dplyr::select(game_id, season, week, season_type = game_type, date = gameday,
                    team_id = away_team, opponent = home_team,
                    points_scored = away_score, points_allowed = home_score) %>%
      dplyr::mutate(location = "away")
  )
  
  # Complete week-team grid to insert byes
  all_weeks <- games_long %>%
    dplyr::distinct(season, week, season_type)
  
  all_teams <- games_long %>%
    dplyr::distinct(season, team_id)
  
  # Rename season in teams to avoid conflict
  all_teams <- all_teams %>%
    dplyr::rename(team_season = season)
  
  full_grid <- tidyr::expand_grid(all_weeks, all_teams) %>%
    dplyr::filter(season == team_season) %>%
    dplyr::transmute(
      season,
      week,
      season_type,
      team_id
    )
  
  
  # Join game data onto grid
  weekly_table <- full_grid %>%
    dplyr::left_join(games_long, by = c("season", "week", "season_type", "team_id")) %>%
    dplyr::arrange(season, team_id, week) %>%
    dplyr::group_by(season, team_id) %>%
    dplyr::mutate(
      wins_entering = dplyr::lag(cumsum(dplyr::coalesce(points_scored > points_allowed, FALSE)), default = 0),
      losses_entering = dplyr::lag(cumsum(dplyr::coalesce(points_scored < points_allowed, FALSE)), default = 0),
      ties_entering = dplyr::lag(cumsum(dplyr::coalesce(points_scored == points_allowed, FALSE)), default = 0),
      points_scored_ytd = dplyr::lag(cumsum(dplyr::coalesce(points_scored, 0)), default = 0),
      points_allowed_ytd = dplyr::lag(cumsum(dplyr::coalesce(points_allowed, 0)), default = 0),
      point_diff_ytd = points_scored_ytd - points_allowed_ytd,
      week_label = dplyr::case_when(
        season_type == "REG" ~ paste0("Week ", week),
        season_type == "POST" & week == 1 ~ "Wild Card",
        season_type == "POST" & week == 2 ~ "Divisional",
        season_type == "POST" & week == 3 ~ "Conference",
        season_type == "POST" & week == 4 ~ "Super Bowl",
        season_type == "PRE" ~ paste0("Preseason Week ", week),
        TRUE ~ paste("Week", week)
      )
    ) %>%
    dplyr::ungroup() %>% 
    filter(!is.na(game_id))
  
  return(weekly_table)
}

summarize_season_team_results <- function(schedule_raw) {
  # Regular season: one row per team per game
  reg_long <- dplyr::bind_rows(
    schedule_raw %>%
      dplyr::filter(game_type == "REG") %>%
      dplyr::transmute(
        season,
        team_id = home_team,
        points_for = home_score,
        points_against = away_score,
        result = dplyr::case_when(
          home_score > away_score ~ "W",
          home_score < away_score ~ "L",
          TRUE ~ "T"
        )
      ),
    schedule_raw %>%
      dplyr::filter(game_type == "REG") %>%
      dplyr::transmute(
        season,
        team_id = away_team,
        points_for = away_score,
        points_against = home_score,
        result = dplyr::case_when(
          away_score > home_score ~ "W",
          away_score < home_score ~ "L",
          TRUE ~ "T"
        )
      )
  )
  
  # Summarize regular season results
  reg_summary <- reg_long %>%
    dplyr::group_by(season, team_id) %>%
    dplyr::summarise(
      wins = sum(result == "W"),
      losses = sum(result == "L"),
      ties = sum(result == "T"),
      points_for = sum(points_for, na.rm = TRUE),
      points_against = sum(points_against, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Postseason appearance and deepest round reached
  post_home <- schedule_raw %>%
    dplyr::filter(game_type %in% c("WC", "DIV", "CON", "SB")) %>%
    dplyr::transmute(
      season,
      team_id = home_team,
      game_type,
      won = home_score > away_score
    )
  
  post_away <- schedule_raw %>%
    dplyr::filter(game_type %in% c("WC", "DIV", "CON", "SB")) %>%
    dplyr::transmute(
      season,
      team_id = away_team,
      game_type,
      won = away_score > home_score
    )
  
  post_long <- dplyr::bind_rows(post_home, post_away) %>%
    dplyr::mutate(
      round = dplyr::case_when(
        game_type == "WC" ~ "WC",
        game_type == "DIV" ~ "DIV",
        game_type == "CON" ~ "CONF",
        game_type == "SB" & won ~ "SB_WIN",
        game_type == "SB" & !won ~ "SB",
        TRUE ~ NA_character_
      )
    )
  
  postseason_summary <- post_long %>%
    dplyr::group_by(season, team_id) %>%
    dplyr::summarise(
      made_playoffs = TRUE,
      postseason_round = factor(
        dplyr::case_when(
          "SB_WIN" %in% round ~ "SB_WIN",
          "SB" %in% round ~ "SB",
          "CONF" %in% round ~ "CONF",
          "DIV" %in% round ~ "DIV",
          "WC" %in% round ~ "WC",
          TRUE ~ "None"
        ),
        levels = c("None", "WC", "DIV", "CONF", "SB", "SB_WIN"),
        ordered = TRUE
      ),
      .groups = "drop"
    )
  
  # Merge into final season summary
  season_summary <- reg_summary %>%
    dplyr::left_join(postseason_summary, by = c("season", "team_id")) %>%
    dplyr::mutate(
      made_playoffs = dplyr::coalesce(made_playoffs, FALSE),
      postseason_round = dplyr::coalesce(as.character(postseason_round), "None"),
      point_diff = points_for - points_against
    ) %>% 
    dplyr::select(season, team_id, wins, losses, ties, points_scored = points_for,
                  points_allowed = points_against, point_diff, made_playoffs, postseason_round)
  
  return(season_summary)
}



