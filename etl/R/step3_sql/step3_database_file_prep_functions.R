#' Format Play-by-Play Data for SQL Import
#'
#' Uses the exact schema from `file_summary.csv` to transform and export the play-by-play table.
#'
#' @param input_path Path to the raw `pbp_cleaned.parquet` file.
#' @param output_path Path to save the SQL-ready parquet file (e.g., `pbp_sql.parquet`).
#' @return Invisibly returns the processed data.
#' @export
format_pbp_for_sql <- function(input_path, output_path = "pbp_sql.parquet") {
  pbp_sql <- arrow::read_parquet(input_path) %>%
    transmute(
      game_id = as.character(game_id),
      play_id = as.integer(play_id),
      qtr = as.integer(qtr),
      down = as.integer(down),
      ydstogo = as.integer(ydstogo),
      yardline_100 = as.integer(yardline_100),
      posteam = as.character(posteam),
      defteam = as.character(defteam),
      play_type = as.character(play_type),
      epa = as.numeric(epa),
      success = as.integer(success),
      touchdown = as.integer(touchdown),
      interception = as.integer(interception),
      penalty = as.integer(penalty),
      pass = as.integer(pass_attempt),
      rush = as.integer(rush_attempt),
      special = as.integer(sack),
      cumulative_epa_offense = as.numeric(cum_epa_offense),
      cumulative_success_offense = as.integer(cum_success_offense),
      cumulative_td_offense = as.integer(cum_td_offense),
      cumulative_int_offense = as.integer(cum_int_offense),
      cumulative_penalty_offense = as.integer(cum_penalty_offense),
      cumulative_epa_defense = as.numeric(cum_epa_defense),
      cumulative_success_defense = as.integer(cum_success_defense),
      cumulative_td_allowed = as.integer(cum_td_allowed),
      cumulative_int_defense = as.integer(cum_int_defense),
      cumulative_penalty_defense = as.integer(cum_penalty_defense)
    )
  
  arrow::write_parquet(pbp_sql, output_path)
  invisible(pbp_sql)
}

#' Format Game Data for SQL Import
#'
#' Transforms raw `games.parquet` data into SQL-aligned format.
#'
#' @param input_path Path to `games.parquet`
#' @param output_path Path to save SQL-ready file (default: "game_sql.parquet")
#' @return SQL-aligned game dataframe (invisibly)
#' @export
format_game_for_sql <- function(input_path, output_path = "game_sql.parquet") {
  game_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      game_id = as.character(game_id),
      season = as.integer(season),
      week = as.integer(week),
      game_type = as.character(game_type),
      kickoff = as.POSIXct(kickoff, tz = "UTC"),
      weekday = as.character(weekday),
      home_team = as.character(home_team),
      away_team = as.character(away_team),
      home_score = as.integer(home_score),
      away_score = as.integer(away_score),
      result = as.character(result),
      favored_team = as.character(favored_team),
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
      spread_line = as.numeric(spread_line),
      total_line = as.numeric(total_line),
      away_moneyline = as.integer(away_moneyline),
      home_moneyline = as.integer(home_moneyline),
      away_spread_odds = as.integer(away_spread_odds),
      home_spread_odds = as.integer(home_spread_odds),
      under_odds = as.integer(under_odds),
      over_odds = as.integer(over_odds),
      old_game_id = as.character(old_game_id),
      gsis = as.character(gsis),
      nfl_detail_id = as.character(nfl_detail_id),
      pfr = as.character(pfr),
      pff = as.character(pff),
      espn = as.character(espn),
      ftn = as.character(ftn)
    )
  
  arrow::write_parquet(game_sql, output_path)
  invisible(game_sql)
}

#' Format Season Data for SQL Import
#'
#' Prepares season summary data for SQL ingestion.
#'
#' @param input_path Path to `season_results.parquet`
#' @param output_path Path to save SQL-ready file (default: "season_sql.parquet")
#' @return SQL-aligned season summary dataframe (invisibly)
#' @export
format_season_for_sql <- function(input_path, output_path = "season_sql.parquet") {
  season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      team_id = as.character(team_id),
      wins = as.integer(wins),
      losses = as.integer(losses),
      ties = as.integer(ties),
      points_for = as.integer(points_scored),
      points_against = as.integer(points_allowed),
      point_diff = as.integer(point_diff),
      made_playoffs = as.logical(made_playoffs),
      postseason_round = as.character(postseason_round)
    )
  
  arrow::write_parquet(season_sql, output_path)
  invisible(season_sql)
}

#' Format Weekly Results Data for SQL Import
#'
#' Transforms `weekly_results.parquet` into a normalized format for SQL ingestion.
#'
#' @param input_path Path to the raw weekly results file.
#' @param output_path Output path for the SQL-aligned Parquet file.
#' @return A cleaned and reordered weekly results table (invisibly).
#' @export
format_weeks_for_sql <- function(input_path, output_path = "weekly_results_tbl.parquet") {
  weekly_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      season_type = as.character(season_type),
      team_id = as.character(team_id),
      game_id = as.character(game_id),
      date = as.Date(date),
      opponent = as.character(opponent),
      points_scored = as.integer(points_scored),
      points_allowed = as.integer(points_allowed),
      location = as.character(location),
      wins_entering = as.integer(wins_entering),
      losses_entering = as.integer(losses_entering),
      ties_entering = as.integer(ties_entering),
      points_scored_ytd = as.numeric(points_scored_ytd),
      points_allowed_ytd = as.numeric(points_allowed_ytd),
      point_diff_ytd = as.numeric(point_diff_ytd),
      week_label = as.character(week_label)
    )
  
  arrow::write_parquet(weekly_sql, output_path)
  invisible(weekly_sql)
}

#' Format Roster Data for SQL Import
#'
#' Prepares the `rosters.parquet` file to match the normalized roster schema for SQL ingestion.
#'
#' @param input_path Path to the raw roster parquet file.
#' @param output_path Path to write SQL-ready Parquet file.
#' @return Roster data in SQL-compatible format (invisibly).
#' @export
format_roster_for_sql <- function(input_path, output_path = "rosters_tbl.parquet") {
  roster_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_id = as.character(player_id),
      team_id = as.character(team),  # Rename to match schema
      season = as.integer(season),
      week = NA_integer_,  # Schema expects week as part of PK; fill in or join later
      full_name = as.character(full_name),
      first_name = as.character(first_name),
      last_name = as.character(last_name),
      position = as.character(position),
      status = as.character(status),
      age = as.numeric(age),
      height = as.integer(height),
      weight = as.integer(weight),
      college = as.character(college),
      years_exp = as.integer(years_exp),
      rookie_year = as.integer(rookie_year),
      entry_year = as.integer(entry_year),
      headshot_url = as.character(headshot_url),
      esb_id = as.character(esb_id)
    )
  
  arrow::write_parquet(roster_sql, output_path)
  invisible(roster_sql)
}

#' Format Roster Summary Data for SQL Import
#'
#' Transforms `roster_summary.parquet` to SQL-aligned format.
#'
#' @param input_path Path to the input roster summary Parquet file.
#' @param output_path Path to write the SQL-ready version (default: "roster_summary_tbl.parquet").
#' @return Cleaned roster summary data (invisibly).
#' @export
format_roster_summary_for_sql <- function(input_path, output_path = "roster_summary_tbl.parquet") {
  roster_summary_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      team = as.character(team),
      n_players = as.integer(n_players),
      avg_age = as.numeric(avg_age),
      avg_height = as.numeric(avg_height),
      avg_weight = as.numeric(avg_weight),
      avg_exp = as.numeric(avg_exp)
    )
  
  arrow::write_parquet(roster_summary_sql, output_path)
  invisible(roster_summary_sql)
}

#' Format Roster Position Summary Data for SQL Import
#'
#' Formats the `roster_position_summary.parquet` file for SQL-ready ingestion.
#'
#' @param input_path Path to the raw input Parquet file.
#' @param output_path Path to write the SQL-ready output file.
#' @return SQL-formatted position summary table (invisibly).
#' @export
format_roster_position_summary_for_sql <- function(input_path, output_path = "roster_position_summary_tbl.parquet") {
  roster_position_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      team = as.character(team),
      position = as.character(position),
      n_players = as.integer(n_players),
      avg_age = as.numeric(avg_age),
      avg_height = as.numeric(avg_height),
      avg_weight = as.numeric(avg_weight),
      avg_exp = as.numeric(avg_exp)
    )
  
  arrow::write_parquet(roster_position_sql, output_path)
  invisible(roster_position_sql)
}

#' Format Weekly QB Stats for SQL Import
#'
#' Transforms `weekly_stats_qb.parquet` into a normalized table for SQL ingestion.
#'
#' @param input_path Path to the raw weekly QB stats Parquet file.
#' @param output_path Path to write SQL-aligned Parquet output.
#' @return Cleaned and typed weekly QB stats (invisibly).
#' @export
format_weekly_qb_stats_for_sql <- function(input_path, output_path = "weekly_stats_qb_tbl.parquet") {
  qb_weekly_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      season_type = as.character(season_type),
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      opponent_team = as.character(opponent_team),
      completions = as.integer(completions),
      attempts = as.integer(attempts),
      passing_yards = as.numeric(passing_yards),
      passing_tds = as.integer(passing_tds),
      interceptions = as.numeric(interceptions),
      sacks = as.numeric(sacks),
      sack_yards = as.numeric(sack_yards),
      sack_fumbles = as.integer(sack_fumbles),
      sack_fumbles_lost = as.integer(sack_fumbles_lost),
      passing_air_yards = as.numeric(passing_air_yards),
      passing_yards_after_catch = as.numeric(passing_yards_after_catch),
      passing_first_downs = as.numeric(passing_first_downs),
      passing_epa = as.numeric(passing_epa),
      passing_2pt_conversions = as.integer(passing_2pt_conversions),
      pacr = as.numeric(pacr),
      dakota = as.numeric(dakota),
      carries = as.integer(carries),
      rushing_yards = as.numeric(rushing_yards),
      rushing_tds = as.integer(rushing_tds),
      rushing_fumbles = as.numeric(rushing_fumbles),
      rushing_fumbles_lost = as.numeric(rushing_fumbles_lost),
      rushing_first_downs = as.numeric(rushing_first_downs),
      rushing_epa = as.numeric(rushing_epa),
      rushing_2pt_conversions = as.integer(rushing_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      cumulative_completions = as.numeric(cumulative_completions),
      cumulative_attempts = as.numeric(cumulative_attempts),
      cumulative_passing_yards = as.numeric(cumulative_passing_yards),
      cumulative_passing_tds = as.numeric(cumulative_passing_tds),
      cumulative_interceptions = as.numeric(cumulative_interceptions),
      cumulative_sacks = as.numeric(cumulative_sacks),
      cumulative_sack_yards = as.numeric(cumulative_sack_yards),
      cumulative_passing_epa = as.numeric(cumulative_passing_epa),
      cumulative_rushing_yards = as.numeric(cumulative_rushing_yards),
      cumulative_rushing_tds = as.numeric(cumulative_rushing_tds),
      cumulative_rushing_epa = as.numeric(cumulative_rushing_epa),
      cumulative_fantasy_points = as.numeric(cumulative_fantasy_points),
      cumulative_fantasy_points_ppr = as.numeric(cumulative_fantasy_points_ppr)
    )
  
  arrow::write_parquet(qb_weekly_sql, output_path)
  invisible(qb_weekly_sql)
}

#' Format Weekly RB Stats for SQL Import
#'
#' Transforms `weekly_stats_rb.parquet` into SQL-compatible format.
#'
#' @param input_path Path to the RB stats Parquet file.
#' @param output_path Output file path for SQL-ready table (default: "weekly_stats_rb_tbl.parquet").
#' @return Normalized weekly RB stats table (invisibly).
#' @export
format_weekly_rb_stats_for_sql <- function(input_path, output_path = "weekly_stats_rb_tbl.parquet") {
  rb_weekly_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      season_type = as.character(season_type),
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      opponent_team = as.character(opponent_team),
      carries = as.integer(carries),
      rushing_yards = as.numeric(rushing_yards),
      rushing_tds = as.integer(rushing_tds),
      rushing_fumbles = as.numeric(rushing_fumbles),
      rushing_fumbles_lost = as.numeric(rushing_fumbles_lost),
      rushing_first_downs = as.numeric(rushing_first_downs),
      rushing_epa = as.numeric(rushing_epa),
      rushing_2pt_conversions = as.integer(rushing_2pt_conversions),
      targets = as.integer(targets),
      receptions = as.integer(receptions),
      receiving_yards = as.numeric(receiving_yards),
      receiving_tds = as.integer(receiving_tds),
      receiving_fumbles = as.numeric(receiving_fumbles),
      receiving_fumbles_lost = as.numeric(receiving_fumbles_lost),
      receiving_air_yards = as.numeric(receiving_air_yards),
      receiving_yards_after_catch = as.numeric(receiving_yards_after_catch),
      receiving_first_downs = as.numeric(receiving_first_downs),
      receiving_epa = as.numeric(receiving_epa),
      receiving_2pt_conversions = as.integer(receiving_2pt_conversions),
      racr = as.numeric(racr),
      target_share = as.numeric(target_share),
      air_yards_share = as.numeric(air_yards_share),
      wopr = as.numeric(wopr),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      cumulative_carries = as.numeric(cumulative_carries),
      cumulative_rushing_yards = as.numeric(cumulative_rushing_yards),
      cumulative_rushing_tds = as.numeric(cumulative_rushing_tds),
      cumulative_rushing_epa = as.numeric(cumulative_rushing_epa),
      cumulative_targets = as.numeric(cumulative_targets),
      cumulative_receptions = as.numeric(cumulative_receptions),
      cumulative_receiving_yards = as.numeric(cumulative_receiving_yards),
      cumulative_receiving_tds = as.numeric(cumulative_receiving_tds),
      cumulative_receiving_epa = as.numeric(cumulative_receiving_epa),
      cumulative_fantasy_points = as.numeric(cumulative_fantasy_points),
      cumulative_fantasy_points_ppr = as.numeric(cumulative_fantasy_points_ppr)
    )
  
  arrow::write_parquet(rb_weekly_sql, output_path)
  invisible(rb_weekly_sql)
}

#' Format Weekly WR/TE Stats for SQL Import
#'
#' Transforms `weekly_stats_wr.parquet` or `weekly_stats_te.parquet`
#' into a SQL-compatible structure.
#'
#' @param input_path Path to the WR or TE weekly stats Parquet file.
#' @param output_path Path to save SQL-ready version (default varies).
#' @return Cleaned WR/TE weekly stats table (invisibly).
#' @export
format_weekly_wrte_stats_for_sql <- function(input_path, output_path = NULL) {
  if (is.null(output_path)) {
    file_base <- gsub("\\.parquet$", "", basename(input_path))
    output_path <- file.path("data", "for_database", paste0(file_base, "_tbl.parquet"))
  }
  
  wrte_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      season_type = as.character(season_type),
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      opponent_team = as.character(opponent_team),
      targets = as.integer(targets),
      receptions = as.integer(receptions),
      receiving_yards = as.numeric(receiving_yards),
      receiving_tds = as.integer(receiving_tds),
      receiving_fumbles = as.numeric(receiving_fumbles),
      receiving_fumbles_lost = as.numeric(receiving_fumbles_lost),
      receiving_air_yards = as.numeric(receiving_air_yards),
      receiving_yards_after_catch = as.numeric(receiving_yards_after_catch),
      receiving_first_downs = as.numeric(receiving_first_downs),
      receiving_epa = as.numeric(receiving_epa),
      receiving_2pt_conversions = as.integer(receiving_2pt_conversions),
      racr = as.numeric(racr),
      target_share = as.numeric(target_share),
      air_yards_share = as.numeric(air_yards_share),
      wopr = as.numeric(wopr),
      carries = as.integer(carries),
      rushing_yards = as.numeric(rushing_yards),
      rushing_tds = as.integer(rushing_tds),
      rushing_fumbles = as.numeric(rushing_fumbles),
      rushing_fumbles_lost = as.numeric(rushing_fumbles_lost),
      rushing_first_downs = as.numeric(rushing_first_downs),
      rushing_epa = as.numeric(rushing_epa),
      rushing_2pt_conversions = as.integer(rushing_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      cumulative_targets = as.numeric(cumulative_targets),
      cumulative_receptions = as.numeric(cumulative_receptions),
      cumulative_receiving_yards = as.numeric(cumulative_receiving_yards),
      cumulative_receiving_tds = as.numeric(cumulative_receiving_tds),
      cumulative_receiving_epa = as.numeric(cumulative_receiving_epa),
      cumulative_carries = as.numeric(cumulative_carries),
      cumulative_rushing_yards = as.numeric(cumulative_rushing_yards),
      cumulative_rushing_tds = as.numeric(cumulative_rushing_tds),
      cumulative_rushing_epa = as.numeric(cumulative_rushing_epa),
      cumulative_fantasy_points = as.numeric(cumulative_fantasy_points),
      cumulative_fantasy_points_ppr = as.numeric(cumulative_fantasy_points_ppr)
    )
  
  arrow::write_parquet(wrte_sql, output_path)
  invisible(wrte_sql)
}

#' Format Season QB Stats for SQL Import
#'
#' Transforms `season_stats_qb.parquet` into a SQL-compatible format.
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path for SQL-ready file (default: "season_stats_qb_tbl.parquet").
#' @return Season-level QB stats ready for database import (invisibly).
#' @export
format_season_qb_stats_for_sql <- function(input_path, output_path = "season_stats_qb_tbl.parquet") {
  qb_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      games_played = as.integer(games_played),
      completions = as.integer(completions),
      attempts = as.integer(attempts),
      passing_yards = as.numeric(passing_yards),
      passing_tds = as.integer(passing_tds),
      interceptions = as.numeric(interceptions),
      sacks = as.numeric(sacks),
      sack_yards = as.numeric(sack_yards),
      sack_fumbles = as.integer(sack_fumbles),
      sack_fumbles_lost = as.integer(sack_fumbles_lost),
      passing_air_yards = as.numeric(passing_air_yards),
      passing_yards_after_catch = as.numeric(passing_yards_after_catch),
      passing_first_downs = as.numeric(passing_first_downs),
      passing_epa = as.numeric(passing_epa),
      passing_2pt_conversions = as.integer(passing_2pt_conversions),
      carries = as.integer(carries),
      rushing_yards = as.numeric(rushing_yards),
      rushing_tds = as.integer(rushing_tds),
      rushing_fumbles = as.numeric(rushing_fumbles),
      rushing_fumbles_lost = as.numeric(rushing_fumbles_lost),
      rushing_first_downs = as.numeric(rushing_first_downs),
      rushing_epa = as.numeric(rushing_epa),
      rushing_2pt_conversions = as.integer(rushing_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      pacr = as.numeric(pacr),
      dakota = as.numeric(dakota)
    )
  
  arrow::write_parquet(qb_season_sql, output_path)
  invisible(qb_season_sql)
}

#' Format Season RB Stats for SQL Import
#'
#' Transforms `season_stats_rb.parquet` into a SQL-aligned structure.
#'
#' @param input_path Path to the RB season-level Parquet file.
#' @param output_path Path for the SQL-ready output (default: "season_stats_rb_tbl.parquet").
#' @return SQL-normalized RB stats (invisibly).
#' @export
format_season_rb_stats_for_sql <- function(input_path, output_path = "season_stats_rb_tbl.parquet") {
  rb_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      games_played = as.integer(games_played),
      carries = as.integer(carries),
      rushing_yards = as.numeric(rushing_yards),
      rushing_tds = as.integer(rushing_tds),
      rushing_epa = as.numeric(rushing_epa),
      rushing_fumbles = as.numeric(rushing_fumbles),
      rushing_fumbles_lost = as.numeric(rushing_fumbles_lost),
      rushing_first_downs = as.numeric(rushing_first_downs),
      rushing_2pt_conversions = as.integer(rushing_2pt_conversions),
      targets = as.integer(targets),
      receptions = as.integer(receptions),
      receiving_yards = as.numeric(receiving_yards),
      receiving_tds = as.integer(receiving_tds),
      receiving_epa = as.numeric(receiving_epa),
      receiving_fumbles = as.numeric(receiving_fumbles),
      receiving_fumbles_lost = as.numeric(receiving_fumbles_lost),
      receiving_air_yards = as.numeric(receiving_air_yards),
      receiving_yards_after_catch = as.numeric(receiving_yards_after_catch),
      receiving_first_downs = as.numeric(receiving_first_downs),
      receiving_2pt_conversions = as.integer(receiving_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      racr = as.numeric(racr),
      target_share = as.numeric(target_share),
      air_yards_share = as.numeric(air_yards_share),
      wopr = as.numeric(wopr)
    )
  
  arrow::write_parquet(rb_season_sql, output_path)
  invisible(rb_season_sql)
}

#' Format Season WR/TE Stats for SQL Import
#'
#' Transforms `season_stats_wr.parquet` or `season_stats_te.parquet`
#' into a normalized SQL-compatible format.
#'
#' @param input_path Path to WR/TE season-level stats file.
#' @param output_path Path for SQL-aligned output (auto-generated if NULL).
#' @return SQL-ready WR/TE season table (invisibly).
#' @export
format_season_wrte_stats_for_sql <- function(input_path, output_path = NULL) {
  if (is.null(output_path)) {
    file_base <- gsub("\\.parquet$", "", basename(input_path))
    output_path <- file.path("data", "for_database", paste0(file_base, "_tbl.parquet"))
  }
  
  wrte_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      games_played = as.integer(games_played),
      targets = as.integer(targets),
      receptions = as.integer(receptions),
      receiving_yards = as.numeric(receiving_yards),
      receiving_tds = as.integer(receiving_tds),
      receiving_fumbles = as.numeric(receiving_fumbles),
      receiving_fumbles_lost = as.numeric(receiving_fumbles_lost),
      receiving_air_yards = as.numeric(receiving_air_yards),
      receiving_yards_after_catch = as.numeric(receiving_yards_after_catch),
      receiving_first_downs = as.numeric(receiving_first_downs),
      receiving_epa = as.numeric(receiving_epa),
      receiving_2pt_conversions = as.integer(receiving_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      racr = as.numeric(racr),
      target_share = as.numeric(target_share),
      air_yards_share = as.numeric(air_yards_share),
      wopr = as.numeric(wopr)
    )
  
  arrow::write_parquet(wrte_season_sql, output_path)
  invisible(wrte_season_sql)
}

#' Format Career QB Stats for SQL Import
#'
#' Transforms `career_stats_qb.parquet` into SQL-ready structure.
#'
#' @param input_path Path to career-level QB stats Parquet file.
#' @param output_path Path to write SQL-compatible output (default: "career_stats_qb_tbl.parquet").
#' @return Cleaned career QB stats table (invisibly).
#' @export
format_career_qb_stats_for_sql <- function(input_path, output_path = "career_stats_qb_tbl.parquet") {
  qb_career_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      seasons_played = as.integer(seasons_played),
      games_played = as.integer(games_played),
      completions = as.integer(completions),
      attempts = as.integer(attempts),
      passing_yards = as.numeric(passing_yards),
      passing_tds = as.integer(passing_tds),
      interceptions = as.numeric(interceptions),
      sacks = as.numeric(sacks),
      sack_yards = as.numeric(sack_yards),
      sack_fumbles = as.integer(sack_fumbles),
      sack_fumbles_lost = as.integer(sack_fumbles_lost),
      passing_air_yards = as.numeric(passing_air_yards),
      passing_yards_after_catch = as.numeric(passing_yards_after_catch),
      passing_first_downs = as.numeric(passing_first_downs),
      passing_epa = as.numeric(passing_epa),
      passing_2pt_conversions = as.integer(passing_2pt_conversions),
      carries = as.integer(carries),
      rushing_yards = as.numeric(rushing_yards),
      rushing_tds = as.integer(rushing_tds),
      rushing_fumbles = as.numeric(rushing_fumbles),
      rushing_fumbles_lost = as.numeric(rushing_fumbles_lost),
      rushing_first_downs = as.numeric(rushing_first_downs),
      rushing_epa = as.numeric(rushing_epa),
      rushing_2pt_conversions = as.integer(rushing_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      pacr = as.numeric(pacr),
      dakota = as.numeric(dakota)
    )
  
  arrow::write_parquet(qb_career_sql, output_path)
  invisible(qb_career_sql)
}

#' Format Career RB Stats for SQL Import
#'
#' Transforms `career_stats_rb.parquet` into a SQL-ingestable format.
#'
#' @param input_path Path to RB career-level stats file.
#' @param output_path Path to write SQL-ready output (default: "career_stats_rb_tbl.parquet").
#' @return SQL-normalized RB stats data (invisibly).
#' @export
format_career_rb_stats_for_sql <- function(input_path, output_path = "career_stats_rb_tbl.parquet") {
  rb_career_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      seasons_played = as.integer(seasons_played),
      games_played = as.integer(games_played),
      carries = as.integer(carries),
      rushing_yards = as.numeric(rushing_yards),
      rushing_tds = as.integer(rushing_tds),
      rushing_epa = as.numeric(rushing_epa),
      rushing_fumbles = as.numeric(rushing_fumbles),
      rushing_fumbles_lost = as.numeric(rushing_fumbles_lost),
      rushing_first_downs = as.numeric(rushing_first_downs),
      rushing_2pt_conversions = as.integer(rushing_2pt_conversions),
      targets = as.integer(targets),
      receptions = as.integer(receptions),
      receiving_yards = as.numeric(receiving_yards),
      receiving_tds = as.integer(receiving_tds),
      receiving_epa = as.numeric(receiving_epa),
      receiving_fumbles = as.numeric(receiving_fumbles),
      receiving_fumbles_lost = as.numeric(receiving_fumbles_lost),
      receiving_air_yards = as.numeric(receiving_air_yards),
      receiving_yards_after_catch = as.numeric(receiving_yards_after_catch),
      receiving_first_downs = as.numeric(receiving_first_downs),
      receiving_2pt_conversions = as.integer(receiving_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      racr = as.numeric(racr),
      target_share = as.numeric(target_share),
      air_yards_share = as.numeric(air_yards_share),
      wopr = as.numeric(wopr)
    )
  
  arrow::write_parquet(rb_career_sql, output_path)
  invisible(rb_career_sql)
}

#' Format Career WR/TE Stats for SQL Import
#'
#' Transforms either `career_stats_wr.parquet` or `career_stats_te.parquet`
#' into a SQL-compatible format.
#'
#' @param input_path Path to WR or TE career-level stats file.
#' @param output_path Path for SQL-ready file (optional; auto-generated if NULL).
#' @return Normalized career WR/TE stats (invisibly).
#' @export
format_career_wrte_stats_for_sql <- function(input_path, output_path = NULL) {
  if (is.null(output_path)) {
    file_base <- gsub("\\.parquet$", "", basename(input_path))
    output_path <- file.path("data", "for_database", paste0(file_base, "_tbl.parquet"))
  }
  
  wrte_career_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      seasons_played = as.integer(seasons_played),
      games_played = as.integer(games_played),
      targets = as.integer(targets),
      receptions = as.integer(receptions),
      receiving_yards = as.numeric(receiving_yards),
      receiving_tds = as.integer(receiving_tds),
      receiving_fumbles = as.numeric(receiving_fumbles),
      receiving_fumbles_lost = as.numeric(receiving_fumbles_lost),
      receiving_air_yards = as.numeric(receiving_air_yards),
      receiving_yards_after_catch = as.numeric(receiving_yards_after_catch),
      receiving_first_downs = as.numeric(receiving_first_downs),
      receiving_epa = as.numeric(receiving_epa),
      receiving_2pt_conversions = as.integer(receiving_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      racr = as.numeric(racr),
      target_share = as.numeric(target_share),
      air_yards_share = as.numeric(air_yards_share),
      wopr = as.numeric(wopr)
    )
  
  arrow::write_parquet(wrte_career_sql, output_path)
  invisible(wrte_career_sql)
}

#' Format Career WR/TE Stats for SQL Import
#'
#' Transforms either `career_stats_wr.parquet` or `career_stats_te.parquet`
#' into a SQL-compatible format.
#'
#' @param input_path Path to WR or TE career-level stats file.
#' @param output_path Path for SQL-ready file (optional; auto-generated if NULL).
#' @return Normalized career WR/TE stats (invisibly).
#' @export
format_career_wrte_stats_for_sql <- function(input_path, output_path = NULL) {
  if (is.null(output_path)) {
    file_base <- gsub("\\.parquet$", "", basename(input_path))
    output_path <- file.path("data", "for_database", paste0(file_base, "_tbl.parquet"))
  }
  
  wrte_career_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_id = as.character(player_id),
      full_name = as.character(full_name),
      position = as.character(position),
      recent_team = as.character(recent_team),
      seasons_played = as.integer(seasons_played),
      games_played = as.integer(games_played),
      targets = as.integer(targets),
      receptions = as.integer(receptions),
      receiving_yards = as.numeric(receiving_yards),
      receiving_tds = as.integer(receiving_tds),
      receiving_fumbles = as.numeric(receiving_fumbles),
      receiving_fumbles_lost = as.numeric(receiving_fumbles_lost),
      receiving_air_yards = as.numeric(receiving_air_yards),
      receiving_yards_after_catch = as.numeric(receiving_yards_after_catch),
      receiving_first_downs = as.numeric(receiving_first_downs),
      receiving_epa = as.numeric(receiving_epa),
      receiving_2pt_conversions = as.integer(receiving_2pt_conversions),
      fantasy_points = as.numeric(fantasy_points),
      fantasy_points_ppr = as.numeric(fantasy_points_ppr),
      racr = as.numeric(racr),
      target_share = as.numeric(target_share),
      air_yards_share = as.numeric(air_yards_share),
      wopr = as.numeric(wopr)
    )
  
  arrow::write_parquet(wrte_career_sql, output_path)
  invisible(wrte_career_sql)
}

#' Format Weekly Injury Data for SQL Import
#'
#' Transforms `injuries_weekly.parquet` into a normalized format for database ingestion.
#'
#' @param input_path Path to the weekly injury Parquet file.
#' @param output_path SQL-ready file output path (default: "injuries_weekly_tbl.parquet").
#' @return Cleaned injury data for SQL (invisibly).
#' @export
format_injuries_weekly_for_sql <- function(input_path, output_path = "injuries_weekly_tbl.parquet") {
  injuries_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      team = as.character(team),
      gsis_id = as.character(gsis_id),
      full_name = as.character(full_name),
      position = as.character(position),
      report_status = as.character(report_status),
      injury_reported = as.logical(injury_reported),
      did_not_practice = as.logical(did_not_practice),
      injury_status = as.character(injury_status),
      practice_status = as.character(practice_status),
      primary_injury = as.character(primary_injury),
      secondary_injury = as.logical(secondary_injury)
    )
  
  arrow::write_parquet(injuries_sql, output_path)
  invisible(injuries_sql)
}

#' Format Weekly Team Injury Stats for SQL Import
#'
#' Transforms `injuries_team_weekly.parquet` into a SQL-compatible format.
#'
#' @param input_path Path to the weekly team injuries file.
#' @param output_path Output path for SQL-formatted file (default: "injuries_team_weekly_tbl.parquet").
#' @return SQL-ready weekly team injury table (invisibly).
#' @export
format_injuries_team_weekly_for_sql <- function(input_path, output_path = "injuries_team_weekly_tbl.parquet") {
  injuries_team_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      team = as.character(team),
      weekly_injuries = as.integer(weekly_injuries),
      cumulative_injuries = as.integer(cumulative_injuries)
    )
  
  arrow::write_parquet(injuries_team_sql, output_path)
  invisible(injuries_team_sql)
}

#' Format Team Season Injury Stats for SQL Import
#'
#' Prepares `injuries_team_season.parquet` for SQL ingestion.
#'
#' @param input_path Path to the team-season injury file.
#' @param output_path Path for SQL-ready output (default: "injuries_team_season_tbl.parquet").
#' @return Season-long team injury totals (invisibly).
#' @export
format_injuries_team_season_for_sql <- function(input_path, output_path = "injuries_team_season_tbl.parquet") {
  injuries_team_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      team = as.character(team),
      season_injuries = as.integer(season_injuries)
    )
  
  arrow::write_parquet(injuries_team_season_sql, output_path)
  invisible(injuries_team_season_sql)
}

#' Format Weekly Team Position Injury Stats for SQL Import
#'
#' Transforms `injuries_position_weekly.parquet` into a normalized SQL-ready format.
#'
#' @param input_path Path to the weekly team-position injury file.
#' @param output_path Output path for SQL-compatible file (default: "injuries_position_weekly_tbl.parquet").
#' @return Cleaned injury summary by team and position (invisibly).
#' @export
format_injuries_team_position_weekly_for_sql <- function(input_path, output_path = "injuries_position_weekly_tbl.parquet") {
  injuries_pos_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      team = as.character(team),
      position = as.character(position),
      position_injuries = as.integer(position_injuries),
      cumulative_position_injuries = as.integer(cumulative_position_injuries)
    )
  
  arrow::write_parquet(injuries_pos_sql, output_path)
  invisible(injuries_pos_sql)
}

#' Format QB Contracts for SQL Import
#'
#' Prepares `contracts_qb.parquet` for ingestion into a relational database.
#'
#' @param input_path Path to the QB contracts Parquet file.
#' @param output_path Path for SQL-ready output (default: "contracts_qb_tbl.parquet").
#' @return Cleaned and typed QB contracts table (invisibly).
#' @export
format_contracts_qb_for_sql <- function(input_path, output_path = "contracts_qb_tbl.parquet") {
  contracts_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player = as.character(player),
      position = as.character(position),
      team = as.character(team),
      is_active = as.logical(is_active),
      year_signed = as.integer(year_signed),
      years = as.integer(years),
      value = as.numeric(value),
      apy = as.numeric(apy),
      guaranteed = as.numeric(guaranteed),
      apy_cap_pct = as.numeric(apy_cap_pct),
      inflated_value = as.numeric(inflated_value),
      inflated_apy = as.numeric(inflated_apy),
      inflated_guaranteed = as.numeric(inflated_guaranteed),
      player_page = as.character(player_page),
      otc_id = as.character(otc_id),
      gsis_id = as.character(gsis_id),
      date_of_birth = as.character(date_of_birth),  # You may want to coerce to Date if formatted cleanly
      height = as.character(height),
      weight = as.numeric(weight),
      college = as.character(college),
      draft_year = as.integer(draft_year),
      draft_round = as.integer(draft_round),
      draft_overall = as.integer(draft_overall),
      draft_team = as.character(draft_team),
      contract_start = as.integer(contract_start),
      contract_end = as.integer(contract_end),
      contract_id = as.integer(contract_id),
      teams_played_for = as.integer(teams_played_for)
    )
  
  arrow::write_parquet(contracts_sql, output_path)
  invisible(contracts_sql)
}

#' Format Position-Level Contract Cap Percentages for SQL Import
#'
#' Prepares `contracts_position_cap_pct.parquet` for SQL ingestion.
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path to SQL-ready file (default: "contracts_position_cap_pct_tbl.parquet").
#' @return Cleaned position-level contract summary (invisibly).
#' @export
format_contracts_cap_pct_for_sql <- function(input_path, output_path = "contracts_position_cap_pct_tbl.parquet") {
  contracts_pct_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      position = as.character(position),
      year_signed = as.integer(year_signed),
      team = as.character(team),
      avg_apy_cap_pct = as.numeric(avg_apy_cap_pct),
      total_apy = as.numeric(total_apy),
      count = as.integer(count)
    )
  
  arrow::write_parquet(contracts_pct_sql, output_path)
  invisible(contracts_pct_sql)
}

#' Format Weekly Special Teams Stats for SQL Import
#'
#' Transforms `st_player_stats_weekly.parquet` into SQL-compatible format.
#'
#' @param input_path Path to the weekly special teams stats file.
#' @param output_path SQL-ready file output path (default: "st_player_stats_weekly_tbl.parquet").
#' @return Normalized weekly special teams table (invisibly).
#' @export
format_weekly_special_teams_for_sql <- function(input_path, output_path = "st_player_stats_weekly_tbl.parquet") {
  st_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      season_type = as.character(season_type),
      player_id = as.character(player_id),
      player_name = as.character(player_name),
      position = as.character(position),
      team = as.character(team),
      fg_made = as.integer(fg_made),
      fg_att = as.integer(fg_att),
      fg_made_0_19 = as.integer(fg_made_0_19),
      fg_made_20_29 = as.integer(fg_made_20_29),
      fg_made_30_39 = as.integer(fg_made_30_39),
      fg_made_40_49 = as.integer(fg_made_40_49),
      fg_made_50_59 = as.integer(fg_made_50_59),
      fg_made_60 = as.integer(fg_made_60),
      fg_missed_0_19 = as.integer(fg_missed_0_19),
      fg_missed_20_29 = as.integer(fg_missed_20_29),
      fg_missed_30_39 = as.integer(fg_missed_30_39),
      fg_missed_40_49 = as.integer(fg_missed_40_49),
      fg_missed_50_59 = as.integer(fg_missed_50_59),
      fg_missed_60 = as.integer(fg_missed_60),
      cumulative_fg_made = as.integer(cumulative_fg_made),
      cumulative_fg_att = as.integer(cumulative_fg_att),
      cumulative_fg_made_0_19 = as.integer(cumulative_fg_made_0_19),
      cumulative_fg_made_20_29 = as.integer(cumulative_fg_made_20_29),
      cumulative_fg_made_30_39 = as.integer(cumulative_fg_made_30_39),
      cumulative_fg_made_40_49 = as.integer(cumulative_fg_made_40_49),
      cumulative_fg_made_50_59 = as.integer(cumulative_fg_made_50_59),
      cumulative_fg_made_60 = as.integer(cumulative_fg_made_60),
      cumulative_fg_missed_0_19 = as.integer(cumulative_fg_missed_0_19),
      cumulative_fg_missed_20_29 = as.integer(cumulative_fg_missed_20_29),
      cumulative_fg_missed_30_39 = as.integer(cumulative_fg_missed_30_39),
      cumulative_fg_missed_40_49 = as.integer(cumulative_fg_missed_40_49),
      cumulative_fg_missed_50_59 = as.integer(cumulative_fg_missed_50_59),
      cumulative_fg_missed_60 = as.integer(cumulative_fg_missed_60)
    )
  
  arrow::write_parquet(st_sql, output_path)
  invisible(st_sql)
}

#' Format Season Special Teams Stats for SQL Import
#'
#' Prepares `st_player_stats_season.parquet` for SQL ingestion.
#'
#' @param input_path Path to the season-level special teams file.
#' @param output_path Output path for SQL-ready file (default: "st_player_stats_season_tbl.parquet").
#' @return Normalized special teams season stats (invisibly).
#' @export
format_season_special_teams_for_sql <- function(input_path, output_path = "st_player_stats_season_tbl.parquet") {
  st_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      player_id = as.character(player_id),
      player_name = as.character(player_name),
      team = as.character(team),
      position = as.character(position),
      games_played = as.integer(games_played),
      fg_att = as.integer(fg_att),
      fg_made = as.integer(fg_made),
      fg_missed = as.integer(fg_missed),
      fg_blocked = as.integer(fg_blocked),
      fg_pct = as.numeric(fg_pct),
      fg_long = as.numeric(fg_long),
      fg_made_0_19 = as.integer(fg_made_0_19),
      fg_made_20_29 = as.integer(fg_made_20_29),
      fg_made_30_39 = as.integer(fg_made_30_39),
      fg_made_40_49 = as.integer(fg_made_40_49),
      fg_made_50_59 = as.integer(fg_made_50_59),
      fg_made_60 = as.integer(fg_made_60),
      fg_missed_0_19 = as.integer(fg_missed_0_19),
      fg_missed_20_29 = as.integer(fg_missed_20_29),
      fg_missed_30_39 = as.integer(fg_missed_30_39),
      fg_missed_40_49 = as.integer(fg_missed_40_49),
      fg_missed_50_59 = as.integer(fg_missed_50_59),
      fg_missed_60 = as.integer(fg_missed_60),
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
  
  arrow::write_parquet(st_season_sql, output_path)
  invisible(st_season_sql)
}

#' Format Weekly Defensive Player Stats for SQL Import
#'
#' Prepares `def_player_stats_weekly.parquet` for SQL ingestion.
#'
#' @param input_path Path to the weekly defensive player stats file.
#' @param output_path Output path for SQL-ready file (default: "def_player_stats_weekly_tbl.parquet").
#' @return Normalized weekly defensive stats (invisibly).
#' @export
format_weekly_defense_player_stats_for_sql <- function(input_path, output_path = "def_player_stats_weekly_tbl.parquet") {
  weekly_def_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      season_type = as.character(season_type),
      player_id = as.character(player_id),
      team = as.character(team),
      position = as.character(position),
      position_group = as.character(position_group),
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
    )
  
  arrow::write_parquet(weekly_def_sql, output_path)
  invisible(weekly_def_sql)
}

#' Format Season Defensive Player Stats for SQL Import
#'
#' Prepares `def_player_stats_season.parquet` for SQL ingestion.
#'
#' @param input_path Path to the season-level defensive player stats file.
#' @param output_path Output path for SQL-ready file (default: "def_player_stats_season_tbl.parquet").
#' @return Normalized season defensive stats (invisibly).
#' @export
format_season_defense_player_stats_for_sql <- function(input_path, output_path = "def_player_stats_season_tbl.parquet") {
  season_def_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      player_id = as.character(player_id),
      team = as.character(team),
      position = as.character(position),
      position_group = as.character(position_group),
      games_played = as.integer(games_played),
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
    )
  
  arrow::write_parquet(season_def_sql, output_path)
  invisible(season_def_sql)
}

#' Format Career Defensive Player Stats for SQL Import
#'
#' Prepares `def_player_stats_career.parquet` for SQL ingestion.
#'
#' @param input_path Path to the career-level defensive player stats file.
#' @param output_path Output path for SQL-ready file (default: "def_player_stats_career_tbl.parquet").
#' @return Normalized career defensive stats (invisibly).
#' @export
format_career_defense_player_stats_for_sql <- function(input_path, output_path = "def_player_stats_career_tbl.parquet") {
  career_def_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_id = as.character(player_id),
      position = as.character(position),
      position_group = as.character(position_group),
      last_team = as.character(last_team),
      seasons_played = as.integer(seasons_played),
      games_played = as.integer(games_played),
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
    )
  
  arrow::write_parquet(career_def_sql, output_path)
  invisible(career_def_sql)
}

#' Format Season Defensive Team Stats for SQL Import
#'
#' Prepares `def_team_stats_season.parquet` for SQL ingestion.
#'
#' @param input_path Path to the season-level team defense stats file.
#' @param output_path Output path for SQL-ready file (default: "def_team_stats_season_tbl.parquet").
#' @return Normalized season team defensive stats (invisibly).
#' @export
format_season_defense_team_stats_for_sql <- function(input_path, output_path = "def_team_stats_season_tbl.parquet") {
  season_def_team_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      team = as.character(team),
      n_players = as.integer(n_players),
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
    )
  
  arrow::write_parquet(season_def_team_sql, output_path)
  invisible(season_def_team_sql)
}

#' Format Depth Chart Player Starts for SQL Import
#'
#' Prepares `depth_charts_player_starts.parquet` for SQL ingestion.
#'
#' @param input_path Path to the depth chart player starts file.
#' @param output_path Output path for SQL-ready file (default: "depth_charts_player_starts_tbl.parquet").
#' @return Normalized player starts by position and team (invisibly).
#' @export
format_depth_chart_player_starts_for_sql <- function(input_path, output_path = "depth_charts_player_starts_tbl.parquet") {
  player_starts_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      team = as.character(team),
      position = as.character(position),
      gsis_id = as.character(gsis_id),
      total_starts = as.integer(total_starts)
    )
  
  arrow::write_parquet(player_starts_sql, output_path)
  invisible(player_starts_sql)
}

#' Format Depth Chart Position Stability for SQL Import
#'
#' Prepares `depth_charts_position_stability.parquet` for SQL ingestion.
#'
#' @param input_path Path to the depth chart position stability file.
#' @param output_path Output path for SQL-ready file (default: "depth_charts_position_stability_tbl.parquet").
#' @return Normalized weekly position group stability scores (invisibly).
#' @export
format_depth_chart_position_stability_for_sql <- function(input_path, output_path = "depth_charts_position_stability_tbl.parquet") {
  stability_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      team = as.character(team),
      position = as.character(position),
      position_group_score = as.numeric(position_group_score)
    )
  
  arrow::write_parquet(stability_sql, output_path)
  invisible(stability_sql)
}

#' Format Depth Chart QB Starts for SQL Import
#'
#' Prepares `depth_charts_qb_team.parquet` for SQL ingestion.
#'
#' @param input_path Path to the depth chart QB team file.
#' @param output_path Output path for SQL-ready file (default: "depth_charts_qb_team_tbl.parquet").
#' @return Normalized QB starter counts per team (invisibly).
#' @export
format_depth_chart_qb_for_sql <- function(input_path, output_path = "depth_charts_qb_team_tbl.parquet") {
  qb_starts_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      team = as.character(team),
      distinct_qb_starters = as.integer(distinct_qb_starters)
    )
  
  arrow::write_parquet(qb_starts_sql, output_path)
  invisible(qb_starts_sql)
}

#' Format Depth Chart Starters for SQL Import
#'
#' Prepares `depth_charts_starters.parquet` for SQL ingestion.
#'
#' @param input_path Path to the depth chart starters file.
#' @param output_path Output path for SQL-ready file (default: "depth_charts_starters_tbl.parquet").
#' @return Normalized weekly team starter details (invisibly).
#' @export
format_depth_chart_starters_for_sql <- function(input_path, output_path = "depth_charts_starters_tbl.parquet") {
  starters_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      week = as.integer(week),
      team = as.character(team),
      player = as.character(player),
      position = as.character(position),
      gsis_id = as.character(gsis_id),
      position_group = as.character(position_group),
      player_starts = as.integer(player_starts),
      is_new_starter = as.logical(is_new_starter)
    )
  
  arrow::write_parquet(starters_sql, output_path)
  invisible(starters_sql)
}

#' Format Weekly Next Gen Stats for SQL Import
#'
#' Prepares `nextgen_stats_player_weekly.parquet` for SQL ingestion.
#'
#' @param input_path Path to the weekly Next Gen Stats file.
#' @param output_path Output path for SQL-ready file (default: "nextgen_stats_player_weekly_tbl.parquet").
#' @return Normalized weekly Next Gen Stats (invisibly).
#' @export
format_weekly_nextgen_stats_for_sql <- function(input_path, output_path = "nextgen_stats_player_weekly_tbl.parquet") {
  ngs_weekly_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      season_type = as.character(season_type),
      week = as.integer(week),
      player_gsis_id = as.character(player_gsis_id),
      team_abbr = as.character(team_abbr),
      player_position = as.character(player_position),
      avg_time_to_throw = as.numeric(avg_time_to_throw),
      avg_completed_air_yards = as.numeric(avg_completed_air_yards),
      avg_intended_air_yards = as.numeric(avg_intended_air_yards),
      avg_air_yards_differential = as.numeric(avg_air_yards_differential),
      aggressiveness = as.numeric(aggressiveness),
      max_completed_air_distance = as.numeric(max_completed_air_distance),
      avg_air_yards_to_sticks = as.numeric(avg_air_yards_to_sticks),
      attempts = as.integer(attempts),
      completions = as.integer(completions),
      pass_yards = as.integer(pass_yards),
      pass_touchdowns = as.integer(pass_touchdowns),
      interceptions = as.integer(interceptions),
      passer_rating = as.numeric(passer_rating),
      completion_percentage = as.numeric(completion_percentage),
      expected_completion_percentage = as.numeric(expected_completion_percentage),
      completion_percentage_above_expectation = as.numeric(completion_percentage_above_expectation),
      avg_air_distance = as.numeric(avg_air_distance),
      max_air_distance = as.numeric(max_air_distance)
    )
  
  arrow::write_parquet(ngs_weekly_sql, output_path)
  invisible(ngs_weekly_sql)
}

#' Format Season Next Gen Stats for SQL Import
#'
#' Prepares `nextgen_stats_player_season.parquet` for SQL ingestion.
#'
#' @param input_path Path to the season-level Next Gen Stats file.
#' @param output_path Output path for SQL-ready file (default: "nextgen_stats_player_season_tbl.parquet").
#' @return Normalized season Next Gen Stats (invisibly).
#' @export
format_season_nextgen_stats_for_sql <- function(input_path, output_path = "nextgen_stats_player_season_tbl.parquet") {
  ngs_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      player_gsis_id = as.character(player_gsis_id),
      team_abbr = as.character(team_abbr),
      player_position = as.character(player_position),
      games_played = as.integer(games_played),
      attempts = as.integer(attempts),
      completions = as.integer(completions),
      pass_yards = as.integer(pass_yards),
      pass_touchdowns = as.integer(pass_touchdowns),
      interceptions = as.integer(interceptions),
      avg_attempts = as.numeric(avg_attempts),
      avg_completions = as.numeric(avg_completions),
      avg_pass_yards = as.numeric(avg_pass_yards),
      avg_pass_touchdowns = as.numeric(avg_pass_touchdowns),
      avg_interceptions = as.numeric(avg_interceptions),
      avg_time_to_throw = as.numeric(avg_time_to_throw),
      avg_completed_air_yards = as.numeric(avg_completed_air_yards),
      avg_intended_air_yards = as.numeric(avg_intended_air_yards),
      avg_air_yards_differential = as.numeric(avg_air_yards_differential),
      aggressiveness = as.numeric(aggressiveness),
      max_completed_air_distance = as.numeric(max_completed_air_distance),
      avg_air_yards_to_sticks = as.numeric(avg_air_yards_to_sticks),
      completion_percentage = as.numeric(completion_percentage),
      expected_completion_percentage = as.numeric(expected_completion_percentage),
      completion_percentage_above_expectation = as.numeric(completion_percentage_above_expectation),
      avg_air_distance = as.numeric(avg_air_distance),
      max_air_distance = as.numeric(max_air_distance),
      passer_rating = as.numeric(passer_rating)
    )
  
  arrow::write_parquet(ngs_season_sql, output_path)
  invisible(ngs_season_sql)
}

#' Format Postseason Next Gen Stats for SQL Import
#'
#' Prepares `nextgen_stats_player_postseason.parquet` for SQL ingestion.
#'
#' @param input_path Path to the postseason-level Next Gen Stats file.
#' @param output_path Output path for SQL-ready file (default: "nextgen_stats_player_postseason_tbl.parquet").
#' @return Normalized postseason Next Gen Stats (invisibly).
#' @export
format_postseason_nextgen_stats_for_sql <- function(input_path, output_path = "nextgen_stats_player_postseason_tbl.parquet") {
  ngs_post_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_gsis_id = as.character(player_gsis_id),
      team_abbr = as.character(team_abbr),
      player_position = as.character(player_position),
      games_played = as.integer(games_played),
      attempts = as.integer(attempts),
      completions = as.integer(completions),
      pass_yards = as.integer(pass_yards),
      pass_touchdowns = as.integer(pass_touchdowns),
      interceptions = as.integer(interceptions),
      avg_attempts = as.numeric(avg_attempts),
      avg_completions = as.numeric(avg_completions),
      avg_pass_yards = as.numeric(avg_pass_yards),
      avg_pass_touchdowns = as.numeric(avg_pass_touchdowns),
      avg_interceptions = as.numeric(avg_interceptions),
      avg_time_to_throw = as.numeric(avg_time_to_throw),
      avg_completed_air_yards = as.numeric(avg_completed_air_yards),
      avg_intended_air_yards = as.numeric(avg_intended_air_yards),
      avg_air_yards_differential = as.numeric(avg_air_yards_differential),
      aggressiveness = as.numeric(aggressiveness),
      max_completed_air_distance = as.numeric(max_completed_air_distance),
      avg_air_yards_to_sticks = as.numeric(avg_air_yards_to_sticks),
      completion_percentage = as.numeric(completion_percentage),
      expected_completion_percentage = as.numeric(expected_completion_percentage),
      completion_percentage_above_expectation = as.numeric(completion_percentage_above_expectation),
      avg_air_distance = as.numeric(avg_air_distance),
      max_air_distance = as.numeric(max_air_distance),
      passer_rating = as.numeric(passer_rating)
    )
  
  arrow::write_parquet(ngs_post_sql, output_path)
  invisible(ngs_post_sql)
}

#' Format Career Next Gen Stats for SQL Import
#'
#' Prepares `nextgen_stats_player_career.parquet` for SQL ingestion.
#'
#' @param input_path Path to the career-level Next Gen Stats file.
#' @param output_path Output path for SQL-ready file (default: "nextgen_stats_player_career_tbl.parquet").
#' @return Normalized career Next Gen Stats (invisibly).
#' @export
format_career_nextgen_stats_for_sql <- function(input_path, output_path = "nextgen_stats_player_career_tbl.parquet") {
  ngs_career_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_gsis_id = as.character(player_gsis_id),
      team_abbr = as.character(team_abbr),
      player_position = as.character(player_position),
      games_played = as.integer(games_played),
      attempts = as.integer(attempts),
      completions = as.integer(completions),
      pass_yards = as.integer(pass_yards),
      pass_touchdowns = as.integer(pass_touchdowns),
      interceptions = as.integer(interceptions),
      avg_attempts = as.numeric(avg_attempts),
      avg_completions = as.numeric(avg_completions),
      avg_pass_yards = as.numeric(avg_pass_yards),
      avg_pass_touchdowns = as.numeric(avg_pass_touchdowns),
      avg_interceptions = as.numeric(avg_interceptions),
      avg_time_to_throw = as.numeric(avg_time_to_throw),
      avg_completed_air_yards = as.numeric(avg_completed_air_yards),
      avg_intended_air_yards = as.numeric(avg_intended_air_yards),
      avg_air_yards_differential = as.numeric(avg_air_yards_differential),
      aggressiveness = as.numeric(aggressiveness),
      max_completed_air_distance = as.numeric(max_completed_air_distance),
      avg_air_yards_to_sticks = as.numeric(avg_air_yards_to_sticks),
      completion_percentage = as.numeric(completion_percentage),
      expected_completion_percentage = as.numeric(expected_completion_percentage),
      completion_percentage_above_expectation = as.numeric(completion_percentage_above_expectation),
      avg_air_distance = as.numeric(avg_air_distance),
      max_air_distance = as.numeric(max_air_distance),
      passer_rating = as.numeric(passer_rating)
    )
  
  arrow::write_parquet(ngs_career_sql, output_path)
  invisible(ngs_career_sql)
}

#' Format Offensive PBP Participation for SQL Import
#'
#' Prepares `participation_offense_pbp.parquet` for SQL ingestion.
#'
#' @param input_path Path to the offensive play-by-play participation file.
#' @param output_path Output path for SQL-ready file (default: "participation_offense_pbp_tbl.parquet").
#' @return Normalized offensive participation (play-level) (invisibly).
#' @export
format_participation_offense_pbp_for_sql <- function(input_path, output_path = "participation_offense_pbp_tbl.parquet") {
  offense_pbp_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      game_id = as.character(game_id),
      play_id = as.integer(play_id),
      season = as.integer(season),
      week = as.integer(week),
      team = as.character(team),
      play_type = as.character(play_type),
      offense_formation = as.character(offense_formation),
      offense_personnel = as.character(offense_personnel),
      n_offense = as.integer(n_offense),
      ngs_air_yards = as.numeric(ngs_air_yards),
      time_to_throw = as.numeric(time_to_throw),
      was_pressure = as.logical(was_pressure),
      route = as.character(route),
      pressures_allowed = as.integer(pressures_allowed)
    )
  
  arrow::write_parquet(offense_pbp_sql, output_path)
  invisible(offense_pbp_sql)
}

#' Format Offensive Game Participation for SQL Import
#'
#' Prepares `participation_offense_game.parquet` for SQL ingestion.
#'
#' @param input_path Path to the offensive game-level participation file.
#' @param output_path Output path for SQL-ready file (default: "participation_offense_game_tbl.parquet").
#' @return Normalized offensive participation (game-level) (invisibly).
#' @export
format_participation_offense_game_for_sql <- function(input_path, output_path = "participation_offense_game_tbl.parquet") {
  offense_game_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      game_id = as.character(game_id),
      team = as.character(team),
      season = as.integer(season),
      week = as.integer(week),
      n_plays = as.integer(n_plays),
      n_pass = as.integer(n_pass),
      n_run = as.integer(n_run),
      n_empty = as.integer(n_empty),
      n_i_form = as.integer(n_i_form),
      n_jumbo = as.integer(n_jumbo),
      n_pistol = as.integer(n_pistol),
      n_shotgun = as.integer(n_shotgun),
      n_singleback = as.integer(n_singleback),
      n_wildcat = as.integer(n_wildcat),
      n_other_formations = as.integer(n_other_formations),
      n_angle = as.integer(n_angle),
      n_corner = as.integer(n_corner),
      n_cross = as.integer(n_cross),
      n_flat = as.integer(n_flat),
      n_go = as.integer(n_go),
      n_hitch = as.integer(n_hitch),
      n_in = as.integer(n_in),
      n_out = as.integer(n_out),
      n_post = as.integer(n_post),
      n_screen = as.integer(n_screen),
      n_slant = as.integer(n_slant),
      n_wheel = as.integer(n_wheel),
      n_other_routes = as.integer(n_other_routes),
      avg_time_to_throw = as.numeric(avg_time_to_throw),
      pressures_allowed = as.integer(pressures_allowed),
      cumulative_plays = as.integer(cumulative_plays),
      cumulative_pass = as.integer(cumulative_pass),
      cumulative_run = as.integer(cumulative_run),
      cumulative_pressures_allowed = as.integer(cumulative_pressures_allowed),
      cumulative_screen = as.integer(cumulative_screen),
      cumulative_flat = as.integer(cumulative_flat),
      cumulative_other_routes = as.integer(cumulative_other_routes),
      avg_time_to_throw_to_date = as.numeric(avg_time_to_throw_to_date)
    )
  
  arrow::write_parquet(offense_game_sql, output_path)
  invisible(offense_game_sql)
}

#' Format Offensive Season Participation for SQL Import
#'
#' Prepares `participation_offense_season.parquet` for SQL ingestion.
#'
#' @param input_path Path to the offensive season-level participation file.
#' @param output_path Output path for SQL-ready file (default: "participation_offense_season_tbl.parquet").
#' @return Normalized offensive participation (season-level) (invisibly).
#' @export
format_participation_offense_season_for_sql <- function(input_path, output_path = "participation_offense_season_tbl.parquet") {
  offense_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      team = as.character(team),
      n_plays = as.integer(n_plays),
      n_pass = as.integer(n_pass),
      n_run = as.integer(n_run),
      n_empty = as.integer(n_empty),
      n_i_form = as.integer(n_i_form),
      n_jumbo = as.integer(n_jumbo),
      n_pistol = as.integer(n_pistol),
      n_shotgun = as.integer(n_shotgun),
      n_singleback = as.integer(n_singleback),
      n_wildcat = as.integer(n_wildcat),
      n_other_formations = as.integer(n_other_formations),
      n_angle = as.integer(n_angle),
      n_corner = as.integer(n_corner),
      n_cross = as.integer(n_cross),
      n_flat = as.integer(n_flat),
      n_go = as.integer(n_go),
      n_hitch = as.integer(n_hitch),
      n_in = as.integer(n_in),
      n_out = as.integer(n_out),
      n_post = as.integer(n_post),
      n_screen = as.integer(n_screen),
      n_slant = as.integer(n_slant),
      n_wheel = as.integer(n_wheel),
      n_other_routes = as.integer(n_other_routes),
      avg_time_to_throw = as.numeric(avg_time_to_throw),
      pressures_allowed = as.integer(pressures_allowed)
    )
  
  arrow::write_parquet(offense_season_sql, output_path)
  invisible(offense_season_sql)
}

#' Format Defensive PBP Participation for SQL Import
#'
#' Prepares `participation_defense_pbp.parquet` for SQL ingestion.
#'
#' @param input_path Path to the defensive play-by-play participation file.
#' @param output_path Output path for SQL-ready file (default: "participation_defense_pbp_tbl.parquet").
#' @return Normalized defensive participation (play-level) (invisibly).
#' @export
format_participation_defense_pbp_for_sql <- function(input_path, output_path = "participation_defense_pbp_tbl.parquet") {
  defense_pbp_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      game_id = as.character(game_id),
      play_id = as.integer(play_id),
      season = as.integer(season),
      week = as.integer(week),
      defense_team = as.character(defense_team),
      play_type = as.character(play_type),
      possession_team = as.character(possession_team),
      defense_personnel = as.character(defense_personnel),
      defenders_in_box = as.integer(defenders_in_box),
      number_of_pass_rushers = as.integer(number_of_pass_rushers),
      defense_man_zone_type = as.character(defense_man_zone_type),
      defense_coverage_type = as.character(defense_coverage_type),
      time_to_throw = as.numeric(time_to_throw),
      was_pressure = as.logical(was_pressure),
      team1 = as.character(team1),
      team2 = as.character(team2),
      rush_bin = as.character(rush_bin),
      box_bin = as.character(box_bin),
      cumulative_pass = as.integer(cumulative_pass),
      cumulative_run = as.integer(cumulative_run),
      cumulative_low_rush = as.integer(cumulative_low_rush),
      cumulative_standard_rush = as.integer(cumulative_standard_rush),
      cumulative_blitz = as.integer(cumulative_blitz),
      cumulative_heavy_blitz = as.integer(cumulative_heavy_blitz),
      cumulative_light_box = as.integer(cumulative_light_box),
      cumulative_standard_box = as.integer(cumulative_standard_box),
      cumulative_stacked_box = as.integer(cumulative_stacked_box),
      cumulative_man = as.integer(cumulative_man),
      cumulative_zone = as.integer(cumulative_zone),
      cumulative_cover_0 = as.integer(cumulative_cover_0),
      cumulative_cover_1 = as.integer(cumulative_cover_1),
      cumulative_cover_2 = as.integer(cumulative_cover_2),
      cumulative_cover_3 = as.integer(cumulative_cover_3),
      cumulative_cover_4 = as.integer(cumulative_cover_4),
      cumulative_cover_6 = as.integer(cumulative_cover_6),
      cumulative_cover_2_man = as.integer(cumulative_cover_2_man),
      cumulative_prevent = as.integer(cumulative_prevent)
    )
  
  arrow::write_parquet(defense_pbp_sql, output_path)
  invisible(defense_pbp_sql)
}

#' Format Defensive Game Participation for SQL Import
#'
#' Prepares `participation_defense_game.parquet` for SQL ingestion.
#'
#' @param input_path Path to the defensive game-level participation file.
#' @param output_path Output path for SQL-ready file (default: "participation_defense_game_tbl.parquet").
#' @return Normalized defensive participation (game-level) (invisibly).
#' @export
format_participation_defense_game_for_sql <- function(input_path, output_path = "participation_defense_game_tbl.parquet") {
  defense_game_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      game_id = as.character(game_id),
      defense_team = as.character(defense_team),
      season = as.integer(season),
      week = as.integer(week),
      n_plays = as.integer(n_plays),
      n_pass = as.integer(n_pass),
      n_run = as.integer(n_run),
      n_low_rush = as.integer(n_low_rush),
      n_standard_rush = as.integer(n_standard_rush),
      n_blitz = as.integer(n_blitz),
      n_heavy_blitz = as.integer(n_heavy_blitz),
      n_light_box = as.integer(n_light_box),
      n_standard_box = as.integer(n_standard_box),
      n_stacked_box = as.integer(n_stacked_box),
      n_man = as.integer(n_man),
      n_zone = as.integer(n_zone),
      n_cover_0 = as.integer(n_cover_0),
      n_cover_1 = as.integer(n_cover_1),
      n_cover_2 = as.integer(n_cover_2),
      n_cover_3 = as.integer(n_cover_3),
      n_cover_4 = as.integer(n_cover_4),
      n_cover_6 = as.integer(n_cover_6),
      n_cover_2_man = as.integer(n_cover_2_man),
      n_prevent = as.integer(n_prevent),
      n_pressures = as.integer(n_pressures),
      avg_time_to_throw = as.numeric(avg_time_to_throw),
      cumulative_plays = as.integer(cumulative_plays),
      cumulative_pass = as.integer(cumulative_pass),
      cumulative_run = as.integer(cumulative_run),
      cumulative_low_rush = as.integer(cumulative_low_rush),
      cumulative_standard_rush = as.integer(cumulative_standard_rush),
      cumulative_blitz = as.integer(cumulative_blitz),
      cumulative_heavy_blitz = as.integer(cumulative_heavy_blitz),
      cumulative_light_box = as.integer(cumulative_light_box),
      cumulative_standard_box = as.integer(cumulative_standard_box),
      cumulative_stacked_box = as.integer(cumulative_stacked_box),
      cumulative_man = as.integer(cumulative_man),
      cumulative_zone = as.integer(cumulative_zone),
      cumulative_cover_0 = as.integer(cumulative_cover_0),
      cumulative_cover_1 = as.integer(cumulative_cover_1),
      cumulative_cover_2 = as.integer(cumulative_cover_2),
      cumulative_cover_3 = as.integer(cumulative_cover_3),
      cumulative_cover_4 = as.integer(cumulative_cover_4),
      cumulative_cover_6 = as.integer(cumulative_cover_6),
      cumulative_cover_2_man = as.integer(cumulative_cover_2_man),
      cumulative_prevent = as.integer(cumulative_prevent),
      cumulative_pressures = as.integer(cumulative_pressures),
      avg_time_to_throw_to_date = as.numeric(avg_time_to_throw_to_date)
    )
  
  arrow::write_parquet(defense_game_sql, output_path)
  invisible(defense_game_sql)
}

#' Format Defensive Season Participation for SQL Import
#'
#' Prepares `participation_defense_season.parquet` for SQL ingestion.
#'
#' @param input_path Path to the defensive season-level participation file.
#' @param output_path Output path for SQL-ready file (default: "participation_defense_season_tbl.parquet").
#' @return Normalized defensive participation (season-level) (invisibly).
#' @export
format_participation_defense_season_for_sql <- function(input_path, output_path = "participation_defense_season_tbl.parquet") {
  defense_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      defense_team = as.character(defense_team),
      n_plays = as.integer(n_plays),
      n_pass = as.integer(n_pass),
      n_run = as.integer(n_run),
      n_low_rush = as.integer(n_low_rush),
      n_standard_rush = as.integer(n_standard_rush),
      n_blitz = as.integer(n_blitz),
      n_heavy_blitz = as.integer(n_heavy_blitz),
      n_light_box = as.integer(n_light_box),
      n_standard_box = as.integer(n_standard_box),
      n_stacked_box = as.integer(n_stacked_box),
      n_man = as.integer(n_man),
      n_zone = as.integer(n_zone),
      n_cover_0 = as.integer(n_cover_0),
      n_cover_1 = as.integer(n_cover_1),
      n_cover_2 = as.integer(n_cover_2),
      n_cover_3 = as.integer(n_cover_3),
      n_cover_4 = as.integer(n_cover_4),
      n_cover_6 = as.integer(n_cover_6),
      n_cover_2_man = as.integer(n_cover_2_man),
      n_prevent = as.integer(n_prevent),
      n_pressures = as.integer(n_pressures),
      avg_time_to_throw = as.numeric(avg_time_to_throw)
    )
  
  arrow::write_parquet(defense_season_sql, output_path)
  invisible(defense_season_sql)
}

#' Format Team Metadata for SQL Import (Including Logos)
#'
#' Prepares `team_metadata.parquet` for SQL ingestion. Includes team identity,
#' conference/division, colors, and all logo-related URLs.
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path to SQL-ready file (default: "team_metadata_tbl.parquet").
#' @return Cleaned team metadata table (invisibly).
#' @export
format_team_metadata_for_sql <- function(input_path, output_path = "team_metadata_tbl.parquet") {
  team_metadata_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      team_id = as.character(team_id),
      team_abbr = as.character(team_abbr),
      team_name = as.character(team_name),
      team_nick = as.character(team_nick),
      team_conf = as.character(team_conf),
      team_division = as.character(team_division),
      team_color = as.character(team_color),
      team_color2 = as.character(team_color2),
      team_color3 = as.character(team_color3),
      team_color4 = as.character(team_color4),
      team_logo_wikipedia = as.character(team_logo_wikipedia),
      team_logo_espn = as.character(team_logo_espn),
      team_wordmark = as.character(team_wordmark),
      team_conference_logo = as.character(team_conference_logo),
      team_league_logo = as.character(team_league_logo),
      team_logo_squared = as.character(team_logo_squared)
    )
  
  arrow::write_parquet(team_metadata_sql, output_path)
  invisible(team_metadata_sql)
}

#' Format ID Map for SQL Import
#'
#' Prepares `id_map.parquet` for SQL ingestion. Retains all player name and ID fields
#' across multiple platforms (GSIS, ESPN, Sportradar, etc.).
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path to SQL-ready file (default: "id_map_tbl.parquet").
#' @return Cleaned ID map table (invisibly).
#' @export
format_id_map_for_sql <- function(input_path, output_path = "id_map_tbl.parquet") {
  id_map_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      full_name = as.character(full_name),
      first_name = as.character(first_name),
      last_name = as.character(last_name),
      gsis_id = as.character(gsis_id),
      espn_id = as.character(espn_id),
      sportradar_id = as.character(sportradar_id),
      yahoo_id = as.character(yahoo_id),
      rotowire_id = as.character(rotowire_id),
      pff_id = as.character(pff_id),
      pfr_id = as.character(pfr_id),
      fantasy_data_id = as.character(fantasy_data_id),
      sleeper_id = as.character(sleeper_id),
      esb_id = as.character(esb_id),
      gsis_it_id = as.character(gsis_it_id),
      smart_id = as.character(smart_id)
    )
  
  arrow::write_parquet(id_map_sql, output_path)
  invisible(id_map_sql)
}

#' Format Weekly Snap Counts for SQL Import
#'
#' Prepares `snapcount_weekly.parquet` for SQL ingestion. Retains all weekly player snap data
#' across offense, defense, and special teams.
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path to SQL-ready file (default: "snapcount_weekly_tbl.parquet").
#' @return Cleaned weekly snap count table (invisibly).
#' @export
format_snapcount_weekly_for_sql <- function(input_path, output_path = "snapcount_weekly_tbl.parquet") {
  snapcount_weekly_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      game_id = as.character(game_id),
      pfr_game_id = as.character(pfr_game_id),
      season = as.integer(season),
      game_type = as.character(game_type),
      week = as.integer(week),
      player = as.character(player),
      pfr_player_id = as.character(pfr_player_id),
      position = as.character(position),
      team = as.character(team),
      opponent = as.character(opponent),
      offense_snaps = as.numeric(offense_snaps),
      offense_pct = as.numeric(offense_pct),
      defense_snaps = as.numeric(defense_snaps),
      defense_pct = as.numeric(defense_pct),
      st_snaps = as.numeric(st_snaps),
      st_pct = as.numeric(st_pct)
    )
  
  arrow::write_parquet(snapcount_weekly_sql, output_path)
  invisible(snapcount_weekly_sql)
}

#' Format Season Snap Counts for SQL Import
#'
#' Prepares `snapcount_season.parquet` for SQL ingestion. Retains all season-level
#' snap count summaries across offense, defense, and special teams.
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path to SQL-ready file (default: "snapcount_season_tbl.parquet").
#' @return Cleaned season snap count table (invisibly).
#' @export
format_snapcount_season_for_sql <- function(input_path, output_path = "snapcount_season_tbl.parquet") {
  snapcount_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      team = as.character(team),
      player = as.character(player),
      pfr_player_id = as.character(pfr_player_id),
      position = as.character(position),
      games_played = as.integer(games_played),
      offense_games = as.integer(offense_games),
      defense_games = as.integer(defense_games),
      st_games = as.integer(st_games),
      offense_snaps = as.numeric(offense_snaps),
      defense_snaps = as.numeric(defense_snaps),
      st_snaps = as.numeric(st_snaps),
      offense_pct_mean = as.numeric(offense_pct_mean),
      defense_pct_mean = as.numeric(defense_pct_mean),
      st_pct_mean = as.numeric(st_pct_mean)
    )
  
  arrow::write_parquet(snapcount_season_sql, output_path)
  invisible(snapcount_season_sql)
}

#' Format Career Snap Counts for SQL Import
#'
#' Prepares `snapcount_career.parquet` for SQL ingestion. Retains all career-level
#' player snap statistics across offense, defense, and special teams.
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path to SQL-ready file (default: "snapcount_career_tbl.parquet").
#' @return Cleaned career snap count table (invisibly).
#' @export
format_snapcount_career_for_sql <- function(input_path, output_path = "snapcount_career_tbl.parquet") {
  snapcount_career_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player = as.character(player),
      pfr_player_id = as.character(pfr_player_id),
      first_season = as.integer(first_season),
      last_season = as.integer(last_season),
      seasons_played = as.integer(seasons_played),
      teams_played_for = as.integer(teams_played_for),
      games_played = as.integer(games_played),
      offense_games = as.integer(offense_games),
      defense_games = as.integer(defense_games),
      st_games = as.integer(st_games),
      offense_snaps = as.numeric(offense_snaps),
      defense_snaps = as.numeric(defense_snaps),
      st_snaps = as.numeric(st_snaps),
      offense_pct_mean = as.numeric(offense_pct_mean),
      defense_pct_mean = as.numeric(defense_pct_mean),
      st_pct_mean = as.numeric(st_pct_mean)
    )
  
  arrow::write_parquet(snapcount_career_sql, output_path)
  invisible(snapcount_career_sql)
}

#' Format ESPN QBR Season Data for SQL Import
#'
#' Prepares `espn_qbr_season.parquet` for SQL ingestion. Retains all ESPN QBR season-level
#' quarterback metrics, including player identity, team, QBR components, and headshots.
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path to SQL-ready file (default: "espn_qbr_season_tbl.parquet").
#' @return Cleaned QBR season summary table (invisibly).
#' @export
format_espn_qbr_season_for_sql <- function(input_path, output_path = "espn_qbr_season_tbl.parquet") {
  espn_qbr_season_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      season = as.integer(season),
      season_type = as.character(season_type),
      game_week = as.character(game_week),
      team_abb = as.character(team_abb),
      player_id = as.character(player_id),
      name_short = as.character(name_short),
      rank = as.numeric(rank),
      qbr_total = as.numeric(qbr_total),
      pts_added = as.numeric(pts_added),
      qb_plays = as.numeric(qb_plays),
      epa_total = as.numeric(epa_total),
      pass = as.numeric(pass),
      run = as.numeric(run),
      exp_sack = as.numeric(exp_sack),
      penalty = as.numeric(penalty),
      qbr_raw = as.numeric(qbr_raw),
      sack = as.numeric(sack),
      name_first = as.character(name_first),
      name_last = as.character(name_last),
      name_display = as.character(name_display),
      headshot_href = as.character(headshot_href),
      team = as.character(team),
      qualified = as.logical(qualified)
    )
  
  arrow::write_parquet(espn_qbr_season_sql, output_path)
  invisible(espn_qbr_season_sql)
}

#' Format ESPN QBR Career Data for SQL Import
#'
#' Prepares `espn_qbr_career.parquet` for SQL ingestion. Retains all ESPN QBR career-level
#' quarterback statistics, including player identity, QBR components, and cumulative metrics.
#'
#' @param input_path Path to the input Parquet file.
#' @param output_path Path to SQL-ready file (default: "espn_qbr_career_tbl.parquet").
#' @return Cleaned QBR career summary table (invisibly).
#' @export
format_espn_qbr_career_for_sql <- function(input_path, output_path = "espn_qbr_career_tbl.parquet") {
  espn_qbr_career_sql <- arrow::read_parquet(input_path) %>%
    dplyr::transmute(
      player_id = as.character(player_id),
      name_display = as.character(name_display),
      season_type = as.character(season_type),
      first_season = as.integer(first_season),
      last_season = as.integer(last_season),
      seasons_played = as.integer(seasons_played),
      teams_played_for = as.integer(teams_played_for),
      qb_plays = as.numeric(qb_plays),
      qbr_total_w = as.numeric(qbr_total_w),
      qbr_raw_w = as.numeric(qbr_raw_w),
      pts_added = as.numeric(pts_added),
      epa = as.numeric(epa),
      pass = as.numeric(pass),
      run = as.numeric(run),
      sack = as.numeric(sack),
      exp_sack = as.numeric(exp_sack),
      penalty = as.numeric(penalty),
      qualified_seasons = as.integer(qualified_seasons)
    )
  
  arrow::write_parquet(espn_qbr_career_sql, output_path)
  invisible(espn_qbr_career_sql)
}



