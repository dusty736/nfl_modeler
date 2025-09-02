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

#' Add cumulative offensive stats by posteam
add_team_cumulative_stats <- function(df) {
  df %>%
    group_by(game_id, posteam) %>%
    arrange(play_id, .by_group = TRUE) %>%
    mutate(
      cum_play_offense = row_number(),
      cum_yards_offense = cumsum(replace_na(yards_gained, 0)),
      cum_epa_offense = cumsum(replace_na(epa, 0)),
      cum_wpa_offense = cumsum(replace_na(wpa, 0)),
      cum_success_offense = cumsum(replace_na(success, 0)),
      cum_td_offense = cumsum(touchdown == TRUE),
      cum_int_offense = cumsum(interception == TRUE),
      cum_penalty_offense = cumsum(penalty == TRUE),
      cum_pass_attempts = cumsum(pass_attempt == TRUE),
      cum_rush_attempts = cumsum(rush_attempt == TRUE),
      run_pass_ratio = cum_rush_attempts / (cum_pass_attempts + 1e-5)
    ) %>%
    ungroup()
}

#' Add cumulative defensive stats by defteam
add_defense_cumulative_stats <- function(df) {
  df %>%
    group_by(game_id, defteam) %>%
    arrange(play_id, .by_group = TRUE) %>%
    mutate(
      cum_play_defense = row_number(),
      cum_yards_defense = cumsum(replace_na(yards_gained, 0)),
      cum_epa_defense = cumsum(replace_na(epa, 0)),
      cum_success_defense = cumsum(replace_na(success, 0)),
      cum_td_allowed = cumsum(touchdown == TRUE),
      cum_int_defense = cumsum(interception == TRUE),
      cum_penalty_defense = cumsum(penalty == TRUE)
    ) %>%
    ungroup()
}

#' Add situational / context features (no new inputs required)
#'
#' Derives compact, modeling-friendly features from existing play-by-play fields:
#' - Distance & field: distance_cat, field_zone, red_zone, estimated_fg_distance, in_fg_range
#' - Downs: is_third_down, is_fourth_down
#' - Score state: score_state, score_diff_abs, posteam_points/defteam_points via score delta
#' - Leverage & neutrality: leverage (=|wpa|), high_leverage, neutral_script (close/normal context)
#' - Dropbacks & explosives: dropback (pass or sack), explosive_play (rush>=10, pass>=15)
#' - Clock landmarks: two_min_warning_half, late_and_close (Q4 within one score, <=10:00)
#'
#' @param df Tibble from `clean_pbp_data()` (columns as in your example)
#' @param fg_cap Numeric max field goal distance to flag "in range" (default 55)
#' @return Tibble with additional engineered features
add_situational_features <- function(df, fg_cap = 55) {
  dplyr::mutate(
    df,
    # --- distance & field position ---
    distance_cat = dplyr::case_when(
      is.na(ydstogo)        ~ NA_character_,
      ydstogo <= 2          ~ "short",
      ydstogo <= 6          ~ "medium",
      ydstogo <= 10         ~ "long",
      TRUE                  ~ "very_long"
    ),
    distance_cat = factor(distance_cat, levels = c("short","medium","long","very_long")),
    
    field_zone = dplyr::case_when(
      is.na(yardline_100)   ~ NA_character_,
      yardline_100 >= 80    ~ "backed_up",   # inside own 20
      yardline_100 <= 20    ~ "red_zone",
      TRUE                  ~ "midfield"
    ),
    field_zone = factor(field_zone, levels = c("backed_up","midfield","red_zone")),
    red_zone = dplyr::coalesce(yardline_100 <= 20, FALSE),
    
    estimated_fg_distance = dplyr::if_else(!is.na(yardline_100),
                                           yardline_100 + 17, NA_real_),
    in_fg_range = dplyr::coalesce(estimated_fg_distance <= fg_cap, FALSE),
    
    # --- down & distance flags ---
    is_third_down  = dplyr::coalesce(down == 3L, FALSE),
    is_fourth_down = dplyr::coalesce(down == 4L, FALSE),
    
    # --- possession & score state ---
    possession_home = dplyr::coalesce(posteam == home_team, FALSE),
    score_state = dplyr::case_when(
      is.na(score_differential) ~ NA_character_,
      score_differential > 0    ~ "leading",
      score_differential < 0    ~ "trailing",
      TRUE                      ~ "tied"
    ),
    score_state = factor(score_state, levels = c("trailing","tied","leading")),
    score_diff_abs = abs(dplyr::coalesce(score_differential, 0L)),
    
    # --- leverage & neutral context ---
    leverage      = abs(dplyr::coalesce(wpa, 0)),
    high_leverage = leverage >= 0.05,  # tweak if you fancy
    neutral_script =
      dplyr::coalesce(abs(score_differential) <= 7, FALSE) &
      dplyr::between(dplyr::coalesce(wp, 0.5), 0.2, 0.8) &
      dplyr::coalesce(yardline_100 > 20 & yardline_100 < 80, FALSE) &
      dplyr::coalesce(qtr %in% c(1L, 2L, 3L), FALSE),
    
    # --- dropbacks & explosives ---
    dropback = dplyr::coalesce(pass_attempt, FALSE) | dplyr::coalesce(sack, FALSE),
    explosive_play =
      (dplyr::coalesce(rush_attempt, FALSE) & dplyr::coalesce(yards_gained, 0L) >= 10L) |
      (dplyr::coalesce(pass_attempt, FALSE) & dplyr::coalesce(yards_gained, 0L) >= 15L),
    
    # --- simple, reliable scoring deltas (pos vs def) ---
    score_diff_delta = dplyr::if_else(
      !is.na(score_differential_post) & !is.na(score_differential),
      score_differential_post - score_differential, NA_real_
    ),
    posteam_points = pmax(score_diff_delta, 0),
    defteam_points = pmax(-score_diff_delta, 0),
    
    # --- clock landmarks ---
    two_min_warning_half = dplyr::coalesce(quarter_seconds_remaining <= 120, FALSE),
    late_and_close = dplyr::coalesce(qtr == 4L, FALSE) &
      dplyr::coalesce(score_diff_abs <= 7, FALSE) &
      dplyr::coalesce(game_seconds_remaining <= 600, FALSE)
  )
}

#' Turn cumulative tallies into stable rates
#' (expects the cum_* columns created by your add_*_cumulative_* functions)
derive_team_rate_features <- function(df) {
  dplyr::mutate(
    df,
    off_epa_per_play        = cum_epa_offense        / pmax(cum_play_offense, 1),
    off_success_rate        = cum_success_offense    / pmax(cum_play_offense, 1),
    off_pass_rate           = cum_pass_attempts      / pmax(cum_play_offense, 1),
    off_run_rate            = cum_rush_attempts      / pmax(cum_play_offense, 1),
    def_epa_per_play_allowed    = cum_epa_defense     / pmax(cum_play_defense, 1),
    def_success_rate_allowed    = cum_success_defense / pmax(cum_play_defense, 1)
  )
}

#' Summarise team-game features from play-by-play
#'
#' Collapses plays -> drives -> team-game. Includes "quality drive" tagging.
#'
#' Quality drive (default): drive ends with >=3 points OR reaches opponent's 40
#' (yardline_100 <= qd_yardline) AND (at least qd_min_plays plays OR
#' total yards gained >= qd_min_yards).
#'
#' @param df Tibble from `clean_pbp_data()` (your filtered play-level data)
#' @param qd_yardline integer. Threshold for field position quality (opp 40 = 40).
#' @param qd_min_plays integer. Minimum plays for a non-scoring drive to count.
#' @param qd_min_yards integer. Minimum yards gained for a non-scoring drive to count.
#' @return Tibble with one row per (game_id, posteam) and rich game features.
summarise_team_game_features <- function(
    df,
    qd_yardline  = 40L,
    qd_min_plays = 4L,
    qd_min_yards = 20L
) {
  
  stopifnot(all(c("game_id","posteam","drive","play_id","yardline_100",
                  "yards_gained","epa","wpa","success","pass_attempt",
                  "rush_attempt","sack","touchdown","interception",
                  "score_differential","score_differential_post",
                  "drive_time_of_possession","drive_ended_with_score") %in% names(df)))
  
  # helper: parse "mm:ss" to seconds
  .mmss_to_sec <- function(x) {
    ok <- !is.na(x) & grepl("^\\d+:\\d{2}$", x)
    out <- rep(NA_real_, length(x))
    out[ok] <- as.numeric(sub(":.*", "", x[ok])) * 60 +
      as.numeric(sub(".*:", "", x[ok]))
    out
  }
  
  df2 <- df %>%
    dplyr::filter(!is.na(game_id), !is.na(posteam), !is.na(drive)) %>%
    dplyr::mutate(
      # points scored by offense on this play via score differential delta
      score_diff_delta = dplyr::if_else(
        !is.na(score_differential_post) & !is.na(score_differential),
        score_differential_post - score_differential,
        NA_real_
      ),
      posteam_points = pmax(dplyr::coalesce(score_diff_delta, 0), 0),
      # simple explosive definition (self-contained; no external deps)
      explosive_play =
        (dplyr::coalesce(rush_attempt, FALSE) & dplyr::coalesce(yards_gained, 0L) >= 10L) |
        (dplyr::coalesce(pass_attempt, FALSE) & dplyr::coalesce(yards_gained, 0L) >= 15L),
      dtop_seconds = .mmss_to_sec(drive_time_of_possession)
    )
  
  # ---- Drive-level rollup (with 3-and-out & short-turnover flags) ----
  drives <- df2 %>%
    dplyr::group_by(game_id, posteam, drive) %>%
    dplyr::arrange(play_id, .by_group = TRUE) %>%
    dplyr::summarise(
      plays           = dplyr::n(),
      yards_total     = sum(dplyr::coalesce(yards_gained, 0L)),
      epa_sum         = sum(dplyr::coalesce(epa, 0)),
      wpa_sum         = sum(dplyr::coalesce(wpa, 0)),
      success_plays   = sum(dplyr::coalesce(success, FALSE)),
      success_rate    = mean(dplyr::coalesce(success, FALSE)),
      passes          = sum(dplyr::coalesce(pass_attempt, FALSE)),
      rushes          = sum(dplyr::coalesce(rush_attempt, FALSE)),
      sacks           = sum(dplyr::coalesce(sack, FALSE)),
      interceptions   = sum(dplyr::coalesce(interception, FALSE)),
      explosive_plays = sum(explosive_play, na.rm = TRUE),
      explosive_rate  = mean(explosive_play, na.rm = TRUE),
      points          = sum(dplyr::coalesce(posteam_points, 0)),
      touchdowns      = any(dplyr::coalesce(touchdown, FALSE)),
      start_yardline_100 = dplyr::first(yardline_100),
      min_yardline_100   = dplyr::if_else(any(!is.na(yardline_100)),
                                          min(yardline_100, na.rm = TRUE),
                                          as.numeric(NA)),
      depth_into_opponent = dplyr::if_else(
        !is.na(start_yardline_100) & !is.na(min_yardline_100),
        start_yardline_100 - min_yardline_100, NA_real_
      ),
      red_zone_trip   = dplyr::coalesce(min_yardline_100 <= 20, FALSE),
      drive_top_seconds = dplyr::first(dtop_seconds[!is.na(dtop_seconds)]),
      
      # --- drive end markers (use only existing fields) ---
      last_play_type      = dplyr::last(play_type),
      last_series_result  = dplyr::last(series_result),
      punt_end            = dplyr::coalesce(last_play_type == "punt", FALSE),
      turnover_on_drive   = dplyr::coalesce(last_series_result %in% c("Interception","Fumble","Downs"), FALSE) |
        any(dplyr::coalesce(interception, FALSE)),
      
      # --- outcomes/quality ---
      drive_scored    = any(dplyr::coalesce(drive_ended_with_score, FALSE)) | points > 0,
      
      # True 3-and-out: exactly 3 plays, ending with a punt
      three_and_out   = (plays == 3L) & punt_end,
      
      # Short-drive turnover: ≤3 plays and ended by INT/Fumble/Downs
      short_turnover  = (plays <= 3L) & turnover_on_drive,
      
      # quality-drive tag (as you had, kept intact)
      reached_quality_fp = dplyr::coalesce(min_yardline_100 <= qd_yardline, FALSE),
      quality_drive = (points >= 3) |
        (reached_quality_fp & (plays >= qd_min_plays | yards_total >= qd_min_yards)),
      .groups = "drop_last"
    ) %>%
    dplyr::ungroup()
  
  # ---- Team-game rollup ----
  games <- drives %>%
    dplyr::group_by(game_id, posteam) %>%
    dplyr::summarise(
      drives                      = dplyr::n(),
      quality_drives              = sum(quality_drive, na.rm = TRUE),
      quality_drive_rate          = quality_drives / pmax(drives, 1),
      points_per_drive            = sum(points, na.rm = TRUE) / pmax(drives, 1),
      epa_total                   = sum(epa_sum, na.rm = TRUE),
      plays_total                 = sum(plays, na.rm = TRUE),
      epa_per_play                = epa_total / pmax(plays_total, 1),
      success_rate                = sum(success_plays, na.rm = TRUE) / pmax(plays_total, 1),
      explosive_rate              = sum(explosive_plays, na.rm = TRUE) / pmax(plays_total, 1),
      pass_rate                   = sum(passes, na.rm = TRUE) / pmax(plays_total, 1),
      rush_rate                   = sum(rushes, na.rm = TRUE) / pmax(plays_total, 1),
      sacks_per_drive             = sum(sacks, na.rm = TRUE) / pmax(drives, 1),
      interceptions_per_drive     = sum(interceptions, na.rm = TRUE) / pmax(drives, 1),
      avg_start_yardline_100      = mean(start_yardline_100, na.rm = TRUE),
      avg_drive_depth_into_opp    = mean(depth_into_opponent, na.rm = TRUE),
      avg_drive_plays             = mean(plays, na.rm = TRUE),
      avg_drive_time_seconds      = mean(drive_top_seconds, na.rm = TRUE),
      red_zone_trips              = sum(red_zone_trip, na.rm = TRUE),
      red_zone_trip_rate          = red_zone_trips / pmax(drives, 1),
      red_zone_scores             = sum(red_zone_trip & points > 0, na.rm = TRUE),
      red_zone_score_rate         = red_zone_scores / pmax(red_zone_trips, 1),
      td_drives                   = sum(touchdowns, na.rm = TRUE),
      td_rate_per_drive           = td_drives / pmax(drives, 1),
      
      # --- new: 3-and-outs & short-drive turnovers ---
      three_and_outs                      = sum(three_and_out, na.rm = TRUE),
      three_and_out_rate                  = three_and_outs / pmax(drives, 1),
      short_turnovers_leq3                = sum(short_turnover, na.rm = TRUE),
      short_turnover_leq3_rate            = short_turnovers_leq3 / pmax(drives, 1),
      three_and_out_or_short_turnover     = sum(three_and_out | short_turnover, na.rm = TRUE),
      three_and_out_or_short_turnover_rate= three_and_out_or_short_turnover / pmax(drives, 1),
      
      wpa_total                   = sum(wpa_sum, na.rm = TRUE),
      three_and_outs_raw          = three_and_outs,   # optional aliases if you like
      short_turnovers_raw         = short_turnovers_leq3,
      .groups = "drop"
    )
  
  # recompute early-down metrics directly from plays for accuracy
  early <- df2 %>%
    dplyr::filter(down %in% c(1L, 2L)) %>%
    dplyr::group_by(game_id, posteam) %>%
    dplyr::summarise(
      early_plays      = dplyr::n(),
      early_epa_total  = sum(dplyr::coalesce(epa, 0)),
      early_successes  = sum(dplyr::coalesce(success, FALSE)),
      early_epa_per_play = early_epa_total / pmax(early_plays, 1),
      early_success_rate = early_successes / pmax(early_plays, 1),
      pass_oe_sum      = sum(dplyr::coalesce(pass_oe, 0)),
      .groups = "drop"
    )
  
  games %>%
    dplyr::left_join(early, by = c("game_id","posteam")) %>%
    dplyr::mutate(
      early_plays          = dplyr::coalesce(early_plays, 0L),
      early_epa_total      = dplyr::coalesce(early_epa_total, 0),
      early_epa_per_play   = dplyr::coalesce(early_epa_per_play, 0),
      early_success_rate   = dplyr::coalesce(early_success_rate, 0),
      pass_oe_mean         = dplyr::coalesce(pass_oe_sum, 0) / pmax(plays_total, 1)
    )
}



