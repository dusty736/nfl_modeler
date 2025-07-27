#' Process offensive participation data on a per-play level
#'
#' @param df Raw participation data frame (e.g., participation_raw)
#'
#' @return A tibble with one row per play including offensive metadata, NGS, pressures, and play type
#' @export
process_participation_offense_by_play <- function(df) {
  df %>%
    filter(possession_team != '') %>% 
    filter(!is.na(offense_formation) | !is.na(offense_personnel)) %>% 
    dplyr::select(
      game_id           = nflverse_game_id,
      play_id,
      team              = possession_team,
      offense_formation,
      offense_personnel,
      n_offense,
      ngs_air_yards,
      time_to_throw,
      was_pressure,
      route
    ) %>%
    dplyr::filter(!is.na(play_id)) %>%
    dplyr::mutate(
      game_id = as.character(game_id),
      play_id = as.integer(play_id),
      season = as.integer(substr(game_id, 1, 4)),
      week = as.integer(substr(game_id, 6, 7)),
      team = dplyr::na_if(team, ""),
      offense_formation = dplyr::case_when(
        is.na(offense_formation) | offense_formation == "" ~ "OTHER",
        TRUE ~ offense_formation
      ),
      offense_personnel = dplyr::case_when(
        is.na(offense_personnel) | offense_personnel == "" ~ "OTHER",
        TRUE ~ offense_personnel
      ),
      route = dplyr::na_if(route, ""),
      ngs_air_yards = as.numeric(ngs_air_yards),
      time_to_throw = as.numeric(time_to_throw),
      was_pressure = as.logical(was_pressure),
      n_offense = as.integer(n_offense),
      play_type = dplyr::case_when(
        !is.na(time_to_throw) ~ "pass",
        is.na(time_to_throw) ~ "run",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::arrange(game_id, team, play_id) %>%
    dplyr::group_by(game_id, team) %>%
    dplyr::mutate(
      pressures_allowed = cumsum(was_pressure %in% TRUE),
      
      # Play type cumulative counts
      cumulative_pass = cumsum(play_type == "pass"),
      cumulative_run = cumsum(play_type == "run"),
      
      # Route cumulative counts
      cumulative_angle   = cumsum(route == "ANGLE" & !is.na(route)),
      cumulative_corner  = cumsum(route == "CORNER" & !is.na(route)),
      cumulative_cross   = cumsum(route == "CROSS" & !is.na(route)),
      cumulative_flat    = cumsum(route == "FLAT" & !is.na(route)),
      cumulative_go      = cumsum(route == "GO" & !is.na(route)),
      cumulative_hitch   = cumsum(route == "HITCH" & !is.na(route)),
      cumulative_in      = cumsum(route == "IN" & !is.na(route)),
      cumulative_out     = cumsum(route == "OUT" & !is.na(route)),
      cumulative_post    = cumsum(route == "POST" & !is.na(route)),
      cumulative_screen  = cumsum(route == "SCREEN" & !is.na(route)),
      cumulative_slant   = cumsum(route == "SLANT" & !is.na(route)),
      cumulative_wheel   = cumsum(route == "WHEEL" & !is.na(route)),
      
      # Formation cumulative counts
      cumulative_empty       = cumsum(offense_formation == "EMPTY"),
      cumulative_i_form      = cumsum(offense_formation == "I_FORM"),
      cumulative_jumbo       = cumsum(offense_formation == "JUMBO"),
      cumulative_other       = cumsum(offense_formation == "OTHER"),
      cumulative_pistol      = cumsum(offense_formation == "PISTOL"),
      cumulative_shotgun     = cumsum(offense_formation == "SHOTGUN"),
      cumulative_singleback  = cumsum(offense_formation == "SINGLEBACK"),
      cumulative_wildcat     = cumsum(offense_formation == "WILDCAT")
    ) %>% 
    dplyr::ungroup() %>% 
    dplyr::select(game_id, season, week, play_id, team, play_type, everything())
}

#' Summarize offensive participation data and compute running totals
#'
#' @param df A tibble returned from process_participation_offense_by_play()
#'
#' @return A tibble with one row per (game_id, team) summarizing offensive stats with running totals
#' @export
summarize_offense_by_team_game <- function(df) {
  df %>%
    dplyr::filter(!is.na(team)) %>%
    dplyr::group_by(game_id, team, season, week) %>%
    dplyr::summarize(
      n_plays = dplyr::n(),
      n_pass = sum(play_type == "pass", na.rm = TRUE),
      n_run = sum(play_type == "run", na.rm = TRUE),
      
      # Formation counts
      n_empty      = sum(offense_formation == "EMPTY", na.rm = TRUE),
      n_i_form     = sum(offense_formation == "I_FORM", na.rm = TRUE),
      n_jumbo      = sum(offense_formation == "JUMBO", na.rm = TRUE),
      n_pistol     = sum(offense_formation == "PISTOL", na.rm = TRUE),
      n_shotgun    = sum(offense_formation == "SHOTGUN", na.rm = TRUE),
      n_singleback = sum(offense_formation == "SINGLEBACK", na.rm = TRUE),
      n_wildcat    = sum(offense_formation == "WILDCAT", na.rm = TRUE),
      n_other_formations = sum(!offense_formation %in% c(
        "EMPTY", "I_FORM", "JUMBO", "PISTOL", "SHOTGUN", "SINGLEBACK", "WILDCAT"
      ), na.rm = TRUE),
      
      # Route counts
      n_angle   = sum(route == "ANGLE", na.rm = TRUE),
      n_corner  = sum(route == "CORNER", na.rm = TRUE),
      n_cross   = sum(route == "CROSS", na.rm = TRUE),
      n_flat    = sum(route == "FLAT", na.rm = TRUE),
      n_go      = sum(route == "GO", na.rm = TRUE),
      n_hitch   = sum(route == "HITCH", na.rm = TRUE),
      n_in      = sum(route == "IN", na.rm = TRUE),
      n_out     = sum(route == "OUT", na.rm = TRUE),
      n_post    = sum(route == "POST", na.rm = TRUE),
      n_screen  = sum(route == "SCREEN", na.rm = TRUE),
      n_slant   = sum(route == "SLANT", na.rm = TRUE),
      n_wheel   = sum(route == "WHEEL", na.rm = TRUE),
      n_other_routes = sum(!is.na(route) & !route %in% c(
        "ANGLE", "CORNER", "CROSS", "FLAT", "GO", "HITCH", "IN", "OUT", "POST",
        "SCREEN", "SLANT", "WHEEL"
      ), na.rm = TRUE),
      
      # Pressure and NGS stats
      avg_time_to_throw = mean(time_to_throw, na.rm = TRUE),
      pressures_allowed = max(pressures_allowed, na.rm = TRUE),
      
      .groups = "drop"
    ) %>%
    dplyr::arrange(season, team, week) %>%
    dplyr::group_by(season, team) %>%
    dplyr::mutate(
      cumulative_plays = cumsum(n_plays),
      cumulative_pass = cumsum(n_pass),
      cumulative_run = cumsum(n_run),
      cumulative_pressures_allowed = cumsum(pressures_allowed),
      
      cumulative_screen = cumsum(n_screen),
      cumulative_flat = cumsum(n_flat),
      cumulative_other_routes = cumsum(n_other_routes),
      
      avg_time_to_throw_to_date = cummean(avg_time_to_throw)
    ) %>%
    dplyr::ungroup()
}

#' Summarize offensive plays by formation at the game-team level
#'
#' @param df A tibble returned from process_participation_offense_by_play()
#'
#' @return A tibble with one row per (game_id, team, offense_formation)
#' @export
summarize_offense_by_team_game_formation <- function(df) {
  df %>%
    dplyr::filter(!is.na(team)) %>%
    dplyr::group_by(game_id, team, offense_formation, season, week) %>%
    dplyr::summarize(
      n_plays = dplyr::n(),
      n_pass = sum(play_type == "pass", na.rm = TRUE),
      n_run = sum(play_type == "run", na.rm = TRUE),
      
      # Route counts
      n_angle   = sum(route == "ANGLE", na.rm = TRUE),
      n_corner  = sum(route == "CORNER", na.rm = TRUE),
      n_cross   = sum(route == "CROSS", na.rm = TRUE),
      n_flat    = sum(route == "FLAT", na.rm = TRUE),
      n_go      = sum(route == "GO", na.rm = TRUE),
      n_hitch   = sum(route == "HITCH", na.rm = TRUE),
      n_in      = sum(route == "IN", na.rm = TRUE),
      n_out     = sum(route == "OUT", na.rm = TRUE),
      n_post    = sum(route == "POST", na.rm = TRUE),
      n_screen  = sum(route == "SCREEN", na.rm = TRUE),
      n_slant   = sum(route == "SLANT", na.rm = TRUE),
      n_wheel   = sum(route == "WHEEL", na.rm = TRUE),
      n_other_routes = sum(!is.na(route) & !route %in% c(
        "ANGLE", "CORNER", "CROSS", "FLAT", "GO", "HITCH", "IN", "OUT", "POST",
        "SCREEN", "SLANT", "WHEEL"
      ), na.rm = TRUE),
      
      avg_time_to_throw = mean(time_to_throw, na.rm = TRUE),
      pressures_allowed = sum(pressures_allowed, na.rm = TRUE),
      
      .groups = "drop"
    ) %>%
    dplyr::arrange(season, team, week)
}

#' Summarize game-level offensive stats by team and season
#'
#' @param df A game-level summary like participation_game_offense
#'
#' @return A tibble with one row per (season, team) with total counts and seasonal averages
#' @export
summarize_offense_by_team_season <- function(df) {
  df %>%
    dplyr::group_by(season, team) %>%
    dplyr::summarize(
      n_plays      = sum(n_plays, na.rm = TRUE),
      n_pass       = sum(n_pass, na.rm = TRUE),
      n_run        = sum(n_run, na.rm = TRUE),
      
      n_empty      = sum(n_empty, na.rm = TRUE),
      n_i_form     = sum(n_i_form, na.rm = TRUE),
      n_jumbo      = sum(n_jumbo, na.rm = TRUE),
      n_pistol     = sum(n_pistol, na.rm = TRUE),
      n_shotgun    = sum(n_shotgun, na.rm = TRUE),
      n_singleback = sum(n_singleback, na.rm = TRUE),
      n_wildcat    = sum(n_wildcat, na.rm = TRUE),
      n_other_formations = sum(n_other_formations, na.rm = TRUE),
      
      n_angle      = sum(n_angle, na.rm = TRUE),
      n_corner     = sum(n_corner, na.rm = TRUE),
      n_cross      = sum(n_cross, na.rm = TRUE),
      n_flat       = sum(n_flat, na.rm = TRUE),
      n_go         = sum(n_go, na.rm = TRUE),
      n_hitch      = sum(n_hitch, na.rm = TRUE),
      n_in         = sum(n_in, na.rm = TRUE),
      n_out        = sum(n_out, na.rm = TRUE),
      n_post       = sum(n_post, na.rm = TRUE),
      n_screen     = sum(n_screen, na.rm = TRUE),
      n_slant      = sum(n_slant, na.rm = TRUE),
      n_wheel      = sum(n_wheel, na.rm = TRUE),
      n_other_routes = sum(n_other_routes, na.rm = TRUE),
      
      avg_time_to_throw = mean(avg_time_to_throw, na.rm = TRUE),
      pressures_allowed = sum(pressures_allowed, na.rm = TRUE),
      
      .groups = "drop"
    )
}

#' Process defensive participation data on a per-play level
#'
#' @param df Raw participation data frame (e.g., participation_raw)
#'
#' @return A tibble with one row per play containing defensive metadata
#' @export
process_participation_defense_by_play <- function(df) {
  df %>%
    filter(!is.na(defense_personnel)) %>% 
    filter(!is.na(possession_team)) %>% 
    filter(possession_team != '') %>% 
    dplyr::select(
      game_id                = nflverse_game_id,
      play_id,
      possession_team,
      defense_personnel,
      defenders_in_box,
      number_of_pass_rushers,
      defense_man_zone_type,
      defense_coverage_type,
      time_to_throw,
      was_pressure
    ) %>%
    dplyr::filter(!is.na(play_id)) %>%
    dplyr::mutate(
      game_id = as.character(game_id),
      season = as.integer(substr(game_id, 1, 4)),
      week = as.integer(substr(game_id, 6, 7)),
      play_id = as.integer(play_id),
      play_type = dplyr::case_when(
        !is.na(time_to_throw) ~ "pass",
        is.na(time_to_throw) ~ "run",
        TRUE ~ NA_character_
      ),
      
      # Extract team1 and team2 from game_id
      team1 = stringr::str_split_fixed(game_id, "_", 4)[, 3],
      team2 = stringr::str_split_fixed(game_id, "_", 4)[, 4],
      
      # Assign defense team as the team not in possession
      defense_team = dplyr::case_when(
        possession_team == team1 ~ team2,
        possession_team == team2 ~ team1,
        TRUE ~ NA_character_
      ),
      
      defense_team = dplyr::na_if(defense_team, ""),
      defense_personnel = dplyr::case_when(
        is.na(defense_personnel) | defense_personnel == "" ~ "OTHER",
        TRUE ~ defense_personnel
      ),
      defenders_in_box = as.integer(defenders_in_box),
      number_of_pass_rushers = as.integer(number_of_pass_rushers),
      time_to_throw = as.numeric(time_to_throw),
      was_pressure = as.logical(was_pressure),
      # Pass rusher bins
      rush_bin = dplyr::case_when(
        number_of_pass_rushers <= 3 ~ "low",
        number_of_pass_rushers == 4 ~ "standard",
        number_of_pass_rushers == 5 ~ "blitz",
        number_of_pass_rushers >= 6 ~ "heavy_blitz",
        TRUE ~ NA_character_
      ),
      
      # Box defender bins
      box_bin = dplyr::case_when(
        defenders_in_box <= 6 ~ "light",
        defenders_in_box == 7 ~ "standard",
        defenders_in_box >= 8 ~ "stacked",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::arrange(game_id, defense_team, play_id) %>%
    dplyr::group_by(game_id, defense_team) %>%
    dplyr::mutate(
      cumulative_pass = cumsum(play_type == "pass"),
      cumulative_run = cumsum(play_type == "run"),
      
      cumulative_low_rush       = cumsum(rush_bin == "low" & !is.na(rush_bin)),
      cumulative_standard_rush  = cumsum(rush_bin == "standard" & !is.na(rush_bin)),
      cumulative_blitz          = cumsum(rush_bin == "blitz" & !is.na(rush_bin)),
      cumulative_heavy_blitz    = cumsum(rush_bin == "heavy_blitz" & !is.na(rush_bin)),
      
      cumulative_light_box      = cumsum(box_bin == "light" & !is.na(box_bin)),
      cumulative_standard_box   = cumsum(box_bin == "standard" & !is.na(box_bin)),
      cumulative_stacked_box    = cumsum(box_bin == "stacked" & !is.na(box_bin)),
      
      cumulative_man   = cumsum(defense_man_zone_type == "MAN_COVERAGE" & !is.na(defense_man_zone_type)),
      cumulative_zone  = cumsum(defense_man_zone_type == "ZONE_COVERAGE" & !is.na(defense_man_zone_type)),
      
      cumulative_cover_0 = cumsum(defense_coverage_type == "COVER_0" & !is.na(defense_coverage_type)),
      cumulative_cover_1 = cumsum(defense_coverage_type == "COVER_1" & !is.na(defense_coverage_type)),
      cumulative_cover_2 = cumsum(defense_coverage_type == "COVER_2" & !is.na(defense_coverage_type)),
      cumulative_cover_3 = cumsum(defense_coverage_type == "COVER_3" & !is.na(defense_coverage_type)),
      cumulative_cover_4 = cumsum(defense_coverage_type == "COVER_4" & !is.na(defense_coverage_type)),
      cumulative_cover_6 = cumsum(defense_coverage_type == "COVER_6" & !is.na(defense_coverage_type)),
      cumulative_cover_2_man = cumsum(defense_coverage_type == "2_MAN" & !is.na(defense_coverage_type)),
      cumulative_prevent = cumsum(defense_coverage_type == "PREVENT" & !is.na(defense_coverage_type))
    ) %>% 
    dplyr::select(
      game_id, season, week, play_id, defense_team, play_type,
      everything()
    ) %>%
    dplyr::arrange(game_id, defense_team, play_id) %>% 
    ungroup(.)
}

#' Summarize defensive play-by-play at the game-team level
#'
#' @param df A tibble like participation_pbp_defense
#'
#' @return A tibble with one row per (game_id, defense_team)
#' @export
summarize_defense_by_team_game <- function(df) {
  df %>%
    dplyr::group_by(game_id, defense_team, season, week) %>%
    dplyr::summarize(
      n_plays = dplyr::n(),
      n_pass = sum(play_type == "pass", na.rm = TRUE),
      n_run  = sum(play_type == "run", na.rm = TRUE),
      
      # Blitz bins
      n_low_rush       = sum(rush_bin == "low", na.rm = TRUE),
      n_standard_rush  = sum(rush_bin == "standard", na.rm = TRUE),
      n_blitz          = sum(rush_bin == "blitz", na.rm = TRUE),
      n_heavy_blitz    = sum(rush_bin == "heavy_blitz", na.rm = TRUE),
      
      # Box bins
      n_light_box      = sum(box_bin == "light", na.rm = TRUE),
      n_standard_box   = sum(box_bin == "standard", na.rm = TRUE),
      n_stacked_box    = sum(box_bin == "stacked", na.rm = TRUE),
      
      # Coverage types
      n_man   = sum(defense_man_zone_type == "MAN_COVERAGE", na.rm = TRUE),
      n_zone  = sum(defense_man_zone_type == "ZONE_COVERAGE", na.rm = TRUE),
      
      n_cover_0 = sum(defense_coverage_type == "COVER_0", na.rm = TRUE),
      n_cover_1 = sum(defense_coverage_type == "COVER_1", na.rm = TRUE),
      n_cover_2 = sum(defense_coverage_type == "COVER_2", na.rm = TRUE),
      n_cover_3 = sum(defense_coverage_type == "COVER_3", na.rm = TRUE),
      n_cover_4 = sum(defense_coverage_type == "COVER_4", na.rm = TRUE),
      n_cover_6 = sum(defense_coverage_type == "COVER_6", na.rm = TRUE),
      n_cover_2_man = sum(defense_coverage_type == "2_MAN", na.rm = TRUE),
      n_prevent = sum(defense_coverage_type == "PREVENT", na.rm = TRUE),
      
      # Pressure and NGS
      n_pressures = sum(was_pressure %in% TRUE, na.rm = TRUE),
      avg_time_to_throw = mean(time_to_throw, na.rm = TRUE),
      
      .groups = "drop"
    ) %>% 
    dplyr::arrange(season, defense_team, week) %>%
    dplyr::group_by(season, defense_team) %>%
    dplyr::mutate(
      cumulative_plays     = cumsum(n_plays),
      cumulative_pass      = cumsum(n_pass),
      cumulative_run       = cumsum(n_run),
      cumulative_low_rush  = cumsum(n_low_rush),
      cumulative_standard_rush = cumsum(n_standard_rush),
      cumulative_blitz     = cumsum(n_blitz),
      cumulative_heavy_blitz = cumsum(n_heavy_blitz),
      
      cumulative_light_box    = cumsum(n_light_box),
      cumulative_standard_box = cumsum(n_standard_box),
      cumulative_stacked_box  = cumsum(n_stacked_box),
      
      cumulative_man   = cumsum(n_man),
      cumulative_zone  = cumsum(n_zone),
      
      cumulative_cover_0 = cumsum(n_cover_0),
      cumulative_cover_1 = cumsum(n_cover_1),
      cumulative_cover_2 = cumsum(n_cover_2),
      cumulative_cover_3 = cumsum(n_cover_3),
      cumulative_cover_4 = cumsum(n_cover_4),
      cumulative_cover_6 = cumsum(n_cover_6),
      cumulative_cover_2_man = cumsum(n_cover_2_man),
      cumulative_prevent     = cumsum(n_prevent),
      
      cumulative_pressures = cumsum(n_pressures),
      avg_time_to_throw_to_date = cummean(avg_time_to_throw)
    ) %>%
    dplyr::ungroup()
}

#' Summarize defensive play-by-play at the season-team level
#'
#' @param df A tibble like participation_pbp_defense
#'
#' @return A tibble with one row per (season, defense_team)
#' @export
summarize_defense_by_team_season <- function(df) {
  df %>%
    dplyr::group_by(season, defense_team) %>%
    dplyr::summarize(
      n_plays = dplyr::n(),
      n_pass = sum(play_type == "pass", na.rm = TRUE),
      n_run  = sum(play_type == "run", na.rm = TRUE),
      
      n_low_rush       = sum(rush_bin == "low", na.rm = TRUE),
      n_standard_rush  = sum(rush_bin == "standard", na.rm = TRUE),
      n_blitz          = sum(rush_bin == "blitz", na.rm = TRUE),
      n_heavy_blitz    = sum(rush_bin == "heavy_blitz", na.rm = TRUE),
      
      n_light_box      = sum(box_bin == "light", na.rm = TRUE),
      n_standard_box   = sum(box_bin == "standard", na.rm = TRUE),
      n_stacked_box    = sum(box_bin == "stacked", na.rm = TRUE),
      
      n_man            = sum(defense_man_zone_type == "MAN_COVERAGE", na.rm = TRUE),
      n_zone           = sum(defense_man_zone_type == "ZONE_COVERAGE", na.rm = TRUE),
      
      n_cover_0        = sum(defense_coverage_type == "COVER_0", na.rm = TRUE),
      n_cover_1        = sum(defense_coverage_type == "COVER_1", na.rm = TRUE),
      n_cover_2        = sum(defense_coverage_type == "COVER_2", na.rm = TRUE),
      n_cover_3        = sum(defense_coverage_type == "COVER_3", na.rm = TRUE),
      n_cover_4        = sum(defense_coverage_type == "COVER_4", na.rm = TRUE),
      n_cover_6        = sum(defense_coverage_type == "COVER_6", na.rm = TRUE),
      n_cover_2_man    = sum(defense_coverage_type == "2_MAN", na.rm = TRUE),
      n_prevent        = sum(defense_coverage_type == "PREVENT", na.rm = TRUE),
      
      n_pressures      = sum(was_pressure %in% TRUE, na.rm = TRUE),
      avg_time_to_throw = mean(time_to_throw, na.rm = TRUE),
      
      .groups = "drop"
    )
}



