#' Clean Next Gen Stats for Modeling and Storage
#'
#' This function processes raw Next Gen Stats data from `nflreadr::load_nextgen_stats()`
#' to remove redundant fields and standardize player identifiers. It filters out `week == 0`
#' entries and retains only relevant features for modeling or dashboarding.
#'
#' @param nextgen_stats_raw A data frame containing raw Next Gen Stats from nflreadr.
#'
#' @return A cleaned tibble with key passing metrics and standardized player/team identifiers.
#'
#' @examples
#' \dontrun{
#' raw_ngs <- nflreadr::load_nextgen_stats()
#' clean_ngs <- process_nextgen_stats(raw_ngs)
#' }
#'
#' @import dplyr
#' @export
process_nextgen_stats <- function(nextgen_stats_raw) {
  nextgen_stats_raw %>%
    dplyr::filter(week > 0) %>%
    dplyr::select(
      season, season_type, week,
      player_gsis_id, full_name = player_display_name,
      player_position, team_abbr,
      avg_time_to_throw,
      avg_completed_air_yards,
      avg_intended_air_yards,
      avg_air_yards_differential,
      aggressiveness,
      max_completed_air_distance,
      avg_air_yards_to_sticks,
      attempts, pass_yards, pass_touchdowns, interceptions,
      passer_rating,
      completions,
      completion_percentage,
      expected_completion_percentage,
      completion_percentage_above_expectation,
      avg_air_distance,
      max_air_distance
    )
}

#' Calculate NFL Passer Rating
#'
#' Computes the traditional NFL passer rating from season or game totals.
#'
#' @param completions Number of completed passes.
#' @param attempts Number of pass attempts.
#' @param yards Total passing yards.
#' @param touchdowns Number of passing touchdowns.
#' @param interceptions Number of interceptions thrown.
#'
#' @return A numeric passer rating, capped between 0 and 158.3.
#'
#' @examples
#' calculate_passer_rating(completions = 350, attempts = 500, yards = 4200,
#'                         touchdowns = 30, interceptions = 10)
#'
#' @export
calculate_passer_rating <- function(completions, attempts, yards, touchdowns, interceptions) {
  a <- ((completions / attempts) - 0.3) * 5
  b <- ((yards / attempts) - 3) * 0.25
  c <- (touchdowns / attempts) * 20
  d <- 2.375 - ((interceptions / attempts) * 25)
  
  # Cap each component between 0 and 2.375
  a <- pmax(0, pmin(a, 2.375))
  b <- pmax(0, pmin(b, 2.375))
  c <- pmax(0, pmin(c, 2.375))
  d <- pmax(0, pmin(d, 2.375))
  
  rating <- ((a + b + c + d) / 6) * 100
  return(round(rating, 1))
}

#' Aggregate Next Gen Stats to Player-Season Level
#'
#' Aggregates cleaned Next Gen Stats to the player-season level,
#' using appropriate logic (e.g., sum for totals, mean for rates),
#' and calculates traditional NFL passer rating and per-game averages.
#'
#' @param nextgen_stats A cleaned nextgen data frame (from `process_nextgen_stats()`),
#'   with player-level weekly stats and identifiers.
#'
#' @return A tibble with one row per player-season including summed volume stats,
#'   averaged rate stats, calculated passer rating, and per-game averages.
#'
#' @examples
#' \dontrun{
#' clean_ngs <- process_nextgen_stats(nextgen_stats_raw)
#' season_ngs <- aggregate_nextgen_by_season(clean_ngs)
#' }
#'
#' @import dplyr
#' @export
aggregate_nextgen_by_season <- function(nextgen_stats) {
  nextgen_stats %>%
    dplyr::group_by(season, player_gsis_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      team_abbr = dplyr::first(team_abbr),
      player_position = dplyr::first(player_position),
      games_played = dplyr::n_distinct(week),
      
      # Sum counting stats
      attempts = sum(attempts, na.rm = TRUE),
      completions = sum(completions, na.rm = TRUE),
      pass_yards = sum(pass_yards, na.rm = TRUE),
      pass_touchdowns = sum(pass_touchdowns, na.rm = TRUE),
      interceptions = sum(interceptions, na.rm = TRUE),
      
      # Per-game averages
      avg_attempts = round(attempts / games_played, 1),
      avg_completions = round(completions / games_played, 1),
      avg_pass_yards = round(pass_yards / games_played, 1),
      avg_pass_touchdowns = round(pass_touchdowns / games_played, 2),
      avg_interceptions = round(interceptions / games_played, 2),
      
      # Average rate stats
      avg_time_to_throw = mean(avg_time_to_throw, na.rm = TRUE),
      avg_completed_air_yards = mean(avg_completed_air_yards, na.rm = TRUE),
      avg_intended_air_yards = mean(avg_intended_air_yards, na.rm = TRUE),
      avg_air_yards_differential = mean(avg_air_yards_differential, na.rm = TRUE),
      aggressiveness = mean(aggressiveness, na.rm = TRUE),
      max_completed_air_distance = max(max_completed_air_distance, na.rm = TRUE),
      avg_air_yards_to_sticks = mean(avg_air_yards_to_sticks, na.rm = TRUE),
      completion_percentage = mean(completion_percentage, na.rm = TRUE),
      expected_completion_percentage = mean(expected_completion_percentage, na.rm = TRUE),
      completion_percentage_above_expectation = mean(completion_percentage_above_expectation, na.rm = TRUE),
      avg_air_distance = mean(avg_air_distance, na.rm = TRUE),
      max_air_distance = max(max_air_distance, na.rm = TRUE),
      
      # Derived stat
      passer_rating = calculate_passer_rating(
        completions = sum(completions, na.rm = TRUE),
        attempts = sum(attempts, na.rm = TRUE),
        yards = sum(pass_yards, na.rm = TRUE),
        touchdowns = sum(pass_touchdowns, na.rm = TRUE),
        interceptions = sum(interceptions, na.rm = TRUE)
      ),
      .groups = "drop"
    )
}

#' Aggregate Next Gen Stats to Player Career Level
#'
#' Aggregates cleaned Next Gen Stats across all seasons per player.
#' Calculates career totals, per-game averages, and passer rating using
#' summed volume stats.
#'
#' @param nextgen_stats A cleaned nextgen data frame (from `process_nextgen_stats()`),
#'   with player-level weekly stats and identifiers.
#'
#' @return A tibble with one row per player containing career totals, rate stats,
#'   per-game averages, and calculated passer rating.
#'
#' @examples
#' \dontrun{
#' clean_ngs <- process_nextgen_stats(nextgen_stats_raw)
#' career_ngs <- aggregate_nextgen_by_career(clean_ngs)
#' }
#'
#' @import dplyr
#' @export
aggregate_nextgen_by_career <- function(nextgen_stats) {
  nextgen_stats %>%
    dplyr::group_by(player_gsis_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      team_abbr = dplyr::last(team_abbr),  # last known team
      player_position = dplyr::first(player_position),
      games_played = dplyr::n_distinct(season, week),
      
      # Career totals
      attempts = sum(attempts, na.rm = TRUE),
      completions = sum(completions, na.rm = TRUE),
      pass_yards = sum(pass_yards, na.rm = TRUE),
      pass_touchdowns = sum(pass_touchdowns, na.rm = TRUE),
      interceptions = sum(interceptions, na.rm = TRUE),
      
      # Career per-game averages
      avg_attempts = round(attempts / games_played, 1),
      avg_completions = round(completions / games_played, 1),
      avg_pass_yards = round(pass_yards / games_played, 1),
      avg_pass_touchdowns = round(pass_touchdowns / games_played, 2),
      avg_interceptions = round(interceptions / games_played, 2),
      
      # Averaged rate stats
      avg_time_to_throw = mean(avg_time_to_throw, na.rm = TRUE),
      avg_completed_air_yards = mean(avg_completed_air_yards, na.rm = TRUE),
      avg_intended_air_yards = mean(avg_intended_air_yards, na.rm = TRUE),
      avg_air_yards_differential = mean(avg_air_yards_differential, na.rm = TRUE),
      aggressiveness = mean(aggressiveness, na.rm = TRUE),
      max_completed_air_distance = max(max_completed_air_distance, na.rm = TRUE),
      avg_air_yards_to_sticks = mean(avg_air_yards_to_sticks, na.rm = TRUE),
      completion_percentage = mean(completion_percentage, na.rm = TRUE),
      expected_completion_percentage = mean(expected_completion_percentage, na.rm = TRUE),
      completion_percentage_above_expectation = mean(completion_percentage_above_expectation, na.rm = TRUE),
      avg_air_distance = mean(avg_air_distance, na.rm = TRUE),
      max_air_distance = max(max_air_distance, na.rm = TRUE),
      
      # Derived stat
      passer_rating = calculate_passer_rating(
        completions = completions,
        attempts = attempts,
        yards = pass_yards,
        touchdowns = pass_touchdowns,
        interceptions = interceptions
      ),
      .groups = "drop"
    )
}

#' Aggregate Postseason Next Gen Stats to Player-Season Level
#'
#' Filters to postseason (`season_type == "POST"`) and aggregates Next Gen Stats
#' to the player-season level. Includes career totals, per-game averages,
#' and calculated passer rating.
#'
#' @param nextgen_stats A cleaned nextgen data frame (from `process_nextgen_stats()`),
#'   with player-level weekly stats and a `season_type` column.
#'
#' @return A tibble with one row per player-season of postseason data,
#'   including totals, per-game averages, rate stats, and passer rating.
#'
#' @examples
#' \dontrun{
#' clean_ngs <- process_nextgen_stats(nextgen_stats_raw)
#' post_ngs <- aggregate_nextgen_postseason(clean_ngs)
#' }
#'
#' @import dplyr
#' @export
aggregate_nextgen_postseason <- function(nextgen_stats) {
  nextgen_stats %>%
    dplyr::filter(season_type == "POST") %>%
    dplyr::group_by(player_gsis_id) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      team_abbr = dplyr::last(team_abbr),
      player_position = dplyr::first(player_position),
      games_played = dplyr::n(),
      
      # Totals
      attempts = sum(attempts, na.rm = TRUE),
      completions = sum(completions, na.rm = TRUE),
      pass_yards = sum(pass_yards, na.rm = TRUE),
      pass_touchdowns = sum(pass_touchdowns, na.rm = TRUE),
      interceptions = sum(interceptions, na.rm = TRUE),
      
      # Per-game averages
      avg_attempts = round(attempts / games_played, 1),
      avg_completions = round(completions / games_played, 1),
      avg_pass_yards = round(pass_yards / games_played, 1),
      avg_pass_touchdowns = round(pass_touchdowns / games_played, 2),
      avg_interceptions = round(interceptions / games_played, 2),
      
      # Rate stats
      avg_time_to_throw = mean(avg_time_to_throw, na.rm = TRUE),
      avg_completed_air_yards = mean(avg_completed_air_yards, na.rm = TRUE),
      avg_intended_air_yards = mean(avg_intended_air_yards, na.rm = TRUE),
      avg_air_yards_differential = mean(avg_air_yards_differential, na.rm = TRUE),
      aggressiveness = mean(aggressiveness, na.rm = TRUE),
      max_completed_air_distance = max(max_completed_air_distance, na.rm = TRUE),
      avg_air_yards_to_sticks = mean(avg_air_yards_to_sticks, na.rm = TRUE),
      completion_percentage = mean(completion_percentage, na.rm = TRUE),
      expected_completion_percentage = mean(expected_completion_percentage, na.rm = TRUE),
      completion_percentage_above_expectation = mean(completion_percentage_above_expectation, na.rm = TRUE),
      avg_air_distance = mean(avg_air_distance, na.rm = TRUE),
      max_air_distance = max(max_air_distance, na.rm = TRUE),
      
      # Derived stat
      passer_rating = calculate_passer_rating(
        completions = completions,
        attempts = attempts,
        yards = pass_yards,
        touchdowns = pass_touchdowns,
        interceptions = interceptions
      ),
      .groups = "drop"
    )
}

#' Compute Cumulative Sums and Means for Next Gen Stats
#'
#' Computes cumulative sum and cumulative mean for selected passing stats
#' at the player-week level, grouped by season and player.
#'
#' @param nextgen_stats A cleaned weekly-level data frame from `process_nextgen_stats()`.
#'
#' @return A tibble with new cumulative variables added. Cumulative sums are prefixed
#'   with `cumulative_`, and cumulative means with `cumulative_`.
#'
#' @examples
#' \dontrun{
#' clean_ngs <- process_nextgen_stats(nextgen_stats_raw)
#' running_ngs <- compute_cumulative_nextgen_stats(clean_ngs)
#' }
#'
#' @import dplyr
#' @export
compute_cumulative_nextgen_stats <- function(nextgen_stats) {
  nextgen_stats %>%
    dplyr::group_by(season, player_gsis_id) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(
      # Cumulative sums (volume stats)
      cumulative_attempts = cumsum(attempts),
      cumulative_completions = cumsum(completions),
      cumulative_pass_yards = cumsum(pass_yards),
      cumulative_pass_touchdowns = cumsum(pass_touchdowns),
      cumulative_interceptions = cumsum(interceptions),
      
      # Cumulative means (rate stats)
      cumulative_avg_time_to_throw = cummean(avg_time_to_throw),
      cumulative_avg_completed_air_yards = cummean(avg_completed_air_yards),
      cumulative_avg_intended_air_yards = cummean(avg_intended_air_yards),
      cumulative_avg_air_yards_differential = cummean(avg_air_yards_differential),
      cumulative_aggressiveness = cummean(aggressiveness),
      cumulative_avg_air_yards_to_sticks = cummean(avg_air_yards_to_sticks),
      cumulative_completion_percentage = cummean(completion_percentage),
      cumulative_expected_completion_percentage = cummean(expected_completion_percentage),
      cumulative_completion_percentage_above_expectation = cummean(completion_percentage_above_expectation),
      cumulative_avg_air_distance = cummean(avg_air_distance)
    ) %>%
    dplyr::ungroup() %>% 
    arrange(player_gsis_id, season, week)
}

