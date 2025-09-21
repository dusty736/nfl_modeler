################################################################################
# step3_long_player_format.R
################################################################################

library(arrow)
library(here)
library(dplyr)
library(tidyverse)

source(here("etl", "R", "step3_sql", "step3_long_team_format_functions.R"))
source(here("etl", "R", "step3_sql", "step3_long_player_format_functions.R"))

team_metadata <- arrow::read_parquet(here("data", "processed", "team_metadata.parquet"))
team_schedule <- arrow::read_parquet(here("data", "processed", "weekly_results.parquet")) %>% 
  dplyr::select(team = team_id, opponent, season, week, season_type) %>% 
  mutate(
    team = toupper(trimws(team)),
    team = dplyr::recode(
      team,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = team
    ),
    opponent = dplyr::recode(
      opponent,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = opponent
    )
  ) %>% 
  distinct()
game_id_map <- arrow::read_parquet(here("data", "processed", "weekly_results.parquet")) %>% 
  dplyr::select(team = team_id, game_id, season, week) %>% 
  mutate(
    team = toupper(trimws(team)),
    team = dplyr::recode(
      team,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = team
    )
  ) %>% 
  distinct()

################################################################################
# PIVOT! - Weekly
################################################################################

weekly_off <- pivot_team_stats_long(here("data", "processed", "off_team_stats_week.parquet"),
                                     team_schedule, "recent_team") %>% filter(stat_name != 'games_played') %>% 
  mutate(
   team = toupper(trimws(team)),
   team = dplyr::recode(
     team,
     "SD"  = "LAC",
     "STL" = "LA",
     "LAR" = "LA",
     "OAK" = "LV",
     "JAC" = "JAX",
     "NOR" = "NO",
     "WSH" = "WAS",
     "GNB" = "GB",
     .default = team
   )
 )
weekly_def <- pivot_team_stats_long(here("data", "processed", "def_team_stats_week.parquet"),
                                     team_schedule, "team") %>% 
  mutate(
    team = toupper(trimws(team)),
    team = dplyr::recode(
      team,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = team
    )
  )
weekly_inj <- pivot_team_stats_long(here("data", "processed", "injuries_team_weekly.parquet"),
                                    team_schedule, "team") %>% 
  mutate(
    team = toupper(trimws(team)),
    team = dplyr::recode(
      team,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = team
    )
  )
weekly_pbp <- pivot_pbp_game_stats_long(input_path = here("data", "processed", 
                                                          "pbp_cleaned_games.parquet")) %>% 
  mutate(
    team = toupper(trimws(team)),
    team = dplyr::recode(
      team,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = team
    )
  )
weekly_game_stats <- pivot_game_results_long(here("data", "processed", "weekly_results.parquet")) %>% 
  left_join(., team_schedule, by=c('team', 'season', 'week')) %>% 
  mutate(
    team = toupper(trimws(team)),
    team = dplyr::recode(
      team,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = team
    )
  )
weekly_st_stats <- pivot_special_teams_long(here("data", "processed", "st_player_stats_weekly.parquet")) %>% 
  left_join(., team_schedule, by=c('team', 'season', 'week', 'season_type')) %>% 
  mutate(
    team = toupper(trimws(team)),
    team = dplyr::recode(
      team,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = team
    )
  )

weekly_team_strength <- arrow::read_parquet("data/for_database/team_strength_tbl.parquet") %>% 
  pivot_longer(., -c(season, week, team), names_to = 'stat_name', values_to = 'value') %>% 
  mutate(stat_type = 'base') %>% 
  mutate(
    team = toupper(trimws(team)),
    team = dplyr::recode(
      team,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = team
    )
  ) %>% 
  left_join(., team_schedule, by=c('team', 'season', 'week'))

weekly_total <- rbind(weekly_off %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_def %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_st_stats %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_inj %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_game_stats,
                      weekly_pbp %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_team_strength %>% left_join(., game_id_map, by=c('team', 'season', 'week'))) %>% 
  distinct()

weekly_total <- derive_defense_allowed_stats(weekly_total)

################################################################################
# PIVOT! - Seasonal
################################################################################

season_total <- aggregate_team_season_stats(weekly_total) %>% 
  distinct()

################################################################################
# PIVOT! - All Time (Since 1999)
################################################################################

alltime_total <- aggregate_team_alltime_stats(weekly_total) %>% 
  distinct()

################################################################################
# Save
################################################################################

arrow::write_parquet(weekly_total %>% 
                       mutate_if(is.numeric, round, 3), "data/for_database/team_weekly_tbl.parquet")
arrow::write_parquet(season_total %>% 
                       mutate_if(is.numeric, round, 3), "data/for_database/team_season_tbl.parquet")
arrow::write_parquet(alltime_total %>% 
                       mutate_if(is.numeric, round, 3), "data/for_database/team_career_tbl.parquet")

