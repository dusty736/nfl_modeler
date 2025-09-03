# etl/R/download_live_nfl_data.R

library(nflreadr)
library(tidyverse)
library(arrow)
library(progressr)

#seasons <- nflreadr::get_current_season()
#weeks <- nflreadr::get_current_week()

seasons <- 2024
weeks <- 1

################################################################################
# Create root folder
################################################################################
dir.create("data/live", recursive = TRUE, showWarnings = FALSE)

################################################################################
# Load Functions
################################################################################
source(here("etl", "R", "step2_process", "step2_pbp_process_functions.R"))
source(here("etl", "R", "step3_sql", "step3_long_player_format_functions.R"))
source(here("etl", "R", "step3_sql", "step3_long_team_format_functions.R"))
source(here("etl", "R", "utils.R"))
source(here("etl", "R", "step4_update_db", "step4_update_prod_functions.R"))
source(here("etl", "R", "step2_process", "step2_schedule_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_team_strength_process_functions.R"))

################################################################################
# Play-by-play data (nflfastR)
################################################################################
pbp <- with_progress(nflreadr::load_pbp(seasons)) %>% 
  filter(week %in% weeks)

pbp_clean <- pbp |>
  clean_pbp_data() |>
  add_team_cumulative_stats() |>
  add_defense_cumulative_stats()

pbp_feats <- pbp_clean |>
  clean_pbp_data() |>
  add_team_cumulative_stats() |>
  add_defense_cumulative_stats() |>
  add_situational_features() |>
  derive_team_rate_features() %>% distinct()

game_team_feats <- pbp_clean %>%
  clean_pbp_data() %>%
  summarise_team_game_features(
    qd_yardline  = 40,  # opp 40
    qd_min_plays = 4,
    qd_min_yards = 20
  ) %>% distinct()

write_parquet(pbp_feats, "data/live/pbp.parquet")
write_parquet(game_team_feats, "data/live/pbp_games.parquet")

################################################################################
# Schedule / Game metadata (nflfastR)
################################################################################
schedule <- with_progress(nflreadr::load_schedules(seasons)) %>% distinct()
schedule_clean <- clean_schedule_data(schedule)
weekly_results <- get_weekly_season_table(schedule)
season_results <- summarize_season_team_results(schedule)

################################################################################
# Long Player Table
################################################################################
id_map <- nflreadr::load_depth_charts(seasons) %>% 
  filter((season %in% seasons & week %in% weeks))

# Offense
weekly_wr <- pivot_player_stats_long(file_path = here("data", "processed",
                                                      "weekly_stats_wr.parquet")) %>% 
  filter(player_id %in% id_map$gsis_id)
weekly_te <- pivot_player_stats_long(file_path = here("data", "processed", 
                                                      "weekly_stats_te.parquet")) %>% 
  filter(player_id %in% id_map$gsis_id)
weekly_qb <- pivot_player_stats_long(file_path = here("data", "processed", 
                                                      "weekly_stats_qb.parquet")) %>% 
  filter(player_id %in% id_map$gsis_id)
weekly_rb <- pivot_player_stats_long(file_path = here("data", "processed", 
                                                      "weekly_stats_rb.parquet")) %>% 
  filter(player_id %in% id_map$gsis_id)
weekly_ng_qb <- pivot_ngs_player_stats_long(file_path = here("data", "processed", 
                                                             "nextgen_stats_player_weekly.parquet"), 
                                            opponent_df = weekly_qb) %>% 
  filter(player_id %in% id_map$gsis_id)
weekly_pbp_qb <- pivot_pbp_game_stats_long(input_path = here("data", "processed", 
                                                             "pbp_cleaned_games.parquet"))

weekly_pbp_qb <- weekly_qb %>% 
  dplyr::select(player_id, name, position, season, season_type, week, team, opponent) %>% 
  distinct() %>% 
  left_join(., weekly_pbp_qb, by=c('season', 'season_type', 'week', 'team', 'opponent'))

# Defense
opponent_df <- weekly_rb %>% 
  dplyr::select(team, opponent, season, week, season_type) %>% 
  distinct()
weekly_def <- pivot_def_player_stats_long(file_path = here("data", "processed", 
                                                           "def_player_stats_weekly.parquet"), 
                                          opponent_df = opponent_df) %>% 
  filter(player_id %in% id_map$gsis_id)

historic_player_long <- DBI::dbGetQuery(conn = con, 
                                        paste0("select * from prod.player_weekly_tbl where season in (", seasons, ") and week in (", paste(weeks, collapse = ", "), ");")) %>% 
  filter(player_id %in% id_map$gsis_id)

# Combine
weekly_players <- rbind(historic_player_long,
                        weekly_qb, 
                        weekly_ng_qb,
                        weekly_rb,
                        weekly_wr,
                        weekly_te,
                        weekly_def) %>% 
  distinct() %>% 
  arrange(player_id, season, week)

seasonal_players <- create_season_stats(weekly_players) %>% 
  mutate_if(is.numeric, round, 0) %>% 
  distinct()

career_players <- create_career_stats(weekly_players) %>% 
  mutate_if(is.numeric, round, 3) %>% 
  distinct()

arrow::write_parquet(weekly_players %>% 
                       mutate_if(is.numeric, round, 3) %>% 
                       distinct() %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/live/player_weekly_tbl.parquet")
arrow::write_parquet(seasonal_players %>% 
                       distinct() %>% filter(season %in% seasons), "data/live/player_season_tbl.parquet")
arrow::write_parquet(career_players %>% 
                       distinct(), "data/live/player_career_tbl.parquet")

################################################################################
# Long Player Table
################################################################################

team_schedule <- weekly_results %>% 
  dplyr::select(team = team_id, opponent, season, week, season_type) %>% 
  distinct()

game_id_map <- weekly_results %>% 
  dplyr::select(team = team_id, game_id, season, week) %>% 
  distinct()

weekly_off <- pivot_team_stats_long(here("data", "processed", "off_team_stats_week.parquet"),
                                    team_schedule, "recent_team") %>% filter(stat_name != 'games_played')
weekly_def <- pivot_team_stats_long(here("data", "processed", "def_team_stats_week.parquet"),
                                    team_schedule, "team")
weekly_inj <- pivot_team_stats_long(here("data", "processed", "injuries_team_weekly.parquet"),
                                    team_schedule, "team")
weekly_pbp <- pivot_pbp_game_stats_long(input_path = here("data", "processed", 
                                                          "pbp_cleaned_games.parquet"))
weekly_game_stats <- pivot_game_results_long(here("data", "processed", "weekly_results.parquet")) %>% 
  left_join(., team_schedule, by=c('team', 'season', 'week'))
weekly_st_stats <- pivot_special_teams_long(here("data", "processed", "st_player_stats_weekly.parquet")) %>% 
  left_join(., team_schedule, by=c('team', 'season', 'week', 'season_type'))

historic_team_long <- DBI::dbGetQuery(conn = con, 
                                        paste0("select * from prod.team_weekly_tbl where season in (", seasons, ") and week in (", paste(weeks, collapse = ", "), ");"))

weekly_total <- rbind(weekly_off %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_def %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_st_stats %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_inj %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      weekly_game_stats,
                      weekly_pbp %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
                      historic_team_long) %>% 
  distinct() %>% arrange(team, season, week)

season_total <- aggregate_team_season_stats(weekly_total) %>% 
  distinct()

alltime_total <- aggregate_team_alltime_stats(weekly_total) %>% 
  distinct()

arrow::write_parquet(weekly_total %>% 
                       mutate_if(is.numeric, round, 3) %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/live/team_weekly_tbl.parquet")
arrow::write_parquet(season_total %>% 
                       mutate_if(is.numeric, round, 3) %>% filter(season %in% seasons), "data/live/team_season_tbl.parquet")
arrow::write_parquet(alltime_total %>% 
                       mutate_if(is.numeric, round, 3), "data/live/team_career_tbl.parquet")

message("All live data saved to /data/live")
