################################################################################
# step3_long_player_format.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step3_sql", "step3_long_player_format_functions.R"))

idmap <- arrow::read_parquet(here("data", "processed", "id_map.parquet"))

################################################################################
# PIVOT! - Weekly
################################################################################

# Offense
weekly_wr <- pivot_player_stats_long(file_path = here("data", "processed", 
                                                      "weekly_stats_wr.parquet"))
weekly_te <- pivot_player_stats_long(file_path = here("data", "processed", 
                                                      "weekly_stats_te.parquet"))
weekly_qb <- pivot_player_stats_long(file_path = here("data", "processed", 
                                                      "weekly_stats_qb.parquet"))
weekly_rb <- pivot_player_stats_long(file_path = here("data", "processed", 
                                                      "weekly_stats_rb.parquet"))
weekly_ng_qb <- pivot_ngs_player_stats_long(file_path = here("data", "processed", 
                                                      "nextgen_stats_player_weekly.parquet"), 
                                            opponent_df = weekly_qb)

# Defense
opponent_df <- weekly_rb %>% 
  dplyr::select(team, opponent, season, week, season_type) %>% 
  distinct()
weekly_def <- pivot_def_player_stats_long(file_path = here("data", "processed", 
                                                           "def_player_stats_weekly.parquet"), 
                                          opponent_df = opponent_df)

# Combine
weekly_players <- rbind(weekly_qb, 
                        weekly_ng_qb,
                        weekly_rb,
                        weekly_wr,
                        weekly_te,
                        weekly_def)

################################################################################
# PIVOT! - Seasonal
################################################################################

seasonal_players <- create_season_stats(weekly_players) %>% 
  mutate_if(is.numeric, round, 0)

################################################################################
# PIVOT! - Career (Starting in 1999)
################################################################################

career_players <- create_career_stats(weekly_players) %>% 
  mutate_if(is.numeric, round, 3)

################################################################################
# Save
################################################################################
arrow::write_parquet(weekly_players %>% 
                       mutate_if(is.numeric, round, 3), "data/for_database/player_weekly_tbl.parquet")
arrow::write_parquet(seasonal_players, "data/for_database/player_season_tbl.parquet")
arrow::write_parquet(career_players, "data/for_database/player_career_tbl.parquet")


