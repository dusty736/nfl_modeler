################################################################################
# step2_team_medadata_process.R
################################################################################

library(arrow)
library(here)
library(tidyverse)
library(scales)

source(here("etl", "R", "misc", "scratch_functions.R"))

team_metadata <- arrow::read_parquet(here("data", "for_database","team_metadata_tbl.parquet")) %>% 
  dplyr::select(team = team_abbr, team_color, team_color2)

################################################################################
# Load raw data
################################################################################

team_weekly <- arrow::read_parquet(here("data", "for_database", 
                                        "team_weekly_tbl.parquet")) %>% 
  left_join(., team_metadata, by='team') %>% 
  distinct()

player_weekly <- arrow::read_parquet(here("data", "for_database", 
                                              "player_weekly_tbl.parquet")) %>% 
  left_join(., team_metadata, by='team') %>% 
  distinct()

################################################################################
# 1. Team Weekly Time Series
################################################################################

plot_player_weekly_trajectory(
  data = player_weekly,
  season_choice = 2024,
  season_type_choice = "REG",
  stat_choice = "fantasy_points_ppr",
  position_choice = "WR",
  top_n = 10,
  week_range = 1:18,
  base_cumulative = "base",
  cumulative_mode = TRUE
)

################################################################################
# 2. Player Violins
################################################################################
plot_player_consistency_violin(
  data = player_weekly,
  season_choice = 2024,
  season_type_choice = "REG",
  stat_choice = "fantasy_points_ppr",
  position_choice = "RB",
  top_n = 10,
  week_range = 1:18,
  base_cumulative = "base",
  order_by = "median",      # rank by robustness (steady first)
  show_points = TRUE     # overlay weekly points in team_color2
)

plot_player_consistency_violin(
  data = player_weekly,
  season_choice = 2024,
  season_type_choice = "ALL",
  stat_choice = "passing_epa",
  position_choice = "QB",
  top_n = 10,
  week_range = 1:22,
  base_cumulative = "base",
  order_by = "median",
  show_points = TRUE
)

################################################################################
# 3. Player Scatter Plots
################################################################################
# passing_epa_per_dropback
# passing_anya
# passing_ypa
# rushing_epa_per_carry
# rushing_ypc
# receiving_epa_per_target
# receiving_ypt
# receiving_ypr
# total_epa_per_opportunity
# yards_per_opportunity
# td_rate_per_opportunity
# receiving_epa_per_opportunity
plot_workload_efficiency_summary(
  data = player_weekly,
  season_choice = 2024,
  season_type_choice = "REG",
  week_range = 1:18,
  position_choice = "RB",
  metric_choice = "rushing_epa_per_carry",
  top_n = 20,
  workload_floor = 20,
  label_all_points = TRUE,  # label every point
  log_x = FALSE
)

plot_player_scatter_quadrants(
  data = player_weekly,
  season_choice = 2024,
  season_type_choice = "REG",
  week_range = 1:18,
  position_choice = "RB",
  top_n = 10,
  metric_x = "carries",
  metric_y = "rushing_epa_per_carry",
  label_all_points = TRUE,
  log_x = FALSE,
  log_y = FALSE,
  top_by = "x_gate"
)

################################################################################
# 4. Rolling Percentiles
################################################################################
plot_player_rolling_percentiles(
  data = player_weekly,
  season_choice = 2024,
  season_type_choice = "ALL",
  week_range = 1:22,
  position_choice = "QB",
  metric_choice = "passing_epa",
  top_n = 10,
  rolling_window = 4,
  show_points = TRUE,
  label_last = TRUE,
  ncol = 4
)

################################################################################
# Team 1. Time Series
################################################################################

plot_team_time_series(
  data = team_weekly,
  season_choice = 2013:2015,
  season_type_choice = "REG",
  stat_choice = "passing_epa",
  top_n = 32,
  week_range = 1:18,
  cumulative_mode = TRUE,
  highlight = c("SEA"),
  facet_by_season = TRUE
)

################################################################################
# Team 2. Violin Plots
################################################################################
plot_team_consistency_violin(
  data = team_weekly,
  season_choice = 2024,
  season_type_choice = "REG",
  stat_choice = "points_scored",
  top_n = 10,
  week_range = 1:18,
  order_by = "median",
  show_points = TRUE
)

################################################################################
# Team 3. Team Quadrants
################################################################################
plot_team_scatter_quadrants(
  data = team_weekly,
  season_choice = 2019:2024,
  season_type_choice = "REG",
  week_range = 1:18,
  top_n = 24,
  metric_x = "passing_epa",
  metric_y = "rushing_epa",
  top_by  = "x_value",
  highlight = c("DET","KC","BUF"),
  label_mode = "highlighted"
)

################################################################################
# Team 4. Team Percentile Rolling
################################################################################
plot_team_rolling_percentiles(
  data = team_weekly,
  season_choice = 2024,
  season_type_choice = "REG",
  week_range = 1:18,
  metric_choice = "rushing_epa",
  top_n = 16,
  rolling_window = 1,
  show_points = TRUE,
  label_last = TRUE,
  ncol = 4
)



blah <- team_weekly %>% 
  filter(team == 'SEA') %>% 
  filter(stat_name == 'wins_entering')








