################################################################################
# step2_depth_charts_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_depth_charts_process_functions.R"))

################################################################################
# Load raw data
################################################################################
depth_charts_raw <- arrow::read_parquet(here("data", "raw", "depth_charts.parquet"))

################################################################################
# Clean and normalize
################################################################################
# 1. Extract starters and add cleaned position columns
starters <- filter_depth_chart_starters(depth_charts_raw) %>% 
  mutate(
    position = clean_position(position),
    position_group = dplyr::case_when(
      position %in% c("OL", "WR", "TE", "QB", "RB") ~ "OFF",
      position %in% c("DL", "LB", "CB", "S") ~ "DEF",
      position %in% c("K", "LS") ~ "ST",
      TRUE ~ "OTHER"
    )
  ) %>% 
  distinct()

# 2. QB stats by team and season
qb_stats_by_team_season <- get_qb_start_stats(starters)

# 3. Total starts per player/position/team/season
player_start_totals_season <- get_player_start_totals(starters)

# 4. Detect starter switches (e.g., new starters week-to-week)
starter_switches_all <- get_starter_switches(starters)

# 5. Lineup stability score per team/season/position
lineup_stability_scores <- get_lineup_stability_by_week(starters)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(starter_switches_all, "data/processed/depth_charts_starters.parquet")
arrow::write_parquet(qb_stats_by_team_season, "data/processed/depth_charts_qb_team.parquet")
arrow::write_parquet(player_start_totals_season, "data/processed/depth_charts_player_starts.parquet")
arrow::write_parquet(lineup_stability_scores, "data/processed/depth_charts_position_stability.parquet")
