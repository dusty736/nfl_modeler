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
    position_clean = clean_position(position),
    position_group = dplyr::case_when(
      position_clean %in% c("OL", "WR", "TE", "QB", "RB") ~ "OFF",
      position_clean %in% c("DL", "LB", "CB", "S") ~ "DEF",
      position_clean %in% c("K", "LS") ~ "ST",
      TRUE ~ "OTHER"
    )
  )

# 2. QB stats by team and season
qb_stats_by_team_season <- get_qb_start_stats(starters)

# 3. Total starts per player/position/team/season
player_start_totals_all <- get_player_start_totals(starters)

# 4. Detect starter switches (e.g., new starters week-to-week)
starter_switches_all <- get_starter_switches(starters)

# 5. Promotions from backup to starter
promotions_to_starter <- get_inseason_promotions(starters)

# 6. Lineup stability score per team/season/position
lineup_stability_scores <- get_lineup_stability_by_week(starters) %>% 
  distinct()

blah <- starters %>% 
  group_by(season, team, position_clean, player) %>% 
  mutate(n_starts = row_number()) %>%  # Counts number of starts for each player
  ungroup() %>% 
  group_by(season, week, team, position_clean) %>% 
  mutate(n_denom = n()) %>%  # Counts the number of players for each position in each week
  ungroup() %>% 
  filter(season == 2016 & team == 'ATL' & week %in% 1:3) %>% 
  group_by(season, week, team, position_clean) %>% 
  mutate(position_group_denom = sum(n_denom),  # Sum of n_denom for the entire group
         position_group_num = sum(n_starts)) %>%  # Sum of n_starts for the entire group
  dplyr::select(season, week, team, position_clean, position_group_num, 
                position_group_denom) %>% 
  distinct() %>%  # Keep distinct combinations
  group_by(season, team, position_clean) %>%  # Group by season, team, position_clean to calculate rolling sum
  mutate(rolling_sum = cumsum(position_group_denom) / n())  # Calculate rolling sum across weeks


################################################################################
# Save processed output
################################################################################
arrow::write_parquet(position_cap_pct, "data/processed/depth_charts_position_cap_pct.parquet")
arrow::write_parquet(qb_depth_charts, "data/processed/qb_depth_charts.parquet")
