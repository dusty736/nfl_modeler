# etl/R/download_raw_nfl_data.R

library(nflreadr)
library(tidyverse)
library(arrow)
library(progressr)

seasons <- 2016:2024  # start with a good historical window

################################################################################
# Create root folder
################################################################################
dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)

################################################################################
# Play-by-play data (nflfastR)
################################################################################
pbp <- with_progress(nflreadr::load_pbp(seasons))
write_parquet(pbp, "data/raw/pbp.parquet")

################################################################################
# Rosters (nflreadr)
################################################################################
rosters <- with_progress(nflreadr::load_rosters(seasons))
write_parquet(rosters, "data/raw/rosters.parquet")

################################################################################
# Depth charts (nflreadr)
################################################################################
depth_charts <- with_progress(nflreadr::load_depth_charts(seasons)) %>% 
  filter(game_type != 'SBBYE')
write_parquet(depth_charts, "data/raw/depth_charts.parquet")

################################################################################
# Injuries (nflreadr)
################################################################################
injuries <- with_progress(nflreadr::load_injuries(seasons))
write_parquet(injuries, "data/raw/injuries.parquet")

################################################################################
# Participation (nflreadr)
################################################################################
participation <- with_progress(nflreadr::load_participation(seasons))
write_parquet(participation, "data/raw/participation.parquet")

################################################################################
# Next Gen Stats (nflreadr)
################################################################################
ngs <- with_progress(nflreadr::load_nextgen_stats(seasons))
write_parquet(ngs, "data/raw/nextgen_stats.parquet")

################################################################################
# Contracts (nflreadr)
################################################################################
contracts <- with_progress(nflreadr::load_contracts())
write_parquet(contracts, "data/raw/contracts.parquet")

################################################################################
# Player Stats: offense + kicking (nflreadr)
################################################################################
offense_stats <- with_progress(nflreadr::load_player_stats(seasons, stat_type = "offense"))
kicking_stats <- with_progress(nflreadr::load_player_stats(seasons, stat_type = "kicking"))
write_parquet(offense_stats, "data/raw/off_player_stats.parquet")
write_parquet(kicking_stats, "data/raw/st_player_stats.parquet")

################################################################################
# Schedule / Game metadata (nflfastR)
################################################################################
schedule <- with_progress(nflreadr::load_schedules(seasons))
write_parquet(schedule, "data/raw/schedule.parquet")

message("All raw data saved to /data/raw")
