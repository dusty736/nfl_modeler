# etl/R/download_raw_nfl_data.R

library(nflreadr)
library(tidyverse)
library(arrow)
library(progressr)

seasons <- 1999:lubridate::year(Sys.Date())

################################################################################
# Create root folder
################################################################################
dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)

################################################################################
# Play-by-play data (nflfastR)
################################################################################
pbp <- with_progress(nflreadr::load_pbp(seasons=TRUE))
write_parquet(pbp, "data/raw/pbp.parquet")

################################################################################
# Rosters (nflreadr)
################################################################################
rosters <- with_progress(nflreadr::load_rosters(seasons))
write_parquet(rosters, "data/raw/rosters.parquet")

################################################################################
# Depth charts (nflreadr)
################################################################################
depth_charts <- with_progress(nflreadr::load_depth_charts(seasons=TRUE)) %>% 
  filter(game_type != 'SBBYE')
write_parquet(depth_charts, "data/raw/depth_charts.parquet")

################################################################################
# Injuries (nflreadr)
################################################################################
injuries <- with_progress(nflreadr::load_injuries(seasons=TRUE))
write_parquet(injuries, "data/raw/injuries.parquet")

################################################################################
# Participation (nflreadr)
################################################################################
participation <- with_progress(nflreadr::load_participation(seasons=TRUE))
write_parquet(participation, "data/raw/participation.parquet")

################################################################################
# Next Gen Stats (nflreadr)
################################################################################
ngs <- with_progress(nflreadr::load_nextgen_stats(seasons=TRUE))
write_parquet(ngs, "data/raw/nextgen_stats.parquet")

################################################################################
# Contracts (nflreadr)
################################################################################
contracts <- with_progress(nflreadr::load_contracts())
write_parquet(contracts, "data/raw/contracts.parquet")

################################################################################
# Team Metadata
################################################################################
team_metadata <- with_progress(nflreadr::load_teams(current=FALSE)) %>% 
  filter(team_abbr != 'LAR')
write_parquet(team_metadata, "data/raw/team_metadata.parquet")

################################################################################
# Snapcount
################################################################################
snapcount <- with_progress(nflreadr::load_snap_counts(seasons=TRUE))
write_parquet(snapcount, "data/raw/player_snapcount.parquet")

################################################################################
# ESPN QBR
################################################################################
espn_qbr <- with_progress(nflreadr::load_espn_qbr(seasons=TRUE))
write_parquet(espn_qbr, "data/raw/espn_qbr.parquet")

################################################################################
# ID Map
################################################################################
id_map <- with_progress(nflreadr::load_rosters(seasons)) %>% 
  dplyr::select(full_name, first_name, last_name, contains("id")) %>% 
  distinct()
write_parquet(id_map, "data/raw/id_map.parquet")

################################################################################
# Player Stats: offense + kicking (nflreadr)
################################################################################
offense_stats <- with_progress(nflreadr::load_player_stats(seasons=TRUE, stat_type = "offense"))
defense_stats <- with_progress(nflreadr::load_player_stats(seasons=TRUE, stat_type = "defense"))
kicking_stats <- with_progress(nflreadr::load_player_stats(seasons=TRUE, stat_type = "kicking"))
write_parquet(offense_stats, "data/raw/off_player_stats.parquet")
write_parquet(defense_stats, "data/raw/def_player_stats.parquet")
write_parquet(kicking_stats, "data/raw/st_player_stats.parquet")

################################################################################
# Schedule / Game metadata (nflfastR)
################################################################################
schedule <- with_progress(nflreadr::load_schedules(seasons=TRUE))
write_parquet(schedule, "data/raw/schedule.parquet")

message("All raw data saved to /data/raw")
