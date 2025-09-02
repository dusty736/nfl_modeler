# etl/R/download_staging_nfl_data.R

library(nflreadr)
library(tidyverse)
library(arrow)
library(progressr)

#seasons <- lubridate::year(Sys.Date())
#weeks <- max(nflreadr::get_current_week() - 2, 1):nflreadr::get_current_week()

seasons <- 2024
weeks <- 1:2

################################################################################
# Create root folder
################################################################################
dir.create("data/staging", recursive = TRUE, showWarnings = FALSE)

################################################################################
# Load Functions
################################################################################
source(here("etl", "R", "step2_process", "step2_pbp_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_rosters_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_depth_charts_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_injuries_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_schedule_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_espn_qbr_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_nextgen_stats_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_participation_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_snapcount_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_off_player_stats_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_st_player_stats_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_def_player_stats_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_contracts_process_functions.R"))
source(here("etl", "R", "step2_process", "step2_schedule_process_functions.R"))
source(here("etl", "R", "step3_sql", "step3_long_player_format_functions.R"))
source(here("etl", "R", "step3_sql", "step3_long_team_format_functions.R"))

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

write_parquet(pbp, "data/staging/pbp.parquet")
write_parquet(game_team_feats, "data/staging/pbp_games.parquet")

################################################################################
# Rosters (nflreadr)
################################################################################
rosters <- with_progress(nflreadr::load_rosters(seasons)) %>% distinct()

roster_clean <- process_rosters(rosters) %>% distinct()
roster_summary <- summarize_rosters_by_team_season(roster_clean) %>% distinct()
roster_position_summary <- summarize_rosters_by_team_position(roster_clean) %>% distinct()

arrow::write_parquet(roster_clean, "data/staging/rosters.parquet")
arrow::write_parquet(roster_summary, "data/staging/roster_summary.parquet")
arrow::write_parquet(roster_position_summary, "data/staging/roster_position_summary.parquet")

################################################################################
# Depth charts (nflreadr)
################################################################################
depth_charts <- with_progress(nflreadr::load_depth_charts(seasons)) %>% 
  filter(game_type != 'SBBYE') %>% distinct()

# 1. Extract starters and add cleaned position columns
starters <- filter_depth_chart_starters(depth_charts) %>% 
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
qb_stats_by_team_season <- get_qb_start_stats(starters) %>% distinct()

# 3. Total starts per player/position/team/season
player_start_totals_season <- get_player_start_totals(starters) %>% distinct()

# 4. Detect starter switches (e.g., new starters week-to-week)
starter_switches_all <- get_starter_switches(starters) %>% distinct()

# 5. Lineup stability score per team/season/position
lineup_stability_scores <- get_lineup_stability_by_week(starters) %>% distinct()

arrow::write_parquet(starter_switches_all %>% 
                       filter(week %in% weeks), "data/staging/depth_charts_starters.parquet")
arrow::write_parquet(qb_stats_by_team_season %>% 
                       filter(week %in% weeks), "data/staging/depth_charts_qb_team.parquet")
arrow::write_parquet(player_start_totals_season, "data/staging/depth_charts_player_starts.parquet")
arrow::write_parquet(lineup_stability_scores %>% 
                       filter(week %in% weeks), "data/staging/depth_charts_position_stability.parquet")

################################################################################
# Injuries (nflreadr)
################################################################################
injuries <- with_progress(nflreadr::load_injuries(seasons)) %>% distinct()

injuries_cleaned <- process_injuries(injuries) %>% distinct()

# Process injuries by position
injuries_position <- position_injury_summary(injuries_cleaned) %>% distinct()

# Process team injuries by week
injuries_week_team <- team_injury_summary(injuries_position) %>% distinct()

# process team injuries by season
injuries_season_team <- season_injury_summary(injuries_week_team) %>% distinct()

arrow::write_parquet(injuries_cleaned %>% 
                       filter(week %in% weeks), "data/staging/injuries_weekly.parquet")
arrow::write_parquet(injuries_position %>% 
                       filter(week %in% weeks), "data/staging/injuries_position_weekly.parquet")
arrow::write_parquet(injuries_week_team %>% 
                       filter(week %in% weeks), "data/staging/injuries_team_weekly.parquet")
arrow::write_parquet(injuries_season_team, "data/staging/injuries_team_season.parquet")

################################################################################
# Participation (nflreadr)
################################################################################
participation <- with_progress(nflreadr::load_participation(seasons)) %>% distinct()

# Offense
participation_pbp_offense <- process_participation_offense_by_play(participation) %>% distinct()
participation_game_offense <- summarize_offense_by_team_game(participation_pbp_offense) %>% distinct()
participation_game_formation_offense <- summarize_offense_by_team_game_formation(participation_pbp_offense) %>% distinct()
participation_season_offense <- summarize_offense_by_team_season(participation_game_offense) %>% distinct()

# Defense
participation_pbp_defense <- process_participation_defense_by_play(participation) %>% distinct()
participation_game_defense <- summarize_defense_by_team_game(participation_pbp_defense) %>% distinct()
participation_season_defense <- summarize_defense_by_team_season(participation_pbp_defense) %>% distinct()

arrow::write_parquet(participation_pbp_offense %>% 
                       filter(week %in% weeks), "data/staging/participation_offense_pbp.parquet")
arrow::write_parquet(participation_game_offense %>% 
                       filter(week %in% weeks), "data/staging/participation_offense_game.parquet")
arrow::write_parquet(participation_game_formation_offense %>% 
                       filter(week %in% weeks), "data/staging/participation_offense_formation_game.parquet")
arrow::write_parquet(participation_season_offense, "data/staging/participation_offense_season.parquet")
arrow::write_parquet(participation_pbp_defense %>% 
                       filter(week %in% weeks), "data/staging/participation_defense_pbp.parquet")
arrow::write_parquet(participation_game_defense %>% 
                       filter(week %in% weeks), "data/staging/participation_defense_game.parquet")
arrow::write_parquet(participation_season_defense, "data/staging/participation_defense_season.parquet")

################################################################################
# Next Gen Stats (nflreadr)
################################################################################
ngs <- with_progress(nflreadr::load_nextgen_stats(seasons)) %>% distinct()

nextgen_stats_cleaned <- process_nextgen_stats(ngs) %>% distinct()

# Get stats by player - season
nextgen_stats_player_season <- aggregate_nextgen_by_season(nextgen_stats_cleaned) %>% distinct()

# Get stats by player
nextgen_stats_player_career <- aggregate_nextgen_by_career(nextgen_stats_cleaned) %>% distinct()

# Get stats by player - season type
nextgen_stats_player_postseason <- aggregate_nextgen_postseason(nextgen_stats_cleaned) %>% distinct()

# Get running player stats by season
nextgen_stats_player_season_cumulative <- compute_cumulative_nextgen_stats(nextgen_stats_cleaned) %>% distinct()

arrow::write_parquet(nextgen_stats_player_season, "data/staging/nextgen_stats_player_season.parquet")
arrow::write_parquet(nextgen_stats_player_career, "data/staging/nextgen_stats_player_career.parquet")
arrow::write_parquet(nextgen_stats_player_postseason, "data/staging/nextgen_stats_player_postseason.parquet")
arrow::write_parquet(nextgen_stats_player_season_cumulative %>% 
                       filter(week %in% weeks), "data/staging/nextgen_stats_player_weekly.parquet")

################################################################################
# Snapcount
################################################################################
snapcount <- with_progress(nflreadr::load_snap_counts(seasons)) %>% distinct()

snapcount_weekly <- snapcount %>% distinct()
snapcount_season <- summarize_snapcounts_season(snapcount) %>% distinct()
snapcount_career <- summarize_snapcounts_career(snapcount) %>% distinct()

arrow::write_parquet(snapcount_weekly %>% 
                       filter(week %in% weeks), "data/staging/snapcount_weekly.parquet")
arrow::write_parquet(snapcount_season, "data/staging/snapcount_season.parquet")
arrow::write_parquet(snapcount_career, "data/staging/snapcount_career.parquet")

################################################################################
# ESPN QBR
################################################################################
espn_qbr <- with_progress(nflreadr::load_espn_qbr(seasons)) %>% distinct()

espn_qbr_season <- espn_qbr %>% distinct()
espn_qbr_career <- espn_qbr_career_by_season_type(espn_qbr) %>% distinct()

arrow::write_parquet(espn_qbr_season, "data/staging/espn_qbr_season.parquet")
arrow::write_parquet(espn_qbr_career, "data/staging/espn_qbr_career.parquet")

################################################################################
# Player Stats: offense
################################################################################
offense_stats <- with_progress(nflreadr::load_player_stats(seasons, stat_type = "offense")) %>% distinct()
offense_stats_full <- with_progress(nflreadr::load_player_stats(seasons=TRUE, stat_type = "offense")) %>% 
  distinct() %>% 
  filter(player_id %in% offense_stats$player_id)
# QB
weekly_qb_stats <- process_qb_stats(offense_stats_full) %>% distinct()
season_qb_stats <- aggregate_qb_season_stats(weekly_qb_stats) %>% distinct()
career_qb_stats <- aggregate_qb_career_stats(weekly_qb_stats) %>% distinct()

# RB
weekly_rb_stats <- process_rb_stats(offense_stats_full) %>% distinct()
season_rb_stats <- aggregate_rb_season_stats(weekly_rb_stats) %>% distinct()
career_rb_stats <- aggregate_rb_career_stats(weekly_rb_stats) %>% distinct()

# WR
weekly_wr_stats <- process_receiver_stats(offense_stats_full, position_group = 'WR') %>% distinct()
season_wr_stats <- aggregate_receiver_season_stats(weekly_wr_stats) %>% distinct()
career_wr_stats <- aggregate_receiver_career_stats(weekly_wr_stats) %>% distinct()

# TE
weekly_te_stats <- process_receiver_stats(offense_stats_full, position_group = 'TE') %>% distinct()
season_te_stats <- aggregate_receiver_season_stats(weekly_te_stats) %>% distinct()
career_te_stats <- aggregate_receiver_career_stats(weekly_te_stats) %>% distinct()

# Team
weekly_team_stats <- aggregate_offense_team_week_stats(offense_stats_full) %>% distinct()
season_team_stats <- aggregate_offense_team_season_stats(offense_stats_full) %>% distinct()

arrow::write_parquet(weekly_qb_stats %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/weekly_stats_qb.parquet")
arrow::write_parquet(season_qb_stats %>% filter(season %in% seasons), "data/staging/season_stats_qb.parquet")
arrow::write_parquet(career_qb_stats, "data/staging/career_stats_qb.parquet")
arrow::write_parquet(weekly_rb_stats %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/weekly_stats_rb.parquet")
arrow::write_parquet(season_rb_stats %>% filter(season %in% seasons), "data/staging/season_stats_rb.parquet")
arrow::write_parquet(career_rb_stats, "data/staging/career_stats_rb.parquet")
arrow::write_parquet(weekly_wr_stats %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/weekly_stats_wr.parquet")
arrow::write_parquet(season_wr_stats %>% filter(season %in% seasons), "data/staging/season_stats_wr.parquet")
arrow::write_parquet(career_wr_stats, "data/staging/career_stats_wr.parquet")
arrow::write_parquet(weekly_te_stats %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/weekly_stats_te.parquet")
arrow::write_parquet(season_te_stats %>% filter(season %in% seasons), "data/staging/season_stats_te.parquet")
arrow::write_parquet(career_te_stats, "data/staging/career_stats_te.parquet")
arrow::write_parquet(weekly_team_stats %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/off_team_stats_week.parquet")
arrow::write_parquet(season_team_stats %>% filter(season %in% seasons), "data/staging/off_team_stats_season.parquet")

################################################################################
# Player Stats: defense
################################################################################

defense_stats <- with_progress(nflreadr::load_player_stats(seasons, stat_type = "defense")) %>% distinct()
defense_stats_full <- with_progress(nflreadr::load_player_stats(seasons=TRUE, stat_type = "defense")) %>% 
  distinct() %>% 
  filter(player_id %in% defense_stats$player_id)

def_player_stats_cleaned <- process_defensive_player_stats(defense_stats_full) %>% distinct()
def_player_stats_season <- summarize_defensive_player_stats_by_season(def_player_stats_cleaned) %>% distinct()
def_team_stats_season <- summarize_defensive_stats_by_team_season(def_player_stats_cleaned) %>% distinct()
def_team_stats_weekly <- summarize_defensive_stats_by_team_weekly(def_player_stats_cleaned) %>% distinct()
def_player_stats_career <- summarize_defensive_stats_by_player(def_player_stats_cleaned) %>% distinct()

arrow::write_parquet(def_player_stats_cleaned %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/def_player_stats_weekly.parquet")
arrow::write_parquet(def_player_stats_season %>% filter(season %in% seasons), "data/staging/def_player_stats_season.parquet")
arrow::write_parquet(def_player_stats_career, "data/staging/def_player_stats_career.parquet")
arrow::write_parquet(def_team_stats_season %>% filter(season %in% seasons), "data/staging/def_team_stats_season.parquet")
arrow::write_parquet(def_team_stats_weekly %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/def_team_stats_week.parquet")

################################################################################
# Player Stats: ST
################################################################################

kicking_stats <- with_progress(nflreadr::load_player_stats(seasons, stat_type = "kicking")) %>% distinct()
kicking_stats_full <- with_progress(nflreadr::load_player_stats(seasons=TRUE, stat_type = "kicking")) %>% 
  distinct() %>% 
  filter(player_id %in% kicking_stats$player_id)

st_stats_cleaned <- process_special_teams_stats(kicking_stats_full) %>% distinct()
st_stats_games <- add_cumulative_special_teams_stats(st_stats_cleaned) %>% distinct()
st_stats_season <- summarize_special_teams_by_season(st_stats_cleaned) %>% distinct()

arrow::write_parquet(st_stats_games %>% 
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/st_player_stats_weekly.parquet")
arrow::write_parquet(st_stats_season %>% filter(season %in% seasons), "data/staging/st_player_stats_season.parquet")

################################################################################
# ID Map
################################################################################
id_map <- with_progress(nflreadr::load_rosters(seasons)) %>% 
  dplyr::select(full_name, first_name, last_name, contains("id")) %>% 
  distinct()
write_parquet(id_map, "data/staging/id_map.parquet")

################################################################################
# Contracts (nflreadr)
################################################################################
contracts <- with_progress(nflreadr::load_contracts()) %>% distinct() %>% 
  filter(gsis_id %in% id_map$gsis_id)

contracts_clean <- clean_contracts_data(contracts) %>% distinct()
position_cap_pct <- summarise_position_cap_pct(contracts_clean) %>% distinct()
qb_contracts <- add_qb_contract_metadata(contracts_clean) %>% distinct()

arrow::write_parquet(position_cap_pct %>% distinct(), "data/staging/contracts_position_cap_pct.parquet")
arrow::write_parquet(qb_contracts %>% distinct(), "data/staging/contracts_qb.parquet")

################################################################################
# Schedule / Game metadata (nflfastR)
################################################################################
schedule <- with_progress(nflreadr::load_schedules(seasons)) %>% distinct()
schedule_clean <- clean_schedule_data(schedule)
weekly_results <- get_weekly_season_table(schedule)
season_results <- summarize_season_team_results(schedule)

arrow::write_parquet(
  schedule_clean %>% distinct(),
  here("data", "staging", "games.parquet")
)
arrow::write_parquet(
  weekly_results %>% distinct(),
  here("data", "staging", "weekly_results.parquet")
)
arrow::write_parquet(
  season_results %>% distinct(),
  here("data", "staging", "season_results.parquet")
)

################################################################################
# Long Player Table
################################################################################
# Offense
weekly_wr <- pivot_player_stats_long(file_path = here("data", "staging", 
                                                      "weekly_stats_wr.parquet"))
weekly_te <- pivot_player_stats_long(file_path = here("data", "staging", 
                                                      "weekly_stats_te.parquet"))
weekly_qb <- pivot_player_stats_long(file_path = here("data", "staging", 
                                                      "weekly_stats_qb.parquet"))
weekly_rb <- pivot_player_stats_long(file_path = here("data", "staging", 
                                                      "weekly_stats_rb.parquet"))
weekly_ng_qb <- pivot_ngs_player_stats_long(file_path = here("data", "staging", 
                                                             "nextgen_stats_player_weekly.parquet"), 
                                            opponent_df = weekly_qb)
weekly_pbp_qb <- pivot_pbp_game_stats_long(input_path = here("data", "staging", 
                                                             "pbp_games.parquet"))

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
                                          opponent_df = opponent_df)

historic_player_long <- arrow::read_parquet("data/for_database/player_weekly_tbl.parquet") %>% 
  filter(player_id %in% id_map$gsis_id) %>% 
  filter(!(season %in% seasons & week %in% weeks))

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
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/player_weekly_tbl.parquet")
arrow::write_parquet(seasonal_players %>% 
                       distinct() %>% filter(season %in% seasons), "data/staging/player_season_tbl.parquet")
arrow::write_parquet(career_players %>% 
                       distinct(), "data/staging/player_career_tbl.parquet")

################################################################################
# Long Player Table
################################################################################

team_schedule <- weekly_results %>% 
  dplyr::select(team = team_id, opponent, season, week, season_type) %>% 
  distinct()

game_id_map <- weekly_results %>% 
  dplyr::select(team = team_id, game_id, season, week) %>% 
  distinct()

weekly_off <- pivot_team_stats_long(here("data", "staging", "off_team_stats_week.parquet"),
                                    team_schedule, "recent_team") %>% filter(stat_name != 'games_played')
weekly_def <- pivot_team_stats_long(here("data", "staging", "def_team_stats_week.parquet"),
                                    team_schedule, "team")
weekly_inj <- pivot_team_stats_long(here("data", "staging", "injuries_team_weekly.parquet"),
                                    team_schedule, "team")
weekly_pbp <- pivot_pbp_game_stats_long(input_path = here("data", "staging", 
                                                          "pbp_games.parquet"))
weekly_game_stats <- pivot_game_results_long(here("data", "staging", "weekly_results.parquet")) %>% 
  left_join(., team_schedule, by=c('team', 'season', 'week'))
weekly_st_stats <- pivot_special_teams_long(here("data", "staging", "st_player_stats_weekly.parquet")) %>% 
  left_join(., team_schedule, by=c('team', 'season', 'week', 'season_type'))

historic_team_long <- arrow::read_parquet("data/for_database/team_weekly_tbl.parquet") %>% 
  filter(!(season %in% seasons & week %in% weeks))

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
                       filter(week %in% weeks) %>% filter(season %in% seasons), "data/staging/team_weekly_tbl.parquet")
arrow::write_parquet(season_total %>% 
                       mutate_if(is.numeric, round, 3) %>% filter(season %in% seasons), "data/staging/team_season_tbl.parquet")
arrow::write_parquet(alltime_total %>% 
                       mutate_if(is.numeric, round, 3), "data/staging/team_career_tbl.parquet")

message("All staging data saved to /data/staging")
