library(nflreadr)
library(tidyverse)
library(arrow)
library(progressr)
library(here)
library(DBI)
library(RPostgres)

Sys.setenv(
 DB_HOST = "localhost",
 DB_USER = "nfl_user", # Replace with your local user
 DB_PASS = "nfl_pass" # Replace with your local password
)

con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "nfl",
  host = Sys.getenv("DB_HOST"),
  user = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASS")
)

con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "nfl",
  host = "/tmp/nfl-modeling:europe-west2:nfl-pg-01", 
  user = "nfl_app",
  password = "CHOOSE_A_STRONG_PASS"
)

# Define seasons and weeks
seasons <- lubridate::year(Sys.Date())
weeks <- max(nflreadr::get_current_week() - 2, 1):nflreadr::get_current_week()

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
source(here("etl", "R", "step2_process", "step2_team_strength_process_functions.R"))
source(here("etl", "R", "step3_sql", "step3_team_rankings_long_functions.R"))
source(here("etl", "R", "step3_sql", "step3_parquet_to_postgres_functions.R")) # Needed for create_index
source(here("etl", "R", "step3_sql", "step3_database_file_prep_functions.R")) # Needed for create/drop schema
source(here("etl", "R", "utils.R")) # Assumed to have drop_schema, create_schema functions
source(here("etl", "R", "step5_modeling_data", "step5_game_model_assembly_functions.R"))

################################################################################
# Prepare the database (drop and re-create stage schema)
################################################################################
drop_schema(con, schema = "stage")
create_schema(con, schema = "stage")

################################################################################
# Play-by-play data (nflfastR)
################################################################################
pbp <- with_progress(nflreadr::load_pbp(seasons = seasons)) %>% 
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
  summarise_team_game_features() %>% 
  distinct()

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.pbp_tbl limit 5"))

DBI::dbWriteTable(
  con, 
  DBI::Id(schema = "stage", table = "pbp_tbl"), 
  pbp_feats %>% dplyr::select(any_of(tmp_names)), 
  overwrite = TRUE
)
create_index(con = con, schema = 'stage', table = 'pbp_tbl', id_cols = c("game_id","play_id"), unique = TRUE)

#tmp_names <- names(DBI::dbGetQuery(con, "select * from stage.pbp_games_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "pbp_games_tbl"), game_team_feats, overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'pbp_games_tbl', id_cols = c("game_id", "posteam"), unique = TRUE)

################################################################################
# Team Strength
################################################################################
games <- with_progress(nflreadr::load_schedules(seasons)) %>% distinct()
games_cleaned <- clean_schedule_data(games) %>% 
  dplyr::mutate(
    game_type = toupper(game_type),
    season_type = dplyr::if_else(game_type == "REG", "REG", "POST", missing = "POST"),
    game_type = dplyr::if_else(game_type == "REG", "REG", "POST", missing = "POST"),
    home_team = dplyr::recode(home_team, OAK = "LV", STL = "LA", SD = "LAC", .default = home_team),
    away_team = dplyr::recode(away_team, OAK = "LV", STL = "LA", SD = "LAC", .default = away_team)
  )

pbp_ratings <- pbp %>% 
  left_join(., games %>% dplyr::select(game_id, season, week), by='game_id')

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.team_strength_tbl limit 5"))

ratings <- build_team_strength_v01(
  pbp = pbp,
  games = games_cleaned, # optional but recommended for bye-week rows
  w_min = 0.25,
  H = 4,
  beta = 0.7,
  min_eff_plays = 20,
  keep_components = FALSE
) %>%
  mutate_if(is.numeric, round, 3) %>%
  filter(week %in% weeks) %>% filter(season %in% seasons) %>% 
  dplyr::select(all_of(tmp_names))

DBI::dbWriteTable(con, DBI::Id("stage", "team_strength_tbl"), ratings, overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'team_strength_tbl', id_cols = c("team", "season", "week"), unique = TRUE)

################################################################################
# Rosters (nflreadr)
################################################################################
rosters <- with_progress(nflreadr::load_rosters(seasons)) %>% distinct() %>% filter(status == 'ACT')

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.rosters_tbl limit 5"))
roster_clean <- process_rosters(rosters) %>% 
  distinct() %>% 
  mutate(team_id = team) %>% 
  dplyr::select(all_of(tmp_names))
DBI::dbWriteTable(con, DBI::Id("stage", "rosters_tbl"), roster_clean, overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'rosters_tbl', id_cols = c("season","player_id","team", "full_name"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.roster_summary_tbl limit 5"))
roster_summary <- summarize_rosters_by_team_season(roster_clean) %>% 
  distinct() %>% 
  dplyr::select(all_of(tmp_names))
DBI::dbWriteTable(con, DBI::Id("stage", "roster_summary_tbl"), roster_summary, overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'roster_summary_tbl', id_cols = c("season","team"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.roster_position_summary_tbl limit 5"))
roster_position_summary <- summarize_rosters_by_team_position(roster_clean) %>% 
  distinct() %>% 
  dplyr::select(all_of(tmp_names))
DBI::dbWriteTable(con, DBI::Id("stage", "roster_position_summary_tbl"), roster_position_summary, overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'roster_position_summary_tbl', id_cols = c("season","team","position"), unique = TRUE)

################################################################################
# Depth charts (nflreadr)
################################################################################
depth_charts <- with_progress(nflreadr::load_depth_charts(seasons)) %>%
  mutate(
    d = as.Date(substr(dt, 1, 10)), # date only; ignore time/tz
    is_wed = format(d, "%u") == "3" # ISO weekday: Mon=1 ... Sun=7
  ) %>%
  filter(is_wed, d >= as.Date("2025-09-03")) %>%
  mutate(
    week = 1L + ((as.integer(d - as.Date("2025-09-03"))) %/% 7L),
    season = seasons
  ) %>%
  select(-is_wed) %>% 
  filter(season %in% seasons) %>% 
  filter(week %in% weeks) %>% 
  mutate(depth_position = clean_position_update(pos_abb),
         game_type = ifelse(max(weeks) %in% 1:18, 'REG', 'POST'),
         depth_team = pos_rank,
         club_code = team,
         full_name = player_name)

# 1. Extract starters and add cleaned position columns
starters <- filter_depth_chart_starters(depth_charts) %>% 
  mutate(
    position = clean_position(position),
    position_group = dplyr::case_when(
      position %in% c("C", "LG", "LT", "OL", "RG", "RT") ~ "OL",
      position %in% c("QB") ~ "QB",
      position %in% c("WR", "TE") ~ "REC",
      position %in% c("RB") ~ "RB",
      position %in% c("CB", "DL", "DT", "EDGE", "FS", "MLB", "OLB", "SS") ~ "DEF",
      position %in% c('K') ~ 'K',
      position %in% c('ST', 'P') ~ 'ST',
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
lineup_stability_scores <- get_lineup_stability_by_week(starters) %>% distinct() %>% 
  rename(position = position_group)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.depth_charts_starters_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "depth_charts_starters_tbl"), 
                  starter_switches_all %>% 
                    filter(week %in% weeks) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'depth_charts_starters_tbl', id_cols = c("season","week","team","gsis_id") , unique = FALSE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.depth_charts_qb_team_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "depth_charts_qb_team_tbl"), qb_stats_by_team_season %>% 
                    filter(week %in% weeks) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'depth_charts_qb_team_tbl', id_cols = c("season","week","team") , unique = FALSE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.depth_charts_player_starts_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "depth_charts_player_starts_tbl"), 
                  player_start_totals_season %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'depth_charts_player_starts_tbl', id_cols = c('team','season','position','gsis_id'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.depth_charts_position_stability_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "depth_charts_position_stability_tbl"), 
                  lineup_stability_scores %>% 
                    filter(week %in% weeks) %>% 
                    mutate(position_group = position) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'depth_charts_position_stability_tbl', id_cols = c('season','team','week','position'), unique = TRUE)

################################################################################
# Next Gen Stats (nflreadr)
################################################################################
ngs <- with_progress(nflreadr::load_nextgen_stats(seasons)) %>% distinct()

nextgen_stats_cleaned <- process_nextgen_stats(ngs) %>% distinct()
nextgen_stats_player_season <- aggregate_nextgen_by_season(nextgen_stats_cleaned) %>% distinct()
nextgen_stats_player_career <- aggregate_nextgen_by_career(nextgen_stats_cleaned) %>% distinct()
nextgen_stats_player_postseason <- aggregate_nextgen_postseason(nextgen_stats_cleaned) %>% distinct()
nextgen_stats_player_season_cumulative <- compute_cumulative_nextgen_stats(nextgen_stats_cleaned) %>% distinct()

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.nextgen_stats_player_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "nextgen_stats_player_season_tbl"), 
                  nextgen_stats_player_season %>% 
                    dplyr::select(all_of(tmp_names)), 
                  overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'nextgen_stats_player_season_tbl', id_cols = c("season","player_gsis_id","team_abbr"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.nextgen_stats_player_career_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "nextgen_stats_player_career_tbl"), 
                  nextgen_stats_player_career %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'nextgen_stats_player_career_tbl', id_cols = c("player_gsis_id"), unique = TRUE)

# tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.nextgen_stats_player_postseason_tbl limit 5"))
# DBI::dbWriteTable(con, DBI::Id("stage", "nextgen_stats_player_postseason_tbl"), nextgen_stats_player_postseason, overwrite = TRUE)
# create_index(con = con, schema = 'stage', table = 'nextgen_stats_player_postseason_tbl', id_cols = c("player_gsis_id","team_abbr"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.nextgen_stats_player_weekly_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "nextgen_stats_player_weekly_tbl"), 
                  nextgen_stats_player_season_cumulative %>% 
                    filter(week %in% weeks) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'nextgen_stats_player_weekly_tbl', id_cols = c("season","season_type","week","player_gsis_id"), unique = TRUE)

################################################################################
# Snapcount
################################################################################
snapcount <- with_progress(nflreadr::load_snap_counts(seasons)) %>% distinct()
snapcount_weekly <- snapcount %>% distinct()
snapcount_season <- summarize_snapcounts_season(snapcount) %>% distinct()
snapcount_career <- summarize_snapcounts_career(snapcount) %>% distinct()

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.snapcount_weekly_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "snapcount_weekly_tbl"), 
                  snapcount_weekly %>% 
                    filter(week %in% weeks) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'snapcount_weekly_tbl', id_cols = c("game_id","pfr_player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.snapcount_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "snapcount_season_tbl"), 
                  snapcount_season %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'snapcount_season_tbl', id_cols = c("season","pfr_player_id","team", "position"), unique = FALSE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.snapcount_career_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "snapcount_career_tbl"), 
                  snapcount_career %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'snapcount_career_tbl', id_cols = c("pfr_player_id"), unique = FALSE)

################################################################################
# ESPN QBR
################################################################################
espn_qbr <- with_progress(nflreadr::load_espn_qbr(seasons)) %>% distinct()
espn_qbr_season <- espn_qbr %>% distinct()
espn_qbr_career <- espn_qbr_career_by_season_type(espn_qbr) %>% distinct()

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.espn_qbr_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "espn_qbr_season_tbl"), 
                  espn_qbr_season %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'espn_qbr_season_tbl', id_cols = c("season","season_type","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.espn_qbr_career_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "espn_qbr_career_tbl"), 
                  espn_qbr_career %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'espn_qbr_career_tbl', id_cols = c("player_id","season_type"), unique = FALSE)

################################################################################
# Player Stats: offense
################################################################################
offense_stats <- with_progress(nflreadr::load_player_stats(seasons)) %>% 
  filter(position %in% c('QB', 'RB', 'WR', 'TE')) %>% 
  distinct() %>% 
  mutate(recent_team = team,
         interceptions = passing_interceptions,
         sacks = sacks_suffered,
         sack_yards = sack_yards_lost,
         dakota = 0)
offense_stats_full <- with_progress(nflreadr::load_player_stats(seasons=TRUE)) %>% 
  filter(position %in% c('QB', 'RB', 'WR', 'TE')) %>% 
  distinct() %>% 
  filter(player_id %in% offense_stats$player_id) %>% 
  mutate(recent_team = team,
         interceptions = passing_interceptions,
         sacks = sacks_suffered,
         sack_yards = sack_yards_lost,
         dakota = 0)

weekly_qb_stats <- process_qb_stats(offense_stats_full) %>% distinct()
season_qb_stats <- aggregate_qb_season_stats(weekly_qb_stats) %>% distinct()
career_qb_stats <- aggregate_qb_career_stats(weekly_qb_stats) %>% distinct()

weekly_rb_stats <- process_rb_stats(offense_stats_full) %>% distinct()
season_rb_stats <- aggregate_rb_season_stats(weekly_rb_stats) %>% distinct()
career_rb_stats <- aggregate_rb_career_stats(weekly_rb_stats) %>% distinct()

weekly_wr_stats <- process_receiver_stats(offense_stats_full, position_group = 'WR') %>% distinct()
season_wr_stats <- aggregate_receiver_season_stats(weekly_wr_stats) %>% distinct()
career_wr_stats <- aggregate_receiver_career_stats(weekly_wr_stats) %>% distinct()

weekly_te_stats <- process_receiver_stats(offense_stats_full, position_group = 'TE') %>% distinct()
season_te_stats <- aggregate_receiver_season_stats(weekly_te_stats) %>% distinct()
career_te_stats <- aggregate_receiver_career_stats(weekly_te_stats) %>% distinct()

weekly_team_stats <- aggregate_offense_team_week_stats(offense_stats_full) %>% distinct() %>% rename(team = recent_team)
season_team_stats <- aggregate_offense_team_season_stats(offense_stats_full) %>% distinct() %>% rename(team = recent_team)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.weekly_stats_qb_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "weekly_stats_qb_tbl"), 
                  weekly_qb_stats %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'weekly_stats_qb_tbl', id_cols = c("season", "recent_team", "season_type","week","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.season_stats_qb_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "season_stats_qb_tbl"), 
                  season_qb_stats %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'season_stats_qb_tbl', id_cols = c("season","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.career_stats_qb_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "career_stats_qb_tbl"), 
                  career_qb_stats %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'career_stats_qb_tbl', id_cols = c('player_id'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.weekly_stats_rb_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "weekly_stats_rb_tbl"), 
                  weekly_rb_stats %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'weekly_stats_rb_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.season_stats_rb_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "season_stats_rb_tbl"), 
                  season_rb_stats %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'season_stats_rb_tbl', id_cols = c("season","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.career_stats_rb_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "career_stats_rb_tbl"), 
                  career_rb_stats %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'career_stats_rb_tbl', id_cols = c('player_id'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.weekly_stats_wr_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "weekly_stats_wr_tbl"), 
                  weekly_wr_stats %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'weekly_stats_wr_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.season_stats_wr_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "season_stats_wr_tbl"), 
                  season_wr_stats %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'season_stats_wr_tbl', id_cols = c("season","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.career_stats_wr_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "career_stats_wr_tbl"), 
                  career_wr_stats %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'career_stats_wr_tbl', id_cols = c('player_id'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.weekly_stats_te_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "weekly_stats_te_tbl"), 
                  weekly_te_stats %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'weekly_stats_te_tbl', id_cols = c("season","recent_team", "season_type","week","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.season_stats_te_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "season_stats_te_tbl"), 
                  season_te_stats %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'season_stats_te_tbl', id_cols = c("season","player_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.career_stats_te_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "career_stats_te_tbl"), 
                  career_te_stats %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'career_stats_te_tbl', id_cols = c('player_id'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.off_team_stats_week_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "off_team_stats_week_tbl"), 
                  weekly_team_stats %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'off_team_stats_week_tbl', id_cols = c("season","week","team"), unique = TRUE)


tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.off_team_stats_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "off_team_stats_season_tbl"), 
                  season_team_stats %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'off_team_stats_season_tbl', id_cols = c("season","team"), unique = TRUE)

################################################################################
# Player Stats: defense
################################################################################
defense_stats <- with_progress(nflreadr::load_player_stats(seasons)) %>% 
  filter(position_group %in% c('DL', 'LB', 'DB')) %>% 
  distinct() %>% 
  mutate(def_tackles = def_tackles_solo,
         def_fumble_recovery_own = fumble_recovery_own,
         def_fumble_recovery_yards_own = fumble_recovery_yards_own,
         def_fumble_recovery_opp = fumble_recovery_opp,
         def_fumble_recovery_yards_opp = fumble_recovery_yards_opp,
         def_safety = def_safeties,
         def_penalty = penalties,
         def_penalty_yards = penalty_yards)
defense_stats_full <- with_progress(nflreadr::load_player_stats(seasons=TRUE)) %>% 
  filter(position_group %in% c('DL', 'LB', 'DB')) %>% 
  distinct() %>% 
  filter(player_id %in% defense_stats$player_id) %>% 
  mutate(def_tackles = def_tackles_solo,
         def_fumble_recovery_own = fumble_recovery_own,
         def_fumble_recovery_yards_own = fumble_recovery_yards_own,
         def_fumble_recovery_opp = fumble_recovery_opp,
         def_fumble_recovery_yards_opp = fumble_recovery_yards_opp,
         def_safety = def_safeties,
         def_penalty = penalties,
         def_penalty_yards = penalty_yards)

def_player_stats_cleaned <- process_defensive_player_stats(defense_stats_full) %>% distinct()
def_player_stats_season <- summarize_defensive_player_stats_by_season(def_player_stats_cleaned) %>% distinct()
def_team_stats_season <- summarize_defensive_stats_by_team_season(def_player_stats_cleaned) %>% distinct()
def_team_stats_weekly <- summarize_defensive_stats_by_team_weekly(def_player_stats_cleaned) %>% distinct()
def_player_stats_career <- summarize_defensive_stats_by_player(def_player_stats_cleaned) %>% distinct()

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.def_player_stats_weekly_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "def_player_stats_weekly_tbl"), 
                  def_player_stats_cleaned %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'def_player_stats_weekly_tbl', id_cols = c('player_id','team','season','week'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.def_player_stats_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "def_player_stats_season_tbl"), 
                  def_player_stats_season %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'def_player_stats_season_tbl', id_cols = c('player_id','season'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.def_player_stats_career_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "def_player_stats_career_tbl"), 
                  def_player_stats_career %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'def_player_stats_career_tbl', id_cols = c('player_id'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.def_team_stats_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "def_team_stats_season_tbl"), 
                  def_team_stats_season %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'def_team_stats_season_tbl', id_cols = c('team','season'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.def_team_stats_week_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "def_team_stats_week_tbl"), 
                  def_team_stats_weekly %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'def_team_stats_week_tbl', id_cols = c("season","week","team"), unique = TRUE)

################################################################################
# Player Stats: ST
################################################################################
kicking_stats <- with_progress(nflreadr::load_player_stats(seasons)) %>% 
  distinct() %>% 
  filter(position == 'K')
kicking_stats_full <- with_progress(nflreadr::load_player_stats(seasons=TRUE)) %>% 
  distinct() %>% 
  filter(player_id %in% kicking_stats$player_id) %>% 
  filter(position == 'K')

st_stats_cleaned <- process_special_teams_stats(kicking_stats_full) %>% distinct()
st_stats_games <- add_cumulative_special_teams_stats(st_stats_cleaned) %>% distinct()
st_stats_season <- summarize_special_teams_by_season(st_stats_cleaned) %>% distinct()

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.st_player_stats_weekly_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "st_player_stats_weekly_tbl"), 
                  st_stats_games %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'st_player_stats_weekly_tbl', id_cols = c("season","season_type","week","player_id"), unique = FALSE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.st_player_stats_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "st_player_stats_season_tbl"), 
                  st_stats_season %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'st_player_stats_season_tbl', id_cols = c("season","player_id"), unique = TRUE)

################################################################################
# ID Map
################################################################################
tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.id_map_tbl limit 5"))
id_map <- with_progress(nflreadr::load_rosters(seasons)) %>% 
  dplyr::select(full_name, first_name, last_name, contains("id")) %>% 
  distinct() %>% 
  dplyr::select(all_of(tmp_names))
DBI::dbWriteTable(con, DBI::Id("stage", "id_map_tbl"), id_map, overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'id_map_tbl', id_cols = c("gsis_id", "espn_id", "full_name"), unique = FALSE)

################################################################################
# Contracts (nflreadr)
################################################################################
contracts <- with_progress(nflreadr::load_contracts()) %>% distinct() %>% 
  filter(gsis_id %in% id_map$gsis_id)
contracts_clean <- clean_contracts_data(contracts) %>% distinct()
position_cap_pct <- summarise_position_cap_pct(contracts_clean) %>% distinct()
qb_contracts <- add_qb_contract_metadata(contracts_clean) %>% distinct()

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.contracts_position_cap_pct_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "contracts_position_cap_pct_tbl"), 
                  position_cap_pct %>% 
                    distinct() %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'contracts_position_cap_pct_tbl', id_cols = c("position","year_signed","team"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.contracts_qb_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "contracts_qb_tbl"), 
                  qb_contracts %>% 
                    distinct() %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'contracts_qb_tbl', id_cols = c("gsis_id", "team", "year_signed") , unique = FALSE)

################################################################################
# Schedule / Game metadata (nflfastR)
################################################################################
schedule <- with_progress(nflreadr::load_schedules(seasons)) %>% distinct()
schedule_clean <- clean_schedule_data(schedule)
weekly_results <- get_weekly_season_table(schedule)
season_results <- summarize_season_team_results(schedule) %>% 
  rename(points_for = points_scored)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.games_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "games_tbl"), 
                  schedule_clean %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'games_tbl', id_cols = c('game_id'), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.weekly_results_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "weekly_results_tbl"), 
                  weekly_results %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'weekly_results_tbl', id_cols = c("game_id","team_id"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.season_results_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "season_results_tbl"), 
                  season_results %>% 
                    mutate(points_against = points_allowed,
                           points_scored = points_for) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'season_results_tbl', id_cols = c("season","team_id") , unique = TRUE)

################################################################################
# Long Player Table
################################################################################
weekly_wr <- pivot_player_stats_long(data = weekly_wr_stats)
weekly_te <- pivot_player_stats_long(data = weekly_te_stats)
weekly_qb <- pivot_player_stats_long(data = weekly_qb_stats)
weekly_rb <- pivot_player_stats_long(data = weekly_rb_stats)
weekly_ng_qb <- pivot_ngs_player_stats_long(data = nextgen_stats_player_season_cumulative, 
                                            opponent_df = weekly_qb)
weekly_pbp_qb <- pivot_pbp_game_stats_long(data = game_team_feats)

weekly_pbp_qb <- weekly_qb %>% 
  dplyr::select(player_id, name, position, season, season_type, week, team, opponent) %>% 
  distinct() %>% 
  left_join(., weekly_pbp_qb, by=c('season', 'season_type', 'week', 'team', 'opponent'))

opponent_df <- weekly_rb %>% 
  dplyr::select(team, opponent, season, week, season_type) %>% 
  distinct()
weekly_def <- pivot_def_player_stats_long(data = def_player_stats_cleaned, 
                                          opponent_df = opponent_df)

historic_player_long <- DBI::dbGetQuery(con, "select * from prod.player_weekly_tbl") %>% 
  filter(player_id %in% id_map$gsis_id) %>% 
  filter(!(season %in% seasons & week %in% weeks))

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

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.player_weekly_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "player_weekly_tbl"), weekly_players %>% 
                    mutate_if(is.numeric, round, 3) %>% distinct() %>% 
                    filter(week %in% weeks) %>% filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'player_weekly_tbl', id_cols = c("season","season_type","week","player_id","stat_name", "stat_type"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.player_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "player_season_tbl"), 
                  seasonal_players %>% 
                    distinct() %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'player_season_tbl', id_cols = c("season","season_type","player_id","name", "stat_name","agg_type"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.player_career_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "player_career_tbl"), 
                  career_players %>% 
                    distinct() %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'player_career_tbl', id_cols = c("player_id","season_type","position","name","stat_name", "agg_type"), unique = TRUE)

################################################################################
# Long Team Table
################################################################################
team_schedule <- weekly_results %>% 
  dplyr::select(team = team_id, opponent, season, week, season_type) %>% 
  distinct()

game_id_map <- weekly_results %>% 
  dplyr::select(team = team_id, game_id, season, week) %>% 
  distinct()

weekly_team_strength <- ratings %>% 
  pivot_longer(., -c(season, week, team), names_to = 'stat_name', values_to = 'value') %>% 
  mutate(stat_type = 'base') %>% 
  left_join(., team_schedule, by=c('team', 'season', 'week'))

weekly_off <- pivot_team_stats_long(data=weekly_team_stats, opponent_df = team_schedule, team_col = "team") %>% filter(stat_name != 'games_played')
weekly_def <- pivot_team_stats_long(data=def_team_stats_weekly, opponent_df = team_schedule, team_col = "team")
#weekly_inj <- pivot_team_stats_long(data=injuries_team_weekly, opponent_df = team_schedule, team_col = "team")
weekly_pbp <- pivot_pbp_game_stats_long(data=game_team_feats)
weekly_game_stats <- pivot_game_results_long(data = weekly_results) %>% left_join(., team_schedule, by=c('team', 'season', 'week'))
weekly_st_stats <- pivot_special_teams_long(data = st_stats_games) %>% left_join(., team_schedule, by=c('team', 'season', 'week', 'season_type'))

# FIX: Combine all current weekly data first, filter it, and then combine with historic data
current_weekly_data <- rbind(
  weekly_off %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
  weekly_def %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
  weekly_st_stats %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
  #weekly_inj %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
  weekly_game_stats,
  weekly_pbp %>% left_join(., game_id_map, by=c('team', 'season', 'week')),
  weekly_team_strength %>% left_join(., game_id_map, by=c('team', 'season', 'week'))
) %>% 
  filter(week %in% weeks) %>% filter(season %in% seasons) %>% distinct()

historic_team_long <- DBI::dbGetQuery(con, "select * from prod.team_weekly_tbl") %>% 
  filter(!(season %in% seasons & week %in% weeks))

weekly_total <- rbind(
  historic_team_long,
  current_weekly_data
) %>% 
  distinct() %>% 
  arrange(team, season, week)

# Aggregation for season and career totals using the combined weekly_total
season_total <- aggregate_team_season_stats(weekly_total) %>% distinct()
alltime_total <- aggregate_team_alltime_stats(weekly_total) %>% distinct()

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.team_weekly_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "team_weekly_tbl"), 
                  weekly_total %>% 
                    mutate_if(is.numeric, round, 3) %>% 
                    filter(week %in% weeks) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'team_weekly_tbl', id_cols = c("season","season_type","week","team","stat_name", "stat_type"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.team_season_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "team_season_tbl"), 
                  season_total %>% 
                    mutate_if(is.numeric, round, 3) %>% 
                    filter(season %in% seasons) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'team_season_tbl', id_cols = c("season","season_type","team","stat_name", "stat_type"), unique = TRUE)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.team_career_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "team_career_tbl"), 
                  alltime_total %>% 
                    mutate_if(is.numeric, round, 3) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'team_career_tbl', id_cols = c("team","season_type","stat_name", "stat_type"), unique = TRUE)

################################################################################
# Weekly Team Rankings
################################################################################
team_ranking_columns <- c(
  # Offense (totals/rates)
  'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'passing_first_downs', 'passing_epa', 'drives',
  'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_first_downs', 'rushing_epa', 'points_scored',
  'fg_pct',
  # Defensive
  'def_tackles', 'def_tackles_for_loss', 'def_fumbles_forced',
  'def_sacks', 'def_qb_hits', 'def_interceptions', 'def_fumbles',
  'def_penalty', 'points_allowed',
  'def_passing_yards_allowed', 'def_passing_tds_allowed', 'def_passing_first_downs_allowed',
  'def_pass_epa_allowed', 'def_drives_allowed', 'def_carries_allowed',
  'def_rushing_yards_allowed', 'def_rushing_tds_allowed', 'def_rushing_first_downs_allowed',
  'def_rushing_epa_allowed'
)

team_rankings_long <- rank_team_stats_weekly(weekly_total, team_ranking_columns)

tmp_names <- names(DBI::dbGetQuery(con, "select * from prod.team_weekly_rankings_tbl limit 5"))
DBI::dbWriteTable(con, DBI::Id("stage", "team_weekly_rankings_tbl"), 
                  team_rankings_long %>% 
                    filter(season %in% seasons) %>% 
                    filter(week %in% weeks) %>% 
                    dplyr::select(all_of(tmp_names)), overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'team_weekly_rankings_tbl', 
             id_cols = c("season", "week", "team", "stat_name"), unique = TRUE)

################################################################################
# Modeling Data
################################################################################

current_week <- nflreadr::get_current_week()

pregame_ds <- build_pregame_dataset(
  con                = con,
  seasons            = 2016:2025,
  schema             = "prod",
  games_table        = "games_tbl",
  team_strength_table = "team_strength_tbl",
  injuries_table     = "injuries_position_weekly_tbl"
)

# Remove ties
current_season <- nflreadr::get_current_season()
current_week <- nflreadr::get_current_week()

pregame_ds <- pregame_ds %>%
  dplyr::filter(!(season == current_season & week > current_week)) %>%
  dplyr::filter(!(!is.na(home_score) & home_score == away_score)) %>% 
  filter(season %in% seasons) %>% 
  filter(week %in% weeks)

# 1 row per game
stopifnot(nrow(pregame_ds) == dplyr::n_distinct(pregame_ds$game_id))

# diff columns really are home - away (spot-check a few)
stopifnot(all.equal(pregame_ds$diff_rating_net,
                    pregame_ds$home_rating_net - pregame_ds$away_rating_net, check.attributes = FALSE))
stopifnot(all.equal(pregame_ds$diff_qb_prior,
                    pregame_ds$home_qb_prior - pregame_ds$away_qb_prior, check.attributes = FALSE))

DBI::dbWriteTable(con, DBI::Id("stage", "game_level_modeling_tbl"), pregame_ds, overwrite = TRUE)
create_index(con = con, schema = 'stage', table = 'game_level_modeling_tbl', id_cols = c("game_id", "season", "week"), unique = TRUE)

message("All staging data successfully loaded to the database!")

#' # Define schemas
#' SRC_SCHEMA <- "stage"
#' DEST_SCHEMA <- "prod"
#' 
#' # --- Main Functions ---
#' 
#' #' Fetch column metadata for a given schema
#' fetch_schema_metadata <- function(con, schema) {
#'   sql <- glue(
#'     "SELECT 
#'        table_name, 
#'        column_name, 
#'        data_type,
#'        ordinal_position
#'      FROM information_schema.columns 
#'      WHERE table_schema = '{schema}'
#'      ORDER BY table_name, ordinal_position;"
#'   )
#'   DBI::dbGetQuery(con, sql)
#' }
#' 
#' #' Check for column mismatches between two schemas
#' check_column_consistency <- function(con) {
#'   
#'   message(glue("--- Comparing columns between {SRC_SCHEMA} and {DEST_SCHEMA} ---"))
#'   
#'   # 1. Fetch metadata for both schemas
#'   stage_df <- fetch_schema_metadata(con, SRC_SCHEMA)
#'   prod_df <- fetch_schema_metadata(con, DEST_SCHEMA)
#'   
#'   if (nrow(stage_df) == 0 || nrow(prod_df) == 0) {
#'     message("One or both schemas are empty. No comparison needed.")
#'     return(invisible(NULL))
#'   }
#'   
#'   # 2. Get the list of overlapping tables
#'   stage_tables <- unique(stage_df$table_name)
#'   prod_tables <- unique(prod_df$table_name)
#'   
#'   overlapping_tables <- intersect(stage_tables, prod_tables)
#'   
#'   if (length(overlapping_tables) == 0) {
#'     message("No tables with matching names found in both schemas.")
#'     return(invisible(NULL))
#'   }
#'   
#'   message(glue("Found {length(overlapping_tables)} overlapping tables to check: {paste(overlapping_tables, collapse = ', ')}"))
#'   
#'   mismatches_found <- FALSE
#'   
#'   for (table in overlapping_tables) {
#'     
#'     # Extract column names for the current table from both schemas
#'     stage_cols <- stage_df$column_name[stage_df$table_name == table]
#'     prod_cols <- prod_df$column_name[prod_df$table_name == table]
#'     
#'     # Check for exact match in name AND order
#'     if (identical(stage_cols, prod_cols)) {
#'       message(glue("  ✅ PASS: Table '{table}' columns match exactly."))
#'       next
#'     }
#'     
#'     mismatches_found <- TRUE
#'     message(glue("  ❌ FAIL: Table '{table}' structure mismatch!"))
#'     
#'     # Find differences (name check)
#'     stage_only <- setdiff(stage_cols, prod_cols)
#'     prod_only <- setdiff(prod_cols, stage_cols)
#'     
#'     if (length(stage_only) > 0) {
#'       message(glue("      - In {SRC_SCHEMA} only: {paste(stage_only, collapse = ', ')}"))
#'     }
#'     if (length(prod_only) > 0) {
#'       message(glue("      - In {DEST_SCHEMA} only: {paste(prod_only, collapse = ', ')}"))
#'     }
#'     
#'     # Check if names match but order is wrong
#'     if (length(stage_only) == 0 && length(prod_only) == 0) {
#'       message("      - Column names match, but the **order** is different.")
#'     }
#'   }
#'   
#'   message("----------------------------------------------------------")
#'   if (mismatches_found) {
#'     stop("Column consistency check failed. Fix schema differences before upserting.")
#'   } else {
#'     message("Column check complete. All overlapping tables are consistent.")
#'   }
#' }
#' 
#' check_column_consistency(con)
