################################################################################
# step3_database_file_prep
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step3_sql", "step3_database_file_prep_functions.R"))
source(here("etl", "R", "utils.R"))

################################################################################
# Process Data
################################################################################

# PBP 
format_pbp_for_sql(file.path("data", "processed", "pbp_cleaned.parquet"),
                   file.path("data", "for_database", "pbp_tbl.parquet"))

# Games (games.parquet)
format_game_for_sql(file.path("data", "processed", "games.parquet"),
                   file.path("data", "for_database", "games_tbl.parquet"))

# Seasons (season_results.parquet)
format_season_for_sql(file.path("data", "processed", "season_results.parquet"),
                    file.path("data", "for_database", "season_results_tbl.parquet"))

# Weeks (weekly_results.parquet)
format_weeks_for_sql(file.path("data", "processed", "weekly_results.parquet"),
                     file.path("data", "for_database", "weekly_results_tbl.parquet"))

# Roster (rosters.parquet)
format_roster_for_sql(file.path("data", "processed", "rosters.parquet"),
                      file.path("data", "for_database", "rosters_tbl.parquet"))

# Roster Summary (roster_summary.parquet)
format_roster_summary_for_sql(file.path("data", "processed", "roster_summary.parquet"),
                              file.path("data", "for_database", "roster_summary_tbl.parquet"))

# Roster Position Summary (roster_position_summary.parquet)
format_roster_position_summary_for_sql(file.path("data", "processed", "roster_position_summary.parquet"),
                                       file.path("data", "for_database", "roster_position_summary_tbl.parquet"))

# Weekly Qb Stats (weekly_stats_qb.parquet)
format_weekly_qb_stats_for_sql(file.path("data", "processed", "weekly_stats_qb.parquet"),
                               file.path("data", "for_database", "weekly_stats_qb_tbl.parquet"))

# Weekly Rb Stats (weekly_stats_rb.parquet)
format_weekly_rb_stats_for_sql(file.path("data", "processed", "weekly_stats_rb.parquet"),
                               file.path("data", "for_database", "weekly_stats_rb_tbl.parquet"))

# Weekly Wrte Stats (weekly_stats_wr.parquet)
format_weekly_wrte_stats_for_sql(file.path("data", "processed", "weekly_stats_wr.parquet"),
                                 file.path("data", "for_database", "weekly_stats_wr_tbl.parquet"))

# Weekly Wrte Stats (weekly_stats_te.parquet)
format_weekly_wrte_stats_for_sql(file.path("data", "processed", "weekly_stats_te.parquet"),
                                 file.path("data", "for_database", "weekly_stats_te_tbl.parquet"))

# Season Qb Stats (season_stats_qb.parquet)
format_season_qb_stats_for_sql(file.path("data", "processed", "season_stats_qb.parquet"),
                               file.path("data", "for_database", "season_stats_qb_tbl.parquet"))

# Season Rb Stats (season_stats_rb.parquet)
format_season_rb_stats_for_sql(file.path("data", "processed", "season_stats_rb.parquet"),
                               file.path("data", "for_database", "season_stats_rb_tbl.parquet"))

# Season Wrte Stats (season_stats_wr.parquet)
format_season_wrte_stats_for_sql(file.path("data", "processed", "season_stats_wr.parquet"),
                                 file.path("data", "for_database", "season_stats_wr_tbl.parquet"))

# Season Wrte Stats (season_stats_te.parquet)
format_season_wrte_stats_for_sql(file.path("data", "processed", "season_stats_te.parquet"),
                                 file.path("data", "for_database", "season_stats_te_tbl.parquet"))

# Career Qb Stats (career_stats_qb.parquet)
format_career_qb_stats_for_sql(file.path("data", "processed", "career_stats_qb.parquet"),
                               file.path("data", "for_database", "career_stats_qb_tbl.parquet"))

# Career Rb Stats (career_stats_rb.parquet)
format_career_rb_stats_for_sql(file.path("data", "processed", "career_stats_rb.parquet"),
                               file.path("data", "for_database", "career_stats_rb_tbl.parquet"))

# Career Wrte Stats (career_stats_wr.parquet)
format_career_wrte_stats_for_sql(file.path("data", "processed", "career_stats_wr.parquet"),
                                 file.path("data", "for_database", "career_stats_wr_tbl.parquet"))

# Career Wrte Stats (career_stats_te.parquet)
format_career_wrte_stats_for_sql(file.path("data", "processed", "career_stats_te.parquet"),
                                 file.path("data", "for_database", "career_stats_te_tbl.parquet"))

# Injuries Weekly (injuries_weekly.parquet)
format_injuries_weekly_for_sql(file.path("data", "processed", "injuries_weekly.parquet"),
                               file.path("data", "for_database", "injuries_weekly_tbl.parquet"))

# Injuries Team Weekly (injuries_team_weekly.parquet)
format_injuries_team_weekly_for_sql(file.path("data", "processed", "injuries_team_weekly.parquet"),
                                    file.path("data", "for_database", "injuries_team_weekly_tbl.parquet"))

# Injuries Team Season (injuries_team_season.parquet)
format_injuries_team_season_for_sql(file.path("data", "processed", "injuries_team_season.parquet"),
                                    file.path("data", "for_database", "injuries_team_season_tbl.parquet"))

# Injuries Team Position Weekly (injuries_position_weekly.parquet)
format_injuries_team_position_weekly_for_sql(file.path("data", "processed", "injuries_position_weekly.parquet"),
                                             file.path("data", "for_database", "injuries_position_weekly_tbl.parquet"))

# Contracts Qb (contracts_qb.parquet)
format_contracts_qb_for_sql(file.path("data", "processed", "contracts_qb.parquet"),
                            file.path("data", "for_database", "contracts_qb_tbl.parquet"))

# Contracts Cap Pct (contracts_position_cap_pct.parquet)
format_contracts_cap_pct_for_sql(file.path("data", "processed", "contracts_position_cap_pct.parquet"),
                                 file.path("data", "for_database", "contracts_position_cap_pct_tbl.parquet"))

# Special Teams Weekly (st_player_stats_weekly.parquet)
format_weekly_special_teams_for_sql(file.path("data", "processed", "st_player_stats_weekly.parquet"),
                                    file.path("data", "for_database", "st_player_stats_weekly_tbl.parquet"))

# Special Teams Season (st_player_stats_season.parquet)
format_season_special_teams_for_sql(file.path("data", "processed", "st_player_stats_season.parquet"),
                                    file.path("data", "for_database", "st_player_stats_season_tbl.parquet"))

# Defensive Players (def_player_stats_weekly.parquet)
format_weekly_defense_player_stats_for_sql(
  file.path("data", "processed", "def_player_stats_weekly.parquet"),
  file.path("data", "for_database", "def_player_stats_weekly_tbl.parquet")
)


# Defensive Players (def_player_stats_season.parquet)
format_season_defense_player_stats_for_sql(
  file.path("data", "processed", "def_player_stats_season.parquet"),
  file.path("data", "for_database", "def_player_stats_season_tbl.parquet")
)

# Defensive Players (def_player_stats_career.parquet)
format_career_defense_player_stats_for_sql(
  file.path("data", "processed", "def_player_stats_career.parquet"),
  file.path("data", "for_database", "def_player_stats_career_tbl.parquet")
)

# Defensive Teams (def_team_stats_season.parquet)
format_season_defense_team_stats_for_sql(
  file.path("data", "processed", "def_team_stats_season.parquet"),
  file.path("data", "for_database", "def_team_stats_season_tbl.parquet")
)

# Depth Charts (depth_charts_player_starts.parquet)
format_depth_chart_player_starts_for_sql(
  file.path("data", "processed", "depth_charts_player_starts.parquet"),
  file.path("data", "for_database", "depth_charts_player_starts_tbl.parquet")
)

# Depth Charts (depth_charts_position_stability.parquet)
format_depth_chart_position_stability_for_sql(
  file.path("data", "processed", "depth_charts_position_stability.parquet"),
  file.path("data", "for_database", "depth_charts_position_stability_tbl.parquet")
)

# Depth Charts (depth_charts_qb_team.parquet)
format_depth_chart_qb_for_sql(
  file.path("data", "processed", "depth_charts_qb_team.parquet"),
  file.path("data", "for_database", "depth_charts_qb_team_tbl.parquet")
)

# Depth Charts (depth_charts_starters.parquet)
format_depth_chart_starters_for_sql(
  file.path("data", "processed", "depth_charts_starters.parquet"),
  file.path("data", "for_database", "depth_charts_starters_tbl.parquet")
)

# Next Gen Stats (nextgen_stats_player_weekly.parquet)
format_weekly_nextgen_stats_for_sql(
  file.path("data", "processed", "nextgen_stats_player_weekly.parquet"),
  file.path("data", "for_database", "nextgen_stats_player_weekly_tbl.parquet")
)

# Next Gen Stats (nextgen_stats_player_season.parquet)
format_season_nextgen_stats_for_sql(
  file.path("data", "processed", "nextgen_stats_player_season.parquet"),
  file.path("data", "for_database", "nextgen_stats_player_season_tbl.parquet")
)

# Next Gen Stats (nextgen_stats_player_postseason.parquet)
format_postseason_nextgen_stats_for_sql(
  file.path("data", "processed", "nextgen_stats_player_postseason.parquet"),
  file.path("data", "for_database", "nextgen_stats_player_postseason_tbl.parquet")
)

# Next Gen Stats (nextgen_stats_player_career.parquet)
format_career_nextgen_stats_for_sql(
  file.path("data", "processed", "nextgen_stats_player_career.parquet"),
  file.path("data", "for_database", "nextgen_stats_player_career_tbl.parquet")
)

# Participation Offense (participation_offense_pbp.parquet)
format_participation_offense_pbp_for_sql(
  file.path("data", "processed", "participation_offense_pbp.parquet"),
  file.path("data", "for_database", "participation_offense_pbp_tbl.parquet")
)

# Participation Offense (participation_offense_game.parquet)
format_participation_offense_game_for_sql(
  file.path("data", "processed", "participation_offense_game.parquet"),
  file.path("data", "for_database", "participation_offense_game_tbl.parquet")
)

# Participation Offense (participation_offense_season.parquet)
format_participation_offense_season_for_sql(
  file.path("data", "processed", "participation_offense_season.parquet"),
  file.path("data", "for_database", "participation_offense_season_tbl.parquet")
)

# Participation Defense (participation_defense_pbp.parquet)
format_participation_defense_pbp_for_sql(
  file.path("data", "processed", "participation_defense_pbp.parquet"),
  file.path("data", "for_database", "participation_defense_pbp_tbl.parquet")
)

# Participation Defense (participation_defense_game.parquet)
format_participation_defense_game_for_sql(
  file.path("data", "processed", "participation_defense_game.parquet"),
  file.path("data", "for_database", "participation_defense_game_tbl.parquet")
)

# Participation Defense (participation_defense_season.parquet)
format_participation_defense_season_for_sql(
  file.path("data", "processed", "participation_defense_season.parquet"),
  file.path("data", "for_database", "participation_defense_season_tbl.parquet")
)

################################################################################
# Summarize Data
################################################################################
file_summary <- summarize_parquet_structure(dir = file.path("data", "for_database"))
data.table::fwrite(file_summary, file.path("data", "database_data_summary.csv"))
