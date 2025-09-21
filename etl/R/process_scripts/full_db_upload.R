################################################################################
# full_db_upload.R
################################################################################

source(here::here("etl", "R", "step1_download", "step1_data_download.R"))

source(here::here("etl", "R", "step2_process", "step2_contracts_process.R"))
source(here::here("etl", "R", "step2_process", "step2_def_player_stats_process.R"))
source(here::here("etl", "R", "step2_process", "step2_depth_charts_process.R"))
source(here::here("etl", "R", "step2_process", "step2_injuries_process.R"))
source(here::here("etl", "R", "step2_process", "step2_nextgen_stats_process.R"))
source(here::here("etl", "R", "step2_process", "step2_off_player_stats_process.R"))
source(here::here("etl", "R", "step2_process", "step2_participation_process.R"))
source(here::here("etl", "R", "step2_process", "step2_pbp_process.R"))
source(here::here("etl", "R", "step2_process", "step2_rosters_process.R"))
source(here::here("etl", "R", "step2_process", "step2_schedule_process.R"))
source(here::here("etl", "R", "step2_process", "step2_st_player_stats_process.R"))
source(here::here("etl", "R", "step2_process", "step2_espn_qbr_process.R"))
source(here::here("etl", "R", "step2_process", "step2_idmap_process.R"))
source(here::here("etl", "R", "step2_process", "step2_team_metadata_process.R"))
source(here::here("etl", "R", "step2_process", "step2_snapcount_process.R"))
source(here::here("etl", "R", "step2_process", "step2_team_strength_process.R"))

source(here::here("etl", "R", "step3_sql", "step3_long_player_format.R"))
source(here::here("etl", "R", "step3_sql", "step3_long_team_format.R"))
source(here::here("etl", "R", "step3_sql", "step3_team_rankings_long.R"))
source(here::here("etl", "R", "step3_sql", "step3_database_file_prep.R"))
source(here::here("etl", "R", "step3_sql", "step3_parquet_to_postgres.R"))

source(here::here("etl", "R", "step5_modeling_data", "step5_game_model_assebly.R"))

testthat::test_dir("tests/testthat")
