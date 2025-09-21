################################################################################
# db_refresh.R
################################################################################

source(here::here("etl", "R", "step4_update_db", "step4_weekly_update_data_download.R"))
source(here::here("etl", "R", "step4_update_db", "step4_weekly_update_to_db.R"))
source(here::here("etl", "R", "step4_update_db", "step4_update_prod.R"))

source(here::here("etl", "R", "step5_modeling_data", "step5_game_model_assembly.R"))
