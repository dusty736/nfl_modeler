################################################################################
# db_refresh.R
################################################################################

library(DBI)
library(here)
library(RPostgres)

source(here::here("etl", "R", "step4_update_db", "step4_weekly_update_data_download_cloud.R"))
source(here::here("etl", "R", "step4_update_db", "step4_update_prod_cloud.R"))

source(here::here("etl", "R", "step5_modeling_data", "step5_game_model_assembly_cloud.R"))
