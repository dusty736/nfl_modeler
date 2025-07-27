################################################################################
# step2_contracts_process.R
################################################################################

library(arrow)
library(here)
library(dplyr)

source(here("etl", "R", "step2_process", "step2_contracts_process_functions.R"))

################################################################################
# Load raw data
################################################################################
contracts_raw <- arrow::read_parquet(here("data", "raw", "contracts.parquet"))

################################################################################
# Clean and normalize
################################################################################
contracts_clean <- clean_contracts_data(contracts_raw)
position_cap_pct <- summarise_position_cap_pct(contracts_clean)
qb_contracts <- add_qb_contract_metadata(contracts_clean)

################################################################################
# Save processed output
################################################################################
arrow::write_parquet(position_cap_pct, "data/processed/contracts_position_cap_pct.parquet")
arrow::write_parquet(qb_contracts, "data/processed/contracts_qb.parquet")

