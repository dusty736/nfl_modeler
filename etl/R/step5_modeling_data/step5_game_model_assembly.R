# step5_game_model_assembly.R 

################################################################################
# Setup
################################################################################
library(DBI)
library(RPostgres) 
library(glue) 
library(digest) 
library(here)

source(here("etl", "R", "step5_modeling_data", "step5_game_model_assembly_functions.R"))
source(here("etl", "R", "step3_sql", "step3_parquet_to_postgres_functions.R"))

con <- dbConnect(Postgres(), 
                 dbname = "nfl", 
                 host = "localhost", 
                 port = 5432, 
                 user = "nfl_user", 
                 password = "nfl_pass")

current_week <- nflreadr::get_current_week()

################################################################################
# Process data
################################################################################
pregame_ds <- build_pregame_dataset(
  con                  = con,
  seasons              = 2016:2025,
  schema               = "prod",
  games_table          = "games_tbl",
  team_strength_table  = "team_strength_tbl",
  injuries_table       = "injuries_position_weekly_tbl"
)

# Remove ties
current_season <- nflreadr::get_current_season()
current_week <- nflreadr::get_current_week()

pregame_ds <- pregame_ds %>% 
  dplyr::filter(!(season == current_season & week > current_week)) %>%
  dplyr::filter(!(!is.na(home_score) & home_score == away_score))

# 1 row per game
stopifnot(nrow(pregame_ds) == dplyr::n_distinct(pregame_ds$game_id))

# diff columns really are home - away (spot-check a few)
stopifnot(all.equal(pregame_ds$diff_rating_net,
                    pregame_ds$home_rating_net - pregame_ds$away_rating_net, check.attributes = FALSE))
stopifnot(all.equal(pregame_ds$diff_qb_prior,
                    pregame_ds$home_qb_prior - pregame_ds$away_qb_prior, check.attributes = FALSE))

# targets present & non-missing

# last four columns are the targets (optional aesthetics check)

################################################################################
# Define drop variables in data
################################################################################

# never allow market inputs
drop_market <- c("spread_line", "spread_home")

# 1) Predicting home_win
drop_for_home_win <- c(
  "home_score", "away_score",          # postgame
  "margin", "total_points", "spread_covered",  # other targets/leaks
  drop_market                           # market inputs
)

# 2) Predicting margin
drop_for_margin <- c(
  "home_score", "away_score",
  "home_win", "total_points", "spread_covered",
  drop_market
)

# 3) Predicting total_points
drop_for_total_points <- c(
  "home_score", "away_score",
  "home_win", "margin", "spread_covered",
  drop_market
)

# 4) Predicting spread_covered
drop_for_spread_covered <- c(
  "home_score", "away_score",
  "home_win", "margin", "total_points",
  drop_market
)

################################################################################
# Save
################################################################################

arrow::write_parquet(pregame_ds, file.path("data", "modeling", "game_level_modeling.parquet"))

################################################################################
# Upload to DB
################################################################################

load_parquet_to_postgres(file.path("data", "modeling", "game_level_modeling.parquet"), schema = 'prod', "game_level_modeling_tbl")
create_index(con = con, schema = 'prod', table = 'game_level_modeling_tbl', id_cols = c("game_id", "season", "week"), unique = TRUE)

