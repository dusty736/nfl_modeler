# SQL Database Design

# Player Table

| Column       | Type      | Description                         | Key         | Source File       |
| ------------ | --------- | ----------------------------------- | ----------- | ----------------- |
| `player_id`  | `TEXT`    | Unique NFL player identifier        | Primary Key | `rosters.parquet` |
| `full_name`  | `TEXT`    | Player’s full name                  |             | `rosters.parquet` |
| `first_name` | `TEXT`    | Player’s first name                 |             | `rosters.parquet` |
| `last_name`  | `TEXT`    | Player’s last name                  |             | `rosters.parquet` |
| `position`   | `TEXT`    | Primary position (e.g., QB, RB, WR) |             | `rosters.parquet` |
| `height`     | `INTEGER` | Height in inches                    |             | `rosters.parquet` |
| `weight`     | `INTEGER` | Weight in pounds                    |             | `rosters.parquet` |
| `age`        | `NUMERIC` | Age at roster snapshot              |             | `rosters.parquet` |
| `college`    | `TEXT`    | College attended                    |             | `rosters.parquet` |
| `years_exp`  | `INTEGER` | Years of NFL experience             |             | `rosters.parquet` |

# Team Table

| Column       | Type   | Description                                   | Key         | Source File(s)              |
| ------------ | ------ | --------------------------------------------- | ----------- | --------------------------- |
| `team_id`    | `TEXT` | Official team abbreviation (e.g., "NE")       | Primary Key | All files referencing teams |
| `team_name`  | `TEXT` | Full team name (e.g., "New England Patriots") |             | *Manual mapping*            |
| `location`   | `TEXT` | Team location or city (e.g., "New England")   |             | *Manual mapping*            |
| `conference` | `TEXT` | Conference (e.g., "AFC", "NFC")               |             | *Manual mapping*            |
| `division`   | `TEXT` | Division (e.g., "AFC East")                   |             | *Manual mapping*            |

# Game Table

| Column             | Type        | Description                                   | Key         | Source File     |
| ------------------ | ----------- | --------------------------------------------- | ----------- | --------------- |
| `game_id`          | `TEXT`      | Unique identifier (e.g., `2016_01_CAR_DEN`)   | Primary Key | `games.parquet` |
| `season`           | `INTEGER`   | NFL season year                               |             | `games.parquet` |
| `week`             | `INTEGER`   | Week number within season                     |             | `games.parquet` |
| `game_type`        | `TEXT`      | Game type (`REG`, `POST`, `SB`)               |             | `games.parquet` |
| `kickoff`          | `TIMESTAMP` | Kickoff datetime (UTC)                        |             | `games.parquet` |
| `weekday`          | `TEXT`      | Day of the week (e.g., `Sunday`)              |             | `games.parquet` |
| `home_team`        | `TEXT`      | Abbreviation of home team                     |             | `games.parquet` |
| `away_team`        | `TEXT`      | Abbreviation of away team                     |             | `games.parquet` |
| `home_score`       | `INTEGER`   | Final home team score                         |             | `games.parquet` |
| `away_score`       | `INTEGER`   | Final away team score                         |             | `games.parquet` |
| `result`           | `TEXT`      | Outcome for home team (`HOME`, `AWAY`, `TIE`) |             | `games.parquet` |
| `favored_team`     | `TEXT`      | Team favored by the spread                    |             | `games.parquet` |
| `overtime`         | `BOOLEAN`   | `TRUE` if game went to overtime               |             | `games.parquet` |
| `stadium`          | `TEXT`      | Stadium name                                  |             | `games.parquet` |
| `stadium_id`       | `TEXT`      | Unique stadium ID                             |             | `games.parquet` |
| `roof`             | `TEXT`      | Roof type (`outdoors`, `dome`, etc.)          |             | `games.parquet` |
| `surface`          | `TEXT`      | Field surface type (`grass`, `turf`, etc.)    |             | `games.parquet` |
| `temp`             | `INTEGER`   | Temperature in Fahrenheit                     |             | `games.parquet` |
| `wind`             | `INTEGER`   | Wind speed in MPH                             |             | `games.parquet` |
| `referee`          | `TEXT`      | Name of referee                               |             | `games.parquet` |
| `home_qb_id`       | `TEXT`      | `player_id` of starting home quarterback      |             | `games.parquet` |
| `away_qb_id`       | `TEXT`      | `player_id` of starting away quarterback      |             | `games.parquet` |
| `home_qb_name`     | `TEXT`      | Full name of home quarterback                 |             | `games.parquet` |
| `away_qb_name`     | `TEXT`      | Full name of away quarterback                 |             | `games.parquet` |
| `home_coach`       | `TEXT`      | Head coach of home team                       |             | `games.parquet` |
| `away_coach`       | `TEXT`      | Head coach of away team                       |             | `games.parquet` |
| `div_game`         | `BOOLEAN`   | `TRUE` if divisional game                     |             | `games.parquet` |
| `spread_line`      | `NUMERIC`   | Vegas point spread (home team)                |             | `games.parquet` |
| `total_line`       | `NUMERIC`   | Vegas over/under total                        |             | `games.parquet` |
| `away_moneyline`   | `INTEGER`   | Vegas moneyline for away team                 |             | `games.parquet` |
| `home_moneyline`   | `INTEGER`   | Vegas moneyline for home team                 |             | `games.parquet` |
| `away_spread_odds` | `INTEGER`   | Odds for away team spread                     |             | `games.parquet` |
| `home_spread_odds` | `INTEGER`   | Odds for home team spread                     |             | `games.parquet` |
| `under_odds`       | `INTEGER`   | Odds for the under                            |             | `games.parquet` |
| `over_odds`        | `INTEGER`   | Odds for the over                             |             | `games.parquet` |
| `old_game_id`      | `TEXT`      | Legacy game identifier                        |             | `games.parquet` |
| `gsis`             | `TEXT`      | NFL GSIS game ID                              |             | `games.parquet` |
| `nfl_detail_id`    | `TEXT`      | NFL detail UUID                               |             | `games.parquet` |
| `pfr`              | `TEXT`      | Pro-Football-Reference ID                     |             | `games.parquet` |
| `pff`              | `TEXT`      | Pro Football Focus ID                         |             | `games.parquet` |
| `espn`             | `TEXT`      | ESPN game ID                                  |             | `games.parquet` |
| `ftn`              | `TEXT`      | FTN data ID                                   |             | `games.parquet` |

# Season Summary
| Column             | Type      | Description                                                        | Key               | Source File(s)                 |
| ------------------ | --------- | ------------------------------------------------------------------ | ----------------- | ------------------------------ |
| `season`           | `INTEGER` | NFL season year                                                    | Primary Key (1/2) | Derived or external            |
| `team_id`          | `TEXT`    | Team abbreviation (e.g., `KC`, `PHI`)                              | Primary Key (2/2) | Derived or external            |
| `wins`             | `INTEGER` | Regular season wins                                                |                   | External or computed           |
| `losses`           | `INTEGER` | Regular season losses                                              |                   |                                |
| `ties`             | `INTEGER` | Regular season ties                                                |                   |                                |
| `points_for`       | `INTEGER` | Total points scored during regular season                          |                   |                                |
| `points_against`   | `INTEGER` | Total points allowed during regular season                         |                   |                                |
| `playoff_seed`     | `INTEGER` | Postseason seed (if qualified, lower = higher seed)                |                   |                                |
| `made_playoffs`    | `BOOLEAN` | TRUE if team reached the postseason                                |                   |                                |
| `postseason_round` | `TEXT`    | Deepest round reached: `None`, `WC`, `DIV`, `CONF`, `SB`, `SB_WIN` |                   | Derived manually or externally |

# Weeks

| Column               | Type      | Description                                         | Key               | Source File(s)      |
| -------------------- | --------- | --------------------------------------------------- | ----------------- | ------------------- |
| `season`             | `INTEGER` | NFL season                                          | Primary Key (1/3) | `games.parquet`     |
| `team_id`            | `TEXT`    | Team abbreviation                                   | Primary Key (2/3) | `games.parquet`     |
| `week`               | `INTEGER` | Week number                                         | Primary Key (3/3) | `games.parquet`     |
| `season_type`        | `TEXT`    | Season type: `REG`, `POST`, `PRE`, `SB`             |                   | `games.parquet`     |
| `week_label`         | `TEXT`    | Friendly label (e.g., "Week 4", "Wild Card")        |                   | Derived             |
| `game_id`            | `TEXT`    | Game ID for the week, if team played                |                   | `games.parquet`     |
| `date`               | `DATE`    | Game date (kickoff), or week start date             |                   | `games.parquet`     |
| `bye_week`           | `BOOLEAN` | TRUE if team did not play that week                 |                   | Derived             |
| `wins_entering`      | `INTEGER` | Number of wins entering the week                    |                   | Computed            |
| `losses_entering`    | `INTEGER` | Number of losses entering the week                  |                   | Computed            |
| `ties_entering`      | `INTEGER` | Number of ties entering the week                    |                   | Computed            |
| `points_scored_ytd`  | `INTEGER` | Total points scored by team through previous weeks  |                   | Computed from games |
| `points_allowed_ytd` | `INTEGER` | Total points allowed by team through previous weeks |                   | Computed from games |

# Roster

| Column         | Type      | Description                                  | Key                 | Source File                       |
| -------------- | --------- | -------------------------------------------- | ------------------- | --------------------------------- |
| `player_id`    | `TEXT`    | Unique player identifier                     | Primary Key (1/4)   | `rosters.parquet`                 |
| `team_id`      | `TEXT`    | Team abbreviation                            | Primary Key (2/4)   | `rosters.parquet` (`team`)        |
| `season`       | `INTEGER` | NFL season                                   | Primary Key (3/4)   | `rosters.parquet`                 |
| `week`         | `INTEGER` | Week of season                               | Primary Key (4/4)\* | *To be joined/added from context* |
| `full_name`    | `TEXT`    | Player full name                             |                     | `rosters.parquet`                 |
| `first_name`   | `TEXT`    | Player first name                            |                     | `rosters.parquet`                 |
| `last_name`    | `TEXT`    | Player last name                             |                     | `rosters.parquet`                 |
| `position`     | `TEXT`    | Player position (e.g., `QB`, `WR`)           |                     | `rosters.parquet`                 |
| `status`       | `TEXT`    | Roster status (`ACT`, `RES`, etc.)           |                     | `rosters.parquet`                 |
| `age`          | `NUMERIC` | Player age at time of roster                 |                     | `rosters.parquet`                 |
| `height`       | `INTEGER` | Player height in inches                      |                     | `rosters.parquet`                 |
| `weight`       | `INTEGER` | Player weight in pounds                      |                     | `rosters.parquet`                 |
| `college`      | `TEXT`    | College attended                             |                     | `rosters.parquet`                 |
| `years_exp`    | `INTEGER` | Number of NFL seasons completed              |                     | `rosters.parquet`                 |
| `rookie_year`  | `INTEGER` | First year in NFL                            |                     | `rosters.parquet`                 |
| `entry_year`   | `INTEGER` | Year entered NFL (often same as rookie year) |                     | `rosters.parquet`                 |
| `headshot_url` | `TEXT`    | URL to player headshot image                 |                     | `rosters.parquet`                 |
| `esb_id`       | `TEXT`    | NFL ESB identifier                           |                     | `rosters.parquet`                 |

# Weekly QB Stats
| Column                          | Type      | Description                                  | Key               | Source File               |
| ------------------------------- | --------- | -------------------------------------------- | ----------------- | ------------------------- |
| `season`                        | `INTEGER` | NFL season year                              | Primary Key (1/3) | `weekly_stats_qb.parquet` |
| `week`                          | `INTEGER` | Week of the season                           | Primary Key (2/3) | `weekly_stats_qb.parquet` |
| `player_id`                     | `TEXT`    | Unique player identifier                     | Primary Key (3/3) | `weekly_stats_qb.parquet` |
| `season_type`                   | `TEXT`    | Season type (`REG`, `POST`, etc.)            |                   | `weekly_stats_qb.parquet` |
| `position`                      | `TEXT`    | Player position (always `QB` for this table) |                   | `weekly_stats_qb.parquet` |
| `recent_team`                   | `TEXT`    | Team the player played for that week         |                   | `weekly_stats_qb.parquet` |
| `opponent_team`                 | `TEXT`    | Opposing team                                |                   | `weekly_stats_qb.parquet` |
| `completions`                   | `INTEGER` | Completed passes                             |                   | `weekly_stats_qb.parquet` |
| `attempts`                      | `INTEGER` | Pass attempts                                |                   | `weekly_stats_qb.parquet` |
| `passing_yards`                 | `NUMERIC` | Total passing yards                          |                   | `weekly_stats_qb.parquet` |
| `passing_tds`                   | `INTEGER` | Passing touchdowns                           |                   | `weekly_stats_qb.parquet` |
| `interceptions`                 | `NUMERIC` | Interceptions thrown                         |                   | `weekly_stats_qb.parquet` |
| `sacks`                         | `NUMERIC` | Times sacked                                 |                   | `weekly_stats_qb.parquet` |
| `sack_yards`                    | `NUMERIC` | Yards lost due to sacks                      |                   | `weekly_stats_qb.parquet` |
| `sack_fumbles`                  | `INTEGER` | Fumbles while being sacked                   |                   | `weekly_stats_qb.parquet` |
| `sack_fumbles_lost`             | `INTEGER` | Sack fumbles that were lost                  |                   | `weekly_stats_qb.parquet` |
| `passing_air_yards`             | `NUMERIC` | Total air yards before catch                 |                   | `weekly_stats_qb.parquet` |
| `passing_yards_after_catch`     | `NUMERIC` | Yards after the catch                        |                   | `weekly_stats_qb.parquet` |
| `passing_first_downs`           | `NUMERIC` | Passes resulting in first downs              |                   | `weekly_stats_qb.parquet` |
| `passing_epa`                   | `NUMERIC` | Expected points added from passing plays     |                   | `weekly_stats_qb.parquet` |
| `passing_2pt_conversions`       | `INTEGER` | Successful 2-point conversion passes         |                   | `weekly_stats_qb.parquet` |
| `pacr`                          | `NUMERIC` | Passing Air Conversion Ratio                 |                   | `weekly_stats_qb.parquet` |
| `dakota`                        | `NUMERIC` | Composite QB efficiency metric (EPA + CPOE)  |                   | `weekly_stats_qb.parquet` |
| `carries`                       | `INTEGER` | Rushing attempts                             |                   | `weekly_stats_qb.parquet` |
| `rushing_yards`                 | `NUMERIC` | Rushing yards                                |                   | `weekly_stats_qb.parquet` |
| `rushing_tds`                   | `INTEGER` | Rushing touchdowns                           |                   | `weekly_stats_qb.parquet` |
| `rushing_fumbles`               | `NUMERIC` | Rushing fumbles                              |                   | `weekly_stats_qb.parquet` |
| `rushing_fumbles_lost`          | `NUMERIC` | Rushing fumbles lost                         |                   | `weekly_stats_qb.parquet` |
| `rushing_first_downs`           | `NUMERIC` | Rushes resulting in first downs              |                   | `weekly_stats_qb.parquet` |
| `rushing_epa`                   | `NUMERIC` | Expected points added from rushing           |                   | `weekly_stats_qb.parquet` |
| `rushing_2pt_conversions`       | `INTEGER` | Successful 2-point rushing conversions       |                   | `weekly_stats_qb.parquet` |
| `fantasy_points`                | `NUMERIC` | Standard fantasy points                      |                   | `weekly_stats_qb.parquet` |
| `fantasy_points_ppr`            | `NUMERIC` | Fantasy points with PPR scoring              |                   | `weekly_stats_qb.parquet` |
| `cumulative_completions`        | `NUMERIC` | Season-to-date completions                   |                   | `weekly_stats_qb.parquet` |
| `cumulative_attempts`           | `NUMERIC` | Season-to-date pass attempts                 |                   | `weekly_stats_qb.parquet` |
| `cumulative_passing_yards`      | `NUMERIC` | Season-to-date passing yards                 |                   | `weekly_stats_qb.parquet` |
| `cumulative_passing_tds`        | `NUMERIC` | Season-to-date passing touchdowns            |                   | `weekly_stats_qb.parquet` |
| `cumulative_interceptions`      | `NUMERIC` | Season-to-date interceptions                 |                   | `weekly_stats_qb.parquet` |
| `cumulative_sacks`              | `NUMERIC` | Season-to-date sacks                         |                   | `weekly_stats_qb.parquet` |
| `cumulative_sack_yards`         | `NUMERIC` | Season-to-date sack yards lost               |                   | `weekly_stats_qb.parquet` |
| `cumulative_passing_epa`        | `NUMERIC` | Season-to-date passing EPA                   |                   | `weekly_stats_qb.parquet` |
| `cumulative_rushing_yards`      | `NUMERIC` | Season-to-date rushing yards                 |                   | `weekly_stats_qb.parquet` |
| `cumulative_rushing_tds`        | `NUMERIC` | Season-to-date rushing touchdowns            |                   | `weekly_stats_qb.parquet` |
| `cumulative_rushing_epa`        | `NUMERIC` | Season-to-date rushing EPA                   |                   | `weekly_stats_qb.parquet` |
| `cumulative_fantasy_points`     | `NUMERIC` | Season-to-date fantasy points (standard)     |                   | `weekly_stats_qb.parquet` |
| `cumulative_fantasy_points_ppr` | `NUMERIC` | Season-to-date fantasy points (PPR)          |                   | `weekly_stats_qb.parquet` |

# Weekly RB Stats
| Column                          | Type      | Description                                  | Key               | Source File               |
| ------------------------------- | --------- | -------------------------------------------- | ----------------- | ------------------------- |
| `season`                        | `INTEGER` | NFL season year                              | Primary Key (1/3) | `weekly_stats_qb.parquet` |
| `week`                          | `INTEGER` | Week of the season                           | Primary Key (2/3) | `weekly_stats_qb.parquet` |
| `player_id`                     | `TEXT`    | Unique player identifier                     | Primary Key (3/3) | `weekly_stats_qb.parquet` |
| `season_type`                   | `TEXT`    | Season type (`REG`, `POST`, etc.)            |                   | `weekly_stats_qb.parquet` |
| `position`                      | `TEXT`    | Player position (always `QB` for this table) |                   | `weekly_stats_qb.parquet` |
| `recent_team`                   | `TEXT`    | Team the player played for that week         |                   | `weekly_stats_qb.parquet` |
| `opponent_team`                 | `TEXT`    | Opposing team                                |                   | `weekly_stats_qb.parquet` |
| `completions`                   | `INTEGER` | Completed passes                             |                   | `weekly_stats_qb.parquet` |
| `attempts`                      | `INTEGER` | Pass attempts                                |                   | `weekly_stats_qb.parquet` |
| `passing_yards`                 | `NUMERIC` | Total passing yards                          |                   | `weekly_stats_qb.parquet` |
| `passing_tds`                   | `INTEGER` | Passing touchdowns                           |                   | `weekly_stats_qb.parquet` |
| `interceptions`                 | `NUMERIC` | Interceptions thrown                         |                   | `weekly_stats_qb.parquet` |
| `sacks`                         | `NUMERIC` | Times sacked                                 |                   | `weekly_stats_qb.parquet` |
| `sack_yards`                    | `NUMERIC` | Yards lost due to sacks                      |                   | `weekly_stats_qb.parquet` |
| `sack_fumbles`                  | `INTEGER` | Fumbles while being sacked                   |                   | `weekly_stats_qb.parquet` |
| `sack_fumbles_lost`             | `INTEGER` | Sack fumbles that were lost                  |                   | `weekly_stats_qb.parquet` |
| `passing_air_yards`             | `NUMERIC` | Total air yards before catch                 |                   | `weekly_stats_qb.parquet` |
| `passing_yards_after_catch`     | `NUMERIC` | Yards after the catch                        |                   | `weekly_stats_qb.parquet` |
| `passing_first_downs`           | `NUMERIC` | Passes resulting in first downs              |                   | `weekly_stats_qb.parquet` |
| `passing_epa`                   | `NUMERIC` | Expected points added from passing plays     |                   | `weekly_stats_qb.parquet` |
| `passing_2pt_conversions`       | `INTEGER` | Successful 2-point conversion passes         |                   | `weekly_stats_qb.parquet` |
| `pacr`                          | `NUMERIC` | Passing Air Conversion Ratio                 |                   | `weekly_stats_qb.parquet` |
| `dakota`                        | `NUMERIC` | Composite QB efficiency metric (EPA + CPOE)  |                   | `weekly_stats_qb.parquet` |
| `carries`                       | `INTEGER` | Rushing attempts                             |                   | `weekly_stats_qb.parquet` |
| `rushing_yards`                 | `NUMERIC` | Rushing yards                                |                   | `weekly_stats_qb.parquet` |
| `rushing_tds`                   | `INTEGER` | Rushing touchdowns                           |                   | `weekly_stats_qb.parquet` |
| `rushing_fumbles`               | `NUMERIC` | Rushing fumbles                              |                   | `weekly_stats_qb.parquet` |
| `rushing_fumbles_lost`          | `NUMERIC` | Rushing fumbles lost                         |                   | `weekly_stats_qb.parquet` |
| `rushing_first_downs`           | `NUMERIC` | Rushes resulting in first downs              |                   | `weekly_stats_qb.parquet` |
| `rushing_epa`                   | `NUMERIC` | Expected points added from rushing           |                   | `weekly_stats_qb.parquet` |
| `rushing_2pt_conversions`       | `INTEGER` | Successful 2-point rushing conversions       |                   | `weekly_stats_qb.parquet` |
| `fantasy_points`                | `NUMERIC` | Standard fantasy points                      |                   | `weekly_stats_qb.parquet` |
| `fantasy_points_ppr`            | `NUMERIC` | Fantasy points with PPR scoring              |                   | `weekly_stats_qb.parquet` |
| `cumulative_completions`        | `NUMERIC` | Season-to-date completions                   |                   | `weekly_stats_qb.parquet` |
| `cumulative_attempts`           | `NUMERIC` | Season-to-date pass attempts                 |                   | `weekly_stats_qb.parquet` |
| `cumulative_passing_yards`      | `NUMERIC` | Season-to-date passing yards                 |                   | `weekly_stats_qb.parquet` |
| `cumulative_passing_tds`        | `NUMERIC` | Season-to-date passing touchdowns            |                   | `weekly_stats_qb.parquet` |
| `cumulative_interceptions`      | `NUMERIC` | Season-to-date interceptions                 |                   | `weekly_stats_qb.parquet` |
| `cumulative_sacks`              | `NUMERIC` | Season-to-date sacks                         |                   | `weekly_stats_qb.parquet` |
| `cumulative_sack_yards`         | `NUMERIC` | Season-to-date sack yards lost               |                   | `weekly_stats_qb.parquet` |
| `cumulative_passing_epa`        | `NUMERIC` | Season-to-date passing EPA                   |                   | `weekly_stats_qb.parquet` |
| `cumulative_rushing_yards`      | `NUMERIC` | Season-to-date rushing yards                 |                   | `weekly_stats_qb.parquet` |
| `cumulative_rushing_tds`        | `NUMERIC` | Season-to-date rushing touchdowns            |                   | `weekly_stats_qb.parquet` |
| `cumulative_rushing_epa`        | `NUMERIC` | Season-to-date rushing EPA                   |                   | `weekly_stats_qb.parquet` |
| `cumulative_fantasy_points`     | `NUMERIC` | Season-to-date fantasy points (standard)     |                   | `weekly_stats_qb.parquet` |
| `cumulative_fantasy_points_ppr` | `NUMERIC` | Season-to-date fantasy points (PPR)          |                   | `weekly_stats_qb.parquet` |

# Weekly WR/TE Stats
| Column                          | Type      | Description                              | Key               | Source File(s)                                       |
| ------------------------------- | --------- | ---------------------------------------- | ----------------- | ---------------------------------------------------- |
| `season`                        | `INTEGER` | NFL season year                          | Primary Key (1/4) | `weekly_stats_wr.parquet`, `weekly_stats_te.parquet` |
| `week`                          | `INTEGER` | Week of the season                       | Primary Key (2/4) | both                                                 |
| `player_id`                     | `TEXT`    | Unique player identifier                 | Primary Key (3/4) | both                                                 |
| `position`                      | `TEXT`    | Position (`WR` or `TE`)                  | Primary Key (4/4) | both                                                 |
| `season_type`                   | `TEXT`    | Season type (`REG`, `POST`, etc.)        |                   | both                                                 |
| `recent_team`                   | `TEXT`    | Team the player played for               |                   | both                                                 |
| `opponent_team`                 | `TEXT`    | Opposing team                            |                   | both                                                 |
| `targets`                       | `INTEGER` | Passing targets                          |                   | both                                                 |
| `receptions`                    | `INTEGER` | Receptions                               |                   | both                                                 |
| `receiving_yards`               | `NUMERIC` | Receiving yards                          |                   | both                                                 |
| `receiving_tds`                 | `INTEGER` | Receiving touchdowns                     |                   | both                                                 |
| `receiving_fumbles`             | `NUMERIC` | Fumbles on receptions                    |                   | both                                                 |
| `receiving_fumbles_lost`        | `NUMERIC` | Lost fumbles on receptions               |                   | both                                                 |
| `receiving_air_yards`           | `NUMERIC` | Total air yards on targets               |                   | both                                                 |
| `receiving_yards_after_catch`   | `NUMERIC` | Yards gained after catch                 |                   | both                                                 |
| `receiving_first_downs`         | `NUMERIC` | Receptions resulting in first downs      |                   | both                                                 |
| `receiving_epa`                 | `NUMERIC` | Expected points added from receptions    |                   | both                                                 |
| `receiving_2pt_conversions`     | `INTEGER` | Successful 2-point receptions            |                   | both                                                 |
| `racr`                          | `NUMERIC` | Receiver Air Conversion Ratio            |                   | both                                                 |
| `target_share`                  | `NUMERIC` | Share of team’s targets                  |                   | both                                                 |
| `air_yards_share`               | `NUMERIC` | Share of team’s air yards                |                   | both                                                 |
| `wopr`                          | `NUMERIC` | Weighted Opportunity Rating              |                   | both                                                 |
| `fantasy_points`                | `NUMERIC` | Standard fantasy points                  |                   | both                                                 |
| `fantasy_points_ppr`            | `NUMERIC` | PPR (point-per-reception) fantasy points |                   | both                                                 |
| `cumulative_targets`            | `NUMERIC` | Season-to-date targets                   |                   | both                                                 |
| `cumulative_receptions`         | `NUMERIC` | Season-to-date receptions                |                   | both                                                 |
| `cumulative_receiving_yards`    | `NUMERIC` | Season-to-date receiving yards           |                   | both                                                 |
| `cumulative_receiving_tds`      | `NUMERIC` | Season-to-date receiving touchdowns      |                   | both                                                 |
| `cumulative_receiving_epa`      | `NUMERIC` | Season-to-date receiving EPA             |                   | both                                                 |
| `cumulative_fantasy_points`     | `NUMERIC` | Season-to-date fantasy points            |                   | both                                                 |
| `cumulative_fantasy_points_ppr` | `NUMERIC` | Season-to-date PPR fantasy points        |                   | both                                                 |

# Season QB Stats
| Column                      | Type      | Description                                 | Key               | Source File               |
| --------------------------- | --------- | ------------------------------------------- | ----------------- | ------------------------- |
| `season`                    | `INTEGER` | NFL season year                             | Primary Key (1/2) | `season_stats_qb.parquet` |
| `player_id`                 | `TEXT`    | Unique player identifier                    | Primary Key (2/2) | `season_stats_qb.parquet` |
| `position`                  | `TEXT`    | Player position (`QB`)                      |                   | `season_stats_qb.parquet` |
| `recent_team`               | `TEXT`    | Team most recently associated with          |                   | `season_stats_qb.parquet` |
| `games_played`              | `INTEGER` | Number of games played                      |                   | `season_stats_qb.parquet` |
| `completions`               | `INTEGER` | Completed passes                            |                   | `season_stats_qb.parquet` |
| `attempts`                  | `INTEGER` | Pass attempts                               |                   | `season_stats_qb.parquet` |
| `passing_yards`             | `NUMERIC` | Total passing yards                         |                   | `season_stats_qb.parquet` |
| `passing_tds`               | `INTEGER` | Passing touchdowns                          |                   | `season_stats_qb.parquet` |
| `interceptions`             | `NUMERIC` | Interceptions thrown                        |                   | `season_stats_qb.parquet` |
| `sacks`                     | `NUMERIC` | Times sacked                                |                   | `season_stats_qb.parquet` |
| `sack_yards`                | `NUMERIC` | Yards lost due to sacks                     |                   | `season_stats_qb.parquet` |
| `sack_fumbles`              | `INTEGER` | Fumbles while being sacked                  |                   | `season_stats_qb.parquet` |
| `sack_fumbles_lost`         | `INTEGER` | Sack fumbles that were lost                 |                   | `season_stats_qb.parquet` |
| `passing_air_yards`         | `NUMERIC` | Total air yards before catch                |                   | `season_stats_qb.parquet` |
| `passing_yards_after_catch` | `NUMERIC` | Yards gained after the catch                |                   | `season_stats_qb.parquet` |
| `passing_first_downs`       | `NUMERIC` | Passes resulting in first downs             |                   | `season_stats_qb.parquet` |
| `passing_epa`               | `NUMERIC` | Expected points added on pass plays         |                   | `season_stats_qb.parquet` |
| `passing_2pt_conversions`   | `INTEGER` | Successful 2-point conversion passes        |                   | `season_stats_qb.parquet` |
| `carries`                   | `INTEGER` | Rushing attempts                            |                   | `season_stats_qb.parquet` |
| `rushing_yards`             | `NUMERIC` | Rushing yards                               |                   | `season_stats_qb.parquet` |
| `rushing_tds`               | `INTEGER` | Rushing touchdowns                          |                   | `season_stats_qb.parquet` |
| `rushing_fumbles`           | `NUMERIC` | Rushing fumbles                             |                   | `season_stats_qb.parquet` |
| `rushing_fumbles_lost`      | `NUMERIC` | Rushing fumbles lost                        |                   | `season_stats_qb.parquet` |
| `rushing_first_downs`       | `NUMERIC` | Rushes resulting in first downs             |                   | `season_stats_qb.parquet` |
| `rushing_epa`               | `NUMERIC` | Expected points added from rushing plays    |                   | `season_stats_qb.parquet` |
| `rushing_2pt_conversions`   | `INTEGER` | Successful 2-point rushing conversions      |                   | `season_stats_qb.parquet` |
| `fantasy_points`            | `NUMERIC` | Standard fantasy points                     |                   | `season_stats_qb.parquet` |
| `fantasy_points_ppr`        | `NUMERIC` | PPR (point-per-reception) fantasy points    |                   | `season_stats_qb.parquet` |
| `pacr`                      | `NUMERIC` | Passing Air Conversion Ratio                |                   | `season_stats_qb.parquet` |
| `dakota`                    | `NUMERIC` | Composite QB efficiency metric (EPA + CPOE) |                   | `season_stats_qb.parquet` |

# Season RB Stats
| Column                        | Type      | Description                              | Key               | Source File               |
| ----------------------------- | --------- | ---------------------------------------- | ----------------- | ------------------------- |
| `season`                      | `INTEGER` | NFL season year                          | Primary Key (1/2) | `season_stats_rb.parquet` |
| `player_id`                   | `TEXT`    | Unique player identifier                 | Primary Key (2/2) | `season_stats_rb.parquet` |
| `position`                    | `TEXT`    | Player position (`RB`, `FB`)             |                   | `season_stats_rb.parquet` |
| `recent_team`                 | `TEXT`    | Team most recently associated with       |                   | `season_stats_rb.parquet` |
| `games_played`                | `INTEGER` | Number of games played                   |                   | `season_stats_rb.parquet` |
| `carries`                     | `INTEGER` | Rushing attempts                         |                   | `season_stats_rb.parquet` |
| `rushing_yards`               | `NUMERIC` | Rushing yards                            |                   | `season_stats_rb.parquet` |
| `rushing_tds`                 | `INTEGER` | Rushing touchdowns                       |                   | `season_stats_rb.parquet` |
| `rushing_epa`                 | `NUMERIC` | Expected points added from rushing plays |                   | `season_stats_rb.parquet` |
| `rushing_fumbles`             | `NUMERIC` | Rushing fumbles                          |                   | `season_stats_rb.parquet` |
| `rushing_fumbles_lost`        | `NUMERIC` | Rushing fumbles lost                     |                   | `season_stats_rb.parquet` |
| `rushing_first_downs`         | `NUMERIC` | Rushes resulting in first downs          |                   | `season_stats_rb.parquet` |
| `rushing_2pt_conversions`     | `INTEGER` | Successful 2-point rushing conversions   |                   | `season_stats_rb.parquet` |
| `targets`                     | `INTEGER` | Passing targets                          |                   | `season_stats_rb.parquet` |
| `receptions`                  | `INTEGER` | Receptions                               |                   | `season_stats_rb.parquet` |
| `receiving_yards`             | `NUMERIC` | Receiving yards                          |                   | `season_stats_rb.parquet` |
| `receiving_tds`               | `INTEGER` | Receiving touchdowns                     |                   | `season_stats_rb.parquet` |
| `receiving_epa`               | `NUMERIC` | Expected points added from receptions    |                   | `season_stats_rb.parquet` |
| `receiving_fumbles`           | `NUMERIC` | Fumbles on receptions                    |                   | `season_stats_rb.parquet` |
| `receiving_fumbles_lost`      | `NUMERIC` | Lost fumbles on receptions               |                   | `season_stats_rb.parquet` |
| `receiving_air_yards`         | `NUMERIC` | Total air yards on targets               |                   | `season_stats_rb.parquet` |
| `receiving_yards_after_catch` | `NUMERIC` | Yards gained after catch                 |                   | `season_stats_rb.parquet` |
| `receiving_first_downs`       | `NUMERIC` | Receptions resulting in first downs      |                   | `season_stats_rb.parquet` |
| `receiving_2pt_conversions`   | `INTEGER` | Successful 2-point receptions            |                   | `season_stats_rb.parquet` |
| `fantasy_points`              | `NUMERIC` | Standard fantasy points                  |                   | `season_stats_rb.parquet` |
| `fantasy_points_ppr`          | `NUMERIC` | PPR (point-per-reception) fantasy points |                   | `season_stats_rb.parquet` |
| `racr`                        | `NUMERIC` | Receiver Air Conversion Ratio            |                   | `season_stats_rb.parquet` |
| `target_share`                | `NUMERIC` | Share of team’s targets                  |                   | `season_stats_rb.parquet` |
| `air_yards_share`             | `NUMERIC` | Share of team’s air yards                |                   | `season_stats_rb.parquet` |
| `wopr`                        | `NUMERIC` | Weighted Opportunity Rating              |                   | `season_stats_rb.parquet` |

# Season WR/TE Stats
| Column                        | Type      | Description                              | Key               | Source File(s)                                       |
| ----------------------------- | --------- | ---------------------------------------- | ----------------- | ---------------------------------------------------- |
| `season`                      | `INTEGER` | NFL season year                          | Primary Key (1/2) | `season_stats_wr.parquet`, `season_stats_te.parquet` |
| `player_id`                   | `TEXT`    | Unique player identifier                 | Primary Key (2/2) | both                                                 |
| `position`                    | `TEXT`    | Player position (`WR` or `TE`)           |                   | both                                                 |
| `recent_team`                 | `TEXT`    | Team most recently associated with       |                   | both                                                 |
| `games_played`                | `INTEGER` | Number of games played                   |                   | both                                                 |
| `targets`                     | `INTEGER` | Passing targets                          |                   | both                                                 |
| `receptions`                  | `INTEGER` | Receptions                               |                   | both                                                 |
| `receiving_yards`             | `NUMERIC` | Receiving yards                          |                   | both                                                 |
| `receiving_tds`               | `INTEGER` | Receiving touchdowns                     |                   | both                                                 |
| `receiving_fumbles`           | `NUMERIC` | Fumbles on receptions                    |                   | both                                                 |
| `receiving_fumbles_lost`      | `NUMERIC` | Lost fumbles on receptions               |                   | both                                                 |
| `receiving_air_yards`         | `NUMERIC` | Total air yards on targets               |                   | both                                                 |
| `receiving_yards_after_catch` | `NUMERIC` | Yards gained after catch                 |                   | both                                                 |
| `receiving_first_downs`       | `NUMERIC` | Receptions resulting in first downs      |                   | both                                                 |
| `receiving_epa`               | `NUMERIC` | Expected points added from receptions    |                   | both                                                 |
| `receiving_2pt_conversions`   | `INTEGER` | Successful 2-point receptions            |                   | both                                                 |
| `fantasy_points`              | `NUMERIC` | Standard fantasy points                  |                   | both                                                 |
| `fantasy_points_ppr`          | `NUMERIC` | PPR (point-per-reception) fantasy points |                   | both                                                 |
| `racr`                        | `NUMERIC` | Receiver Air Conversion Ratio            |                   | both                                                 |
| `target_share`                | `NUMERIC` | Share of team’s targets                  |                   | both                                                 |
| `air_yards_share`             | `NUMERIC` | Share of team’s air yards                |                   | both                                                 |
| `wopr`                        | `NUMERIC` | Weighted Opportunity Rating              |                   | both                                                 |

# Career QB Stats
| Column                      | Type      | Description                                 | Key         | Source File               |
| --------------------------- | --------- | ------------------------------------------- | ----------- | ------------------------- |
| `player_id`                 | `TEXT`    | Unique player identifier                    | Primary Key | `career_stats_qb.parquet` |
| `position`                  | `TEXT`    | Player position (`QB`)                      |             | `career_stats_qb.parquet` |
| `recent_team`               | `TEXT`    | Most recent team affiliation                |             | `career_stats_qb.parquet` |
| `seasons_played`            | `INTEGER` | Number of seasons in the league             |             | `career_stats_qb.parquet` |
| `games_played`              | `INTEGER` | Number of games played                      |             | `career_stats_qb.parquet` |
| `completions`               | `INTEGER` | Completed passes                            |             | `career_stats_qb.parquet` |
| `attempts`                  | `INTEGER` | Pass attempts                               |             | `career_stats_qb.parquet` |
| `passing_yards`             | `NUMERIC` | Total passing yards                         |             | `career_stats_qb.parquet` |
| `passing_tds`               | `INTEGER` | Passing touchdowns                          |             | `career_stats_qb.parquet` |
| `interceptions`             | `NUMERIC` | Interceptions thrown                        |             | `career_stats_qb.parquet` |
| `sacks`                     | `NUMERIC` | Times sacked                                |             | `career_stats_qb.parquet` |
| `sack_yards`                | `NUMERIC` | Yards lost due to sacks                     |             | `career_stats_qb.parquet` |
| `sack_fumbles`              | `INTEGER` | Fumbles on sacks                            |             | `career_stats_qb.parquet` |
| `sack_fumbles_lost`         | `INTEGER` | Lost fumbles from sacks                     |             | `career_stats_qb.parquet` |
| `passing_air_yards`         | `NUMERIC` | Total air yards on passes                   |             | `career_stats_qb.parquet` |
| `passing_yards_after_catch` | `NUMERIC` | Yards gained after catch                    |             | `career_stats_qb.parquet` |
| `passing_first_downs`       | `NUMERIC` | Completions resulting in first downs        |             | `career_stats_qb.parquet` |
| `passing_epa`               | `NUMERIC` | Expected points added from passing          |             | `career_stats_qb.parquet` |
| `passing_2pt_conversions`   | `INTEGER` | 2-point conversions via pass                |             | `career_stats_qb.parquet` |
| `carries`                   | `INTEGER` | Rushing attempts                            |             | `career_stats_qb.parquet` |
| `rushing_yards`             | `NUMERIC` | Rushing yards                               |             | `career_stats_qb.parquet` |
| `rushing_tds`               | `INTEGER` | Rushing touchdowns                          |             | `career_stats_qb.parquet` |
| `rushing_fumbles`           | `NUMERIC` | Rushing fumbles                             |             | `career_stats_qb.parquet` |
| `rushing_fumbles_lost`      | `NUMERIC` | Lost rushing fumbles                        |             | `career_stats_qb.parquet` |
| `rushing_first_downs`       | `NUMERIC` | Rushes for first downs                      |             | `career_stats_qb.parquet` |
| `rushing_epa`               | `NUMERIC` | Expected points added from rushing          |             | `career_stats_qb.parquet` |
| `rushing_2pt_conversions`   | `INTEGER` | 2-point conversions via rush                |             | `career_stats_qb.parquet` |
| `fantasy_points`            | `NUMERIC` | Standard fantasy points                     |             | `career_stats_qb.parquet` |
| `fantasy_points_ppr`        | `NUMERIC` | PPR (point-per-reception) fantasy points    |             | `career_stats_qb.parquet` |
| `pacr`                      | `NUMERIC` | Passing Air Conversion Ratio                |             | `career_stats_qb.parquet` |
| `dakota`                    | `NUMERIC` | Composite QB efficiency metric (EPA + CPOE) |             | `career_stats_qb.parquet` |

# Career RB Stats
| Column                        | Type      | Description                                                     | Key         | Source File               |
| ----------------------------- | --------- | --------------------------------------------------------------- | ----------- | ------------------------- |
| `player_id`                   | `TEXT`    | Unique player identifier                                        | Primary Key | `career_stats_rb.parquet` |
| `position`                    | `TEXT`    | Player position (`RB`, `FB`)                                    |             | `career_stats_rb.parquet` |
| `recent_team`                 | `TEXT`    | Most recent team affiliation                                    |             | `career_stats_rb.parquet` |
| `seasons_played`              | `INTEGER` | Number of seasons played                                        |             | `career_stats_rb.parquet` |
| `games_played`                | `INTEGER` | Number of games played                                          |             | `career_stats_rb.parquet` |
| `carries`                     | `INTEGER` | Rushing attempts                                                |             | `career_stats_rb.parquet` |
| `rushing_yards`               | `NUMERIC` | Rushing yards                                                   |             | `career_stats_rb.parquet` |
| `rushing_tds`                 | `INTEGER` | Rushing touchdowns                                              |             | `career_stats_rb.parquet` |
| `rushing_epa`                 | `NUMERIC` | Expected points added from rushing plays                        |             | `career_stats_rb.parquet` |
| `rushing_fumbles`             | `NUMERIC` | Rushing fumbles                                                 |             | `career_stats_rb.parquet` |
| `rushing_fumbles_lost`        | `NUMERIC` | Lost fumbles on rushing plays                                   |             | `career_stats_rb.parquet` |
| `rushing_first_downs`         | `NUMERIC` | Rushes resulting in first downs                                 |             | `career_stats_rb.parquet` |
| `rushing_2pt_conversions`     | `INTEGER` | 2-point conversions via rushing                                 |             | `career_stats_rb.parquet` |
| `targets`                     | `INTEGER` | Passing targets                                                 |             | `career_stats_rb.parquet` |
| `receptions`                  | `INTEGER` | Receptions                                                      |             | `career_stats_rb.parquet` |
| `receiving_yards`             | `NUMERIC` | Receiving yards                                                 |             | `career_stats_rb.parquet` |
| `receiving_tds`               | `INTEGER` | Receiving touchdowns                                            |             | `career_stats_rb.parquet` |
| `receiving_epa`               | `NUMERIC` | Expected points added from receptions                           |             | `career_stats_rb.parquet` |
| `receiving_fumbles`           | `NUMERIC` | Fumbles on receptions                                           |             | `career_stats_rb.parquet` |
| `receiving_fumbles_lost`      | `NUMERIC` | Lost fumbles on receptions                                      |             | `career_stats_rb.parquet` |
| `receiving_air_yards`         | `NUMERIC` | Total air yards on targets                                      |             | `career_stats_rb.parquet` |
| `receiving_yards_after_catch` | `NUMERIC` | Yards gained after catch                                        |             | `career_stats_rb.parquet` |
| `receiving_first_downs`       | `NUMERIC` | Receptions resulting in first downs                             |             | `career_stats_rb.parquet` |
| `receiving_2pt_conversions`   | `INTEGER` | 2-point conversions via reception                               |             | `career_stats_rb.parquet` |
| `fantasy_points`              | `NUMERIC` | Standard fantasy points                                         |             | `career_stats_rb.parquet` |
| `fantasy_points_ppr`          | `NUMERIC` | PPR (point-per-reception) fantasy points                        |             | `career_stats_rb.parquet` |
| `racr`                        | `NUMERIC` | Receiver Air Conversion Ratio                                   |             | `career_stats_rb.parquet` |
| `target_share`                | `NUMERIC` | Share of team’s targets                                         |             | `career_stats_rb.parquet` |
| `air_yards_share`             | `NUMERIC` | Share of team’s air yards                                       |             | `career_stats_rb.parquet` |
| `wopr`                        | `NUMERIC` | Weighted Opportunity Rating (target\_share + air\_yards\_share) |             | `career_stats_rb.parquet` |

# Career WR/TE Stats
| Column                        | Type      | Description                              | Key         | Source File(s)                                       |
| ----------------------------- | --------- | ---------------------------------------- | ----------- | ---------------------------------------------------- |
| `player_id`                   | `TEXT`    | Unique player identifier                 | Primary Key | `career_stats_wr.parquet`, `career_stats_te.parquet` |
| `position`                    | `TEXT`    | Player position (`WR` or `TE`)           |             | both                                                 |
| `recent_team`                 | `TEXT`    | Most recent team affiliation             |             | both                                                 |
| `seasons_played`              | `INTEGER` | Number of seasons played                 |             | both                                                 |
| `games_played`                | `INTEGER` | Number of games played                   |             | both                                                 |
| `targets`                     | `INTEGER` | Passing targets                          |             | both                                                 |
| `receptions`                  | `INTEGER` | Receptions                               |             | both                                                 |
| `receiving_yards`             | `NUMERIC` | Receiving yards                          |             | both                                                 |
| `receiving_tds`               | `INTEGER` | Receiving touchdowns                     |             | both                                                 |
| `receiving_fumbles`           | `NUMERIC` | Fumbles on receptions                    |             | both                                                 |
| `receiving_fumbles_lost`      | `NUMERIC` | Lost fumbles on receptions               |             | both                                                 |
| `receiving_air_yards`         | `NUMERIC` | Total air yards on targets               |             | both                                                 |
| `receiving_yards_after_catch` | `NUMERIC` | Yards gained after catch                 |             | both                                                 |
| `receiving_first_downs`       | `NUMERIC` | Receptions resulting in first downs      |             | both                                                 |
| `receiving_epa`               | `NUMERIC` | Expected points added from receptions    |             | both                                                 |
| `receiving_2pt_conversions`   | `INTEGER` | 2-point conversions via reception        |             | both                                                 |
| `fantasy_points`              | `NUMERIC` | Standard fantasy points                  |             | both                                                 |
| `fantasy_points_ppr`          | `NUMERIC` | PPR (point-per-reception) fantasy points |             | both                                                 |
| `racr`                        | `NUMERIC` | Receiver Air Conversion Ratio            |             | both                                                 |
| `target_share`                | `NUMERIC` | Share of team’s targets                  |             | both                                                 |
| `air_yards_share`             | `NUMERIC` | Share of team’s air yards                |             | both                                                 |
| `wopr`                        | `NUMERIC` | Weighted Opportunity Rating              |             | both                                                 |

# Weekly Defensive Player Stats
| Column                          | Type      | Description                                   | Key               | Source File                       |
| ------------------------------- | --------- | --------------------------------------------- | ----------------- | --------------------------------- |
| `season`                        | `INTEGER` | NFL season year                               | Primary Key (1/4) | `def_player_stats_weekly.parquet` |
| `week`                          | `INTEGER` | Week of the season                            | Primary Key (2/4) | `def_player_stats_weekly.parquet` |
| `season_type`                   | `TEXT`    | Season type: "REG", "POST", etc.              | Primary Key (3/4) | `def_player_stats_weekly.parquet` |
| `player_id`                     | `TEXT`    | Unique player identifier                      | Primary Key (4/4) | `def_player_stats_weekly.parquet` |
| `team`                          | `TEXT`    | Team for which the player played              |                   | `def_player_stats_weekly.parquet` |
| `position`                      | `TEXT`    | Player’s detailed position (e.g., OLB, FS)    |                   | `def_player_stats_weekly.parquet` |
| `position_group`                | `TEXT`    | Generalized position group (DL, LB, DB)       |                   | `def_player_stats_weekly.parquet` |
| `def_tackles`                   | `INTEGER` | Total tackles                                 |                   |                                   |
| `def_tackles_solo`              | `INTEGER` | Solo tackles                                  |                   |                                   |
| `def_tackle_assists`            | `INTEGER` | Assisted tackles                              |                   |                                   |
| `def_tackles_for_loss`          | `INTEGER` | Tackles for loss                              |                   |                                   |
| `def_tackles_for_loss_yards`    | `NUMERIC` | Yards lost due to tackles for loss            |                   |                                   |
| `def_fumbles_forced`            | `INTEGER` | Forced fumbles                                |                   |                                   |
| `def_sacks`                     | `NUMERIC` | Total sacks                                   |                   |                                   |
| `def_sack_yards`                | `NUMERIC` | Yards lost on sacks                           |                   |                                   |
| `def_qb_hits`                   | `NUMERIC` | Hits on opposing quarterbacks                 |                   |                                   |
| `def_interceptions`             | `NUMERIC` | Interceptions made                            |                   |                                   |
| `def_interception_yards`        | `NUMERIC` | Yards gained on interception returns          |                   |                                   |
| `def_pass_defended`             | `NUMERIC` | Passes defended                               |                   |                                   |
| `def_tds`                       | `NUMERIC` | Defensive touchdowns                          |                   |                                   |
| `def_fumbles`                   | `NUMERIC` | Fumbles recovered                             |                   |                                   |
| `def_fumble_recovery_own`       | `NUMERIC` | Fumbles recovered from own team               |                   |                                   |
| `def_fumble_recovery_yards_own` | `NUMERIC` | Return yards after own team’s fumble recovery |                   |                                   |
| `def_fumble_recovery_opp`       | `NUMERIC` | Fumbles recovered from opponent               |                   |                                   |
| `def_fumble_recovery_yards_opp` | `NUMERIC` | Return yards after opponent fumble recovery   |                   |                                   |
| `def_safety`                    | `INTEGER` | Safeties recorded                             |                   |                                   |
| `def_penalty`                   | `NUMERIC` | Number of penalties committed                 |                   |                                   |
| `def_penalty_yards`             | `NUMERIC` | Yards lost to defensive penalties             |                   |                                   |

# Season Defensive Player Stats
| Column                          | Type      | Description                                   | Key               | Source File                       |
| ------------------------------- | --------- | --------------------------------------------- | ----------------- | --------------------------------- |
| `season`                        | `INTEGER` | NFL season year                               | Primary Key (1/2) | `def_player_stats_season.parquet` |
| `player_id`                     | `TEXT`    | Unique player identifier                      | Primary Key (2/2) | `def_player_stats_season.parquet` |
| `team`                          | `TEXT`    | Team for which the player played              |                   | `def_player_stats_season.parquet` |
| `position`                      | `TEXT`    | Player’s detailed position (e.g., OLB, FS)    |                   |                                   |
| `position_group`                | `TEXT`    | Generalized position group (DL, LB, DB)       |                   |                                   |
| `games_played`                  | `INTEGER` | Number of games played                        |                   |                                   |
| `def_tackles`                   | `INTEGER` | Total tackles                                 |                   |                                   |
| `def_tackles_solo`              | `INTEGER` | Solo tackles                                  |                   |                                   |
| `def_tackle_assists`            | `INTEGER` | Assisted tackles                              |                   |                                   |
| `def_tackles_for_loss`          | `INTEGER` | Tackles for loss                              |                   |                                   |
| `def_tackles_for_loss_yards`    | `NUMERIC` | Yards lost due to tackles for loss            |                   |                                   |
| `def_fumbles_forced`            | `INTEGER` | Forced fumbles                                |                   |                                   |
| `def_sacks`                     | `NUMERIC` | Total sacks                                   |                   |                                   |
| `def_sack_yards`                | `NUMERIC` | Yards lost on sacks                           |                   |                                   |
| `def_qb_hits`                   | `NUMERIC` | Hits on opposing quarterbacks                 |                   |                                   |
| `def_interceptions`             | `NUMERIC` | Interceptions made                            |                   |                                   |
| `def_interception_yards`        | `NUMERIC` | Yards gained on interception returns          |                   |                                   |
| `def_pass_defended`             | `NUMERIC` | Passes defended                               |                   |                                   |
| `def_tds`                       | `NUMERIC` | Defensive touchdowns                          |                   |                                   |
| `def_fumbles`                   | `NUMERIC` | Fumbles recovered                             |                   |                                   |
| `def_fumble_recovery_own`       | `NUMERIC` | Fumbles recovered from own team               |                   |                                   |
| `def_fumble_recovery_yards_own` | `NUMERIC` | Return yards after own team’s fumble recovery |                   |                                   |
| `def_fumble_recovery_opp`       | `NUMERIC` | Fumbles recovered from opponent               |                   |                                   |
| `def_fumble_recovery_yards_opp` | `NUMERIC` | Return yards after opponent fumble recovery   |                   |                                   |
| `def_safety`                    | `INTEGER` | Safeties recorded                             |                   |                                   |
| `def_penalty`                   | `NUMERIC` | Number of penalties committed                 |                   |                                   |
| `def_penalty_yards`             | `NUMERIC` | Yards lost to defensive penalties             |                   |                                   |

# Career Defensive Player Stats
| Column                          | Type      | Description                                    | Key         | Source File                       |
| ------------------------------- | --------- | ---------------------------------------------- | ----------- | --------------------------------- |
| `player_id`                     | `TEXT`    | Unique player identifier                       | Primary Key | `def_player_stats_career.parquet` |
| `position`                      | `TEXT`    | Player’s detailed position (e.g., OLB, DE, FS) |             |                                   |
| `position_group`                | `TEXT`    | Generalized position group (DL, LB, DB)        |             |                                   |
| `last_team`                     | `TEXT`    | Most recent team affiliation                   |             |                                   |
| `seasons_played`                | `INTEGER` | Number of seasons played                       |             |                                   |
| `games_played`                  | `INTEGER` | Number of games played                         |             |                                   |
| `def_tackles`                   | `INTEGER` | Total tackles                                  |             |                                   |
| `def_tackles_solo`              | `INTEGER` | Solo tackles                                   |             |                                   |
| `def_tackle_assists`            | `INTEGER` | Assisted tackles                               |             |                                   |
| `def_tackles_for_loss`          | `INTEGER` | Tackles for loss                               |             |                                   |
| `def_tackles_for_loss_yards`    | `NUMERIC` | Yards lost due to tackles for loss             |             |                                   |
| `def_fumbles_forced`            | `INTEGER` | Forced fumbles                                 |             |                                   |
| `def_sacks`                     | `NUMERIC` | Total sacks                                    |             |                                   |
| `def_sack_yards`                | `NUMERIC` | Yards lost on sacks                            |             |                                   |
| `def_qb_hits`                   | `NUMERIC` | Hits on opposing quarterbacks                  |             |                                   |
| `def_interceptions`             | `NUMERIC` | Interceptions made                             |             |                                   |
| `def_interception_yards`        | `NUMERIC` | Yards gained on interception returns           |             |                                   |
| `def_pass_defended`             | `NUMERIC` | Passes defended                                |             |                                   |
| `def_tds`                       | `NUMERIC` | Defensive touchdowns                           |             |                                   |
| `def_fumbles`                   | `NUMERIC` | Fumbles recovered                              |             |                                   |
| `def_fumble_recovery_own`       | `NUMERIC` | Own-team fumbles recovered                     |             |                                   |
| `def_fumble_recovery_yards_own` | `NUMERIC` | Return yards after own-team recovery           |             |                                   |
| `def_fumble_recovery_opp`       | `NUMERIC` | Opponent fumbles recovered                     |             |                                   |
| `def_fumble_recovery_yards_opp` | `NUMERIC` | Return yards after opponent recovery           |             |                                   |
| `def_safety`                    | `INTEGER` | Safeties recorded                              |             |                                   |
| `def_penalty`                   | `NUMERIC` | Defensive penalties committed                  |             |                                   |
| `def_penalty_yards`             | `NUMERIC` | Yards penalized                                |             |                                   |

# Weekly Team Defensive Stats

# Season Team Defensive Stats
| Column                          | Type      | Description                                              | Key               | Source File                     |
| ------------------------------- | --------- | -------------------------------------------------------- | ----------------- | ------------------------------- |
| `season`                        | `INTEGER` | NFL season year                                          | Primary Key (1/2) | `def_team_stats_season.parquet` |
| `team`                          | `TEXT`    | Team abbreviation (e.g., `BUF`, `NE`)                    | Primary Key (2/2) | `def_team_stats_season.parquet` |
| `n_players`                     | `INTEGER` | Number of unique players contributing to defensive stats |                   |                                 |
| `def_tackles`                   | `INTEGER` | Total tackles                                            |                   |                                 |
| `def_tackles_solo`              | `INTEGER` | Solo tackles                                             |                   |                                 |
| `def_tackle_assists`            | `INTEGER` | Assisted tackles                                         |                   |                                 |
| `def_tackles_for_loss`          | `INTEGER` | Tackles behind the line of scrimmage                     |                   |                                 |
| `def_tackles_for_loss_yards`    | `NUMERIC` | Total yards lost on tackles for loss                     |                   |                                 |
| `def_fumbles_forced`            | `INTEGER` | Fumbles forced by the defense                            |                   |                                 |
| `def_sacks`                     | `NUMERIC` | Total quarterback sacks                                  |                   |                                 |
| `def_sack_yards`                | `NUMERIC` | Yards lost on sacks                                      |                   |                                 |
| `def_qb_hits`                   | `NUMERIC` | Hits on opposing quarterbacks                            |                   |                                 |
| `def_interceptions`             | `NUMERIC` | Total interceptions                                      |                   |                                 |
| `def_interception_yards`        | `NUMERIC` | Yards returned on interceptions                          |                   |                                 |
| `def_pass_defended`             | `NUMERIC` | Pass breakups / passes defended                          |                   |                                 |
| `def_tds`                       | `NUMERIC` | Defensive touchdowns (pick-6, fumble return, etc.)       |                   |                                 |
| `def_fumbles`                   | `NUMERIC` | Total fumble recoveries                                  |                   |                                 |
| `def_fumble_recovery_own`       | `NUMERIC` | Own-team fumbles recovered                               |                   |                                 |
| `def_fumble_recovery_yards_own` | `NUMERIC` | Return yards after own-team fumble recovery              |                   |                                 |
| `def_fumble_recovery_opp`       | `NUMERIC` | Opponent fumbles recovered                               |                   |                                 |
| `def_fumble_recovery_yards_opp` | `NUMERIC` | Return yards after recovering opponent fumbles           |                   |                                 |
| `def_safety`                    | `INTEGER` | Safeties recorded                                        |                   |                                 |
| `def_penalty`                   | `NUMERIC` | Number of defensive penalties committed                  |                   |                                 |
| `def_penalty_yards`             | `NUMERIC` | Yards penalized on defense                               |                   |                                 |

# Weekly NextGen Stats
| Column                                    | Type      | Description                                         | Key               | Source File                           |
| ----------------------------------------- | --------- | --------------------------------------------------- | ----------------- | ------------------------------------- |
| `season`                                  | `INTEGER` | NFL season                                          | Primary Key (1/4) | `nextgen_stats_player_weekly.parquet` |
| `season_type`                             | `TEXT`    | Regular (`REG`) or Postseason (`POST`)              | Primary Key (2/4) |                                       |
| `week`                                    | `INTEGER` | Week number within the season                       | Primary Key (3/4) |                                       |
| `player_gsis_id`                          | `TEXT`    | Player GSIS (Game Stats and Information System) ID  | Primary Key (4/4) |                                       |
| `team_abbr`                               | `TEXT`    | Team abbreviation                                   |                   |                                       |
| `player_position`                         | `TEXT`    | Position played (typically `QB`)                    |                   |                                       |
| `avg_time_to_throw`                       | `NUMERIC` | Time from snap to throw (seconds)                   |                   |                                       |
| `avg_completed_air_yards`                 | `NUMERIC` | Average air yards on completed passes               |                   |                                       |
| `avg_intended_air_yards`                  | `NUMERIC` | Average air yards intended per pass                 |                   |                                       |
| `avg_air_yards_differential`              | `NUMERIC` | Difference between intended and completed air yards |                   |                                       |
| `aggressiveness`                          | `NUMERIC` | % of passes thrown into tight coverage              |                   |                                       |
| `max_completed_air_distance`              | `NUMERIC` | Longest completed air yard distance                 |                   |                                       |
| `avg_air_yards_to_sticks`                 | `NUMERIC` | Average air yards relative to first down marker     |                   |                                       |
| `attempts`                                | `INTEGER` | Total pass attempts                                 |                   |                                       |
| `completions`                             | `INTEGER` | Total pass completions                              |                   |                                       |
| `pass_yards`                              | `INTEGER` | Total passing yards                                 |                   |                                       |
| `pass_touchdowns`                         | `INTEGER` | Passing touchdowns                                  |                   |                                       |
| `interceptions`                           | `INTEGER` | Interceptions thrown                                |                   |                                       |
| `passer_rating`                           | `NUMERIC` | NFL passer rating                                   |                   |                                       |
| `completion_percentage`                   | `NUMERIC` | Actual completion rate                              |                   |                                       |
| `expected_completion_percentage`          | `NUMERIC` | Modeled expectation of completion rate              |                   |                                       |
| `completion_percentage_above_expectation` | `NUMERIC` | Difference between actual and expected completion % |                   |                                       |
| `avg_air_distance`                        | `NUMERIC` | Average air yards per attempt                       |                   |                                       |
| `max_air_distance`                        | `NUMERIC` | Max air yards on any pass                           |                   |                                       |

# Season NextGen Stats
| Column                                    | Type      | Description                                               | Key               | Source File                           |
| ----------------------------------------- | --------- | --------------------------------------------------------- | ----------------- | ------------------------------------- |
| `season`                                  | `INTEGER` | NFL season                                                | Primary Key (1/2) | `nextgen_stats_player_season.parquet` |
| `player_gsis_id`                          | `TEXT`    | Player GSIS (Game Stats and Information System) ID        | Primary Key (2/2) |                                       |
| `team_abbr`                               | `TEXT`    | Team abbreviation                                         |                   |                                       |
| `player_position`                         | `TEXT`    | Position played (typically `QB`)                          |                   |                                       |
| `games_played`                            | `INTEGER` | Games played that season                                  |                   |                                       |
| `attempts`                                | `INTEGER` | Total pass attempts                                       |                   |                                       |
| `completions`                             | `INTEGER` | Total completions                                         |                   |                                       |
| `pass_yards`                              | `INTEGER` | Total passing yards                                       |                   |                                       |
| `pass_touchdowns`                         | `INTEGER` | Passing touchdowns                                        |                   |                                       |
| `interceptions`                           | `INTEGER` | Interceptions thrown                                      |                   |                                       |
| `avg_attempts`                            | `NUMERIC` | Average attempts per game                                 |                   |                                       |
| `avg_completions`                         | `NUMERIC` | Average completions per game                              |                   |                                       |
| `avg_pass_yards`                          | `NUMERIC` | Average passing yards per game                            |                   |                                       |
| `avg_pass_touchdowns`                     | `NUMERIC` | Average passing TDs per game                              |                   |                                       |
| `avg_interceptions`                       | `NUMERIC` | Average interceptions per game                            |                   |                                       |
| `avg_time_to_throw`                       | `NUMERIC` | Average time from snap to throw (seconds)                 |                   |                                       |
| `avg_completed_air_yards`                 | `NUMERIC` | Average air yards on completed passes                     |                   |                                       |
| `avg_intended_air_yards`                  | `NUMERIC` | Average air yards on all pass attempts                    |                   |                                       |
| `avg_air_yards_differential`              | `NUMERIC` | Difference between intended and completed air yards       |                   |                                       |
| `aggressiveness`                          | `NUMERIC` | % of attempts thrown into tight coverage                  |                   |                                       |
| `max_completed_air_distance`              | `NUMERIC` | Longest completed pass by air yards                       |                   |                                       |
| `avg_air_yards_to_sticks`                 | `NUMERIC` | Avg distance relative to 1st down marker on pass attempts |                   |                                       |
| `completion_percentage`                   | `NUMERIC` | Actual completion rate                                    |                   |                                       |
| `expected_completion_percentage`          | `NUMERIC` | Modeled expected completion rate                          |                   |                                       |
| `completion_percentage_above_expectation` | `NUMERIC` | Difference between actual and expected completion %       |                   |                                       |
| `avg_air_distance`                        | `NUMERIC` | Average air yards per attempt                             |                   |                                       |
| `max_air_distance`                        | `NUMERIC` | Max air yards recorded on any throw                       |                   |                                       |
| `passer_rating`                           | `NUMERIC` | NFL passer rating                                         |                   |                                       |

# Postseason NextGen Stats
| Column                                    | Type      | Description                                         | Key         | Source File                               |
| ----------------------------------------- | --------- | --------------------------------------------------- | ----------- | ----------------------------------------- |
| `player_gsis_id`                          | `TEXT`    | Player GSIS (Game Stats and Information System) ID  | Primary Key | `nextgen_stats_player_postseason.parquet` |
| `team_abbr`                               | `TEXT`    | Team abbreviation (e.g., `PHI`, `KC`)               |             |                                           |
| `player_position`                         | `TEXT`    | Position played (typically `QB`)                    |             |                                           |
| `games_played`                            | `INTEGER` | Number of postseason games played                   |             |                                           |
| `attempts`                                | `INTEGER` | Total postseason pass attempts                      |             |                                           |
| `completions`                             | `INTEGER` | Total postseason pass completions                   |             |                                           |
| `pass_yards`                              | `INTEGER` | Total postseason passing yards                      |             |                                           |
| `pass_touchdowns`                         | `INTEGER` | Total postseason passing touchdowns                 |             |                                           |
| `interceptions`                           | `INTEGER` | Total postseason interceptions thrown               |             |                                           |
| `avg_attempts`                            | `NUMERIC` | Avg pass attempts per game                          |             |                                           |
| `avg_completions`                         | `NUMERIC` | Avg completions per game                            |             |                                           |
| `avg_pass_yards`                          | `NUMERIC` | Avg passing yards per game                          |             |                                           |
| `avg_pass_touchdowns`                     | `NUMERIC` | Avg passing TDs per game                            |             |                                           |
| `avg_interceptions`                       | `NUMERIC` | Avg interceptions per game                          |             |                                           |
| `avg_time_to_throw`                       | `NUMERIC` | Time from snap to throw (seconds)                   |             |                                           |
| `avg_completed_air_yards`                 | `NUMERIC` | Average air yards on completed passes               |             |                                           |
| `avg_intended_air_yards`                  | `NUMERIC` | Average air yards on all pass attempts              |             |                                           |
| `avg_air_yards_differential`              | `NUMERIC` | Difference between intended and completed air yards |             |                                           |
| `aggressiveness`                          | `NUMERIC` | % of attempts into tight coverage                   |             |                                           |
| `max_completed_air_distance`              | `NUMERIC` | Longest completed pass by air yards                 |             |                                           |
| `avg_air_yards_to_sticks`                 | `NUMERIC` | Avg air yards relative to first down marker         |             |                                           |
| `completion_percentage`                   | `NUMERIC` | Actual completion %                                 |             |                                           |
| `expected_completion_percentage`          | `NUMERIC` | Modeled expected completion %                       |             |                                           |
| `completion_percentage_above_expectation` | `NUMERIC` | Actual minus expected completion %                  |             |                                           |
| `avg_air_distance`                        | `NUMERIC` | Average air yards per attempt                       |             |                                           |
| `max_air_distance`                        | `NUMERIC` | Max air yards recorded on any throw                 |             |                                           |
| `passer_rating`                           | `NUMERIC` | Traditional NFL passer rating                       |             |                                           |

# Career NextGen Stats
| Column                                    | Type      | Description                                             | Key         | Source File                           |
| ----------------------------------------- | --------- | ------------------------------------------------------- | ----------- | ------------------------------------- |
| `player_gsis_id`                          | `TEXT`    | Player GSIS (Game Stats and Information System) ID      | Primary Key | `nextgen_stats_player_career.parquet` |
| `team_abbr`                               | `TEXT`    | Most recent team affiliation                            |             |                                       |
| `player_position`                         | `TEXT`    | Player's position (typically `QB`)                      |             |                                       |
| `games_played`                            | `INTEGER` | Total number of games played in career                  |             |                                       |
| `attempts`                                | `INTEGER` | Total career pass attempts                              |             |                                       |
| `completions`                             | `INTEGER` | Total career pass completions                           |             |                                       |
| `pass_yards`                              | `INTEGER` | Total career passing yards                              |             |                                       |
| `pass_touchdowns`                         | `INTEGER` | Total career passing touchdowns                         |             |                                       |
| `interceptions`                           | `INTEGER` | Total career interceptions thrown                       |             |                                       |
| `avg_attempts`                            | `NUMERIC` | Avg pass attempts per game over career                  |             |                                       |
| `avg_completions`                         | `NUMERIC` | Avg completions per game over career                    |             |                                       |
| `avg_pass_yards`                          | `NUMERIC` | Avg passing yards per game over career                  |             |                                       |
| `avg_pass_touchdowns`                     | `NUMERIC` | Avg passing TDs per game over career                    |             |                                       |
| `avg_interceptions`                       | `NUMERIC` | Avg interceptions per game over career                  |             |                                       |
| `avg_time_to_throw`                       | `NUMERIC` | Avg time from snap to throw across career (in seconds)  |             |                                       |
| `avg_completed_air_yards`                 | `NUMERIC` | Avg air yards on completed passes across career         |             |                                       |
| `avg_intended_air_yards`                  | `NUMERIC` | Avg air yards on all attempts across career             |             |                                       |
| `avg_air_yards_differential`              | `NUMERIC` | Difference between intended and completed air yards     |             |                                       |
| `aggressiveness`                          | `NUMERIC` | % of attempts into tight coverage across career         |             |                                       |
| `max_completed_air_distance`              | `NUMERIC` | Longest completed pass by air yards in career           |             |                                       |
| `avg_air_yards_to_sticks`                 | `NUMERIC` | Avg air yards relative to 1st down marker across career |             |                                       |
| `completion_percentage`                   | `NUMERIC` | Actual completion % across career                       |             |                                       |
| `expected_completion_percentage`          | `NUMERIC` | Modeled expected completion % over career               |             |                                       |
| `completion_percentage_above_expectation` | `NUMERIC` | Actual minus expected completion % across career        |             |                                       |
| `avg_air_distance`                        | `NUMERIC` | Avg air distance per attempt across career              |             |                                       |
| `max_air_distance`                        | `NUMERIC` | Longest air distance on any attempt in career           |             |                                       |
| `passer_rating`                           | `NUMERIC` | Career passer rating                                    |             |                                       |

# QB Contracts
| Column                | Type      | Description                                                         | Key         | Source File            |
| --------------------- | --------- | ------------------------------------------------------------------- | ----------- | ---------------------- |
| `contract_id`         | `INTEGER` | Unique contract ID per quarterback contract                         | Primary Key | `contracts_qb.parquet` |
| `gsis_id`             | `TEXT`    | Player GSIS (Game Stats and Information System) ID                  |             |                        |
| `player`              | `TEXT`    | Player name                                                         |             |                        |
| `position`            | `TEXT`    | Player position (always `QB`)                                       |             |                        |
| `team`                | `TEXT`    | Team under which contract was signed                                |             |                        |
| `is_active`           | `BOOLEAN` | Whether the contract is currently active                            |             |                        |
| `year_signed`         | `INTEGER` | Year the contract was signed                                        |             |                        |
| `years`               | `INTEGER` | Contract duration in years                                          |             |                        |
| `value`               | `NUMERIC` | Total contract value in millions USD                                |             |                        |
| `apy`                 | `NUMERIC` | Average per year (APY) salary in millions USD                       |             |                        |
| `guaranteed`          | `NUMERIC` | Guaranteed money in the contract in millions USD                    |             |                        |
| `apy_cap_pct`         | `NUMERIC` | Percentage of league salary cap that APY represents in signing year |             |                        |
| `inflated_value`      | `NUMERIC` | Inflation-adjusted contract value in millions USD                   |             |                        |
| `inflated_apy`        | `NUMERIC` | Inflation-adjusted APY in millions USD                              |             |                        |
| `inflated_guaranteed` | `NUMERIC` | Inflation-adjusted guaranteed amount in millions USD                |             |                        |
| `contract_start`      | `INTEGER` | Starting season of the contract                                     |             |                        |
| `contract_end`        | `INTEGER` | Ending season of the contract                                       |             |                        |
| `teams_played_for`    | `INTEGER` | Total number of teams played for over career                        |             |                        |
| `player_page`         | `TEXT`    | URL to player’s OverTheCap contract profile                         |             |                        |
| `otc_id`              | `TEXT`    | OverTheCap internal player ID                                       |             |                        |
| `date_of_birth`       | `TEXT`    | Player date of birth                                                |             |                        |
| `height`              | `TEXT`    | Player height (e.g., `6'3"`)                                        |             |                        |
| `weight`              | `NUMERIC` | Player weight in pounds                                             |             |                        |
| `college`             | `TEXT`    | College attended                                                    |             |                        |
| `draft_year`          | `INTEGER` | Year player was drafted                                             |             |                        |
| `draft_round`         | `INTEGER` | Draft round                                                         |             |                        |
| `draft_overall`       | `INTEGER` | Overall draft pick number                                           |             |                        |
| `draft_team`          | `TEXT`    | Team that drafted the player                                        |             |                        |

# Contract Cap Percentage
| Column            | Type      | Description                                                                         | Key               | Source File                          |
| ----------------- | --------- | ----------------------------------------------------------------------------------- | ----------------- | ------------------------------------ |
| `position`        | `TEXT`    | Player position (e.g., `QB`, `WR`, `CB`)                                            | Primary Key (1/3) | `contracts_position_cap_pct.parquet` |
| `year_signed`     | `INTEGER` | Year the contract was signed                                                        | Primary Key (2/3) |                                      |
| `team`            | `TEXT`    | Team abbreviation                                                                   | Primary Key (3/3) |                                      |
| `avg_apy_cap_pct` | `NUMERIC` | Average % of cap spent per contract at this position for this team-year             |                   |                                      |
| `total_apy`       | `NUMERIC` | Sum of all contract APYs signed for this team and position that year (millions USD) |                   |                                      |
| `count`           | `INTEGER` | Number of contracts signed at this position by this team in the given year          |                   |                                      |

# Weekly Injuries
| Column             | Type      | Description                                                    | Key               | Source File               |
| ------------------ | --------- | -------------------------------------------------------------- | ----------------- | ------------------------- |
| `season`           | `INTEGER` | NFL season                                                     | Primary Key (1/4) | `injuries_weekly.parquet` |
| `week`             | `INTEGER` | Week of the season (1–21)                                      | Primary Key (2/4) |                           |
| `team`             | `TEXT`    | Team abbreviation (e.g., `KC`, `BUF`)                          | Primary Key (3/4) |                           |
| `gsis_id`          | `TEXT`    | Player GSIS ID                                                 | Primary Key (4/4) |                           |
| `full_name`        | `TEXT`    | Player full name                                               |                   |                           |
| `position`         | `TEXT`    | Player position (e.g., `QB`, `CB`, `TE`)                       |                   |                           |
| `report_status`    | `TEXT`    | Status on official injury report (`Out`, `Questionable`, etc.) |                   |                           |
| `injury_reported`  | `BOOLEAN` | Whether any injury was reported (`TRUE` or `FALSE`)            |                   |                           |
| `did_not_practice` | `BOOLEAN` | If player missed practice that week                            |                   |                           |
| `injury_status`    | `TEXT`    | Overall game-day injury designation (e.g., `Out`, `Probable`)  |                   |                           |
| `practice_status`  | `TEXT`    | Practice participation status (`Limited`, `Full`, `DNP`, etc.) |                   |                           |
| `primary_injury`   | `TEXT`    | Description of primary injury (e.g., `Hamstring`, `Ankle`)     |                   |                           |
| `secondary_injury` | `BOOLEAN` | Whether a secondary injury was reported                        |                   |                           |

# Weekly Team Injuries
| Column                | Type      | Description                                                             | Key               | Source File                    |
| --------------------- | --------- | ----------------------------------------------------------------------- | ----------------- | ------------------------------ |
| `season`              | `INTEGER` | NFL season                                                              | Primary Key (1/3) | `injuries_team_weekly.parquet` |
| `week`                | `INTEGER` | Week of the season (1–21)                                               | Primary Key (2/3) |                                |
| `team`                | `TEXT`    | Team abbreviation (e.g., `KC`, `CHI`)                                   | Primary Key (3/3) |                                |
| `weekly_injuries`     | `INTEGER` | Number of new injuries reported this week                               |                   |                                |
| `cumulative_injuries` | `INTEGER` | Total injuries reported by this team up to and including the given week |                   |                                |

# Season Team Injuries
| Column            | Type      | Description                                             | Key               | Source File                    |
| ----------------- | --------- | ------------------------------------------------------- | ----------------- | ------------------------------ |
| `season`          | `INTEGER` | NFL season                                              | Primary Key (1/2) | `injuries_team_season.parquet` |
| `team`            | `TEXT`    | Team abbreviation (e.g., `LAR`, `PIT`)                  | Primary Key (2/2) |                                |
| `season_injuries` | `INTEGER` | Total number of reported player injuries for the season |                   |                                |

# Weekly Team Position Injuries
| Column                         | Type      | Description                                                         | Key               | Source File                        |
| ------------------------------ | --------- | ------------------------------------------------------------------- | ----------------- | ---------------------------------- |
| `season`                       | `INTEGER` | NFL season                                                          | Primary Key (1/4) | `injuries_position_weekly.parquet` |
| `week`                         | `INTEGER` | Week of the season (1–21)                                           | Primary Key (2/4) |                                    |
| `team`                         | `TEXT`    | Team abbreviation (e.g., `NYG`, `TEN`)                              | Primary Key (3/4) |                                    |
| `position`                     | `TEXT`    | Position group (e.g., `OL`, `WR`, `LB`)                             | Primary Key (4/4) |                                    |
| `position_injuries`            | `INTEGER` | Number of injuries reported at this position in this week           |                   |                                    |
| `cumulative_position_injuries` | `INTEGER` | Cumulative count of injuries to this position group up to this week |                   |                                    |

# PBP Offense Participation
| Column              | Type      | Description                                       | Key               | Source File                         |
| ------------------- | --------- | ------------------------------------------------- | ----------------- | ----------------------------------- |
| `game_id`           | `TEXT`    | Unique game identifier (e.g., `2016_01_BUF_BAL`)  | Primary Key (1/2) | `participation_offense_pbp.parquet` |
| `play_id`           | `INTEGER` | Unique play identifier within game                | Primary Key (2/2) |                                     |
| `season`            | `INTEGER` | NFL season                                        |                   |                                     |
| `week`              | `INTEGER` | NFL week                                          |                   |                                     |
| `team`              | `TEXT`    | Offensive team abbreviation                       |                   |                                     |
| `play_type`         | `TEXT`    | Type of play (`pass`, `run`, etc.)                |                   |                                     |
| `offense_formation` | `TEXT`    | Named formation (e.g., `SHOTGUN`, `I_FORM`)       |                   |                                     |
| `offense_personnel` | `TEXT`    | Personnel group string (e.g., `2 RB, 1 TE, 2 WR`) |                   |                                     |
| `n_offense`         | `INTEGER` | Number of offensive players recorded              |                   |                                     |
| `ngs_air_yards`     | `NUMERIC` | Air yards (Next Gen Stats)                        |                   |                                     |
| `time_to_throw`     | `NUMERIC` | Time in seconds from snap to throw                |                   |                                     |
| `was_pressure`      | `BOOLEAN` | Whether the play involved QB pressure             |                   |                                     |
| `route`             | `TEXT`    | Receiver route run on this play (if applicable)   |                   |                                     |
| `pressures_allowed` | `INTEGER` | Count of pressures allowed                        |                   |                                     |

# Game Offense Participation
| Column                        | Type      | Description                                             | Key               | Source File                          |
| -----------------------------| --------- | ------------------------------------------------------- | ----------------- | ------------------------------------ |
| `game_id`                    | `TEXT`    | Unique game identifier (e.g., `2016_01_NE_ARI`)         | Primary Key (1/2) | `participation_offense_game.parquet` |
| `team`                       | `TEXT`    | Team abbreviation (e.g., `ARI`)                         | Primary Key (2/2) |                                      |
| `season`                     | `INTEGER` | NFL season                                              |                   |                                      |
| `week`                       | `INTEGER` | NFL week                                                |                   |                                      |
| `n_plays`                    | `INTEGER` | Total offensive plays                                   |                   |                                      |
| `n_pass`                     | `INTEGER` | Number of passing plays                                 |                   |                                      |
| `n_run`                      | `INTEGER` | Number of running plays                                 |                   |                                      |
| `n_empty`                    | `INTEGER` | Plays using `EMPTY` formation                           |                   |                                      |
| `n_i_form`                   | `INTEGER` | Plays using `I_FORM` formation                          |                   |                                      |
| `n_jumbo`                    | `INTEGER` | Plays using `JUMBO` formation                           |                   |                                      |
| `n_pistol`                   | `INTEGER` | Plays using `PISTOL` formation                          |                   |                                      |
| `n_shotgun`                  | `INTEGER` | Plays using `SHOTGUN` formation                         |                   |                                      |
| `n_singleback`               | `INTEGER` | Plays using `SINGLEBACK` formation                      |                   |                                      |
| `n_wildcat`                  | `INTEGER` | Plays using `WILDCAT` formation                         |                   |                                      |
| `n_other_formations`         | `INTEGER` | Plays using other or unclassified formations            |                   |                                      |
| `n_angle`                    | `INTEGER` | Passing plays using `ANGLE` route                       |                   |                                      |
| `n_corner`                   | `INTEGER` | Passing plays using `CORNER` route                      |                   |                                      |
| `n_cross`                    | `INTEGER` | Passing plays using `CROSS` route                       |                   |                                      |
| `n_flat`                     | `INTEGER` | Passing plays using `FLAT` route                        |                   |                                      |
| `n_go`                       | `INTEGER` | Passing plays using `GO` route                          |                   |                                      |
| `n_hitch`                    | `INTEGER` | Passing plays using `HITCH` route                       |                   |                                      |
| `n_in`                       | `INTEGER` | Passing plays using `IN` route                          |                   |                                      |
| `n_out`                      | `INTEGER` | Passing plays using `OUT` route                         |                   |                                      |
| `n_post`                     | `INTEGER` | Passing plays using `POST` route                        |                   |                                      |
| `n_screen`                   | `INTEGER` | Passing plays using `SCREEN` route                      |                   |                                      |
| `n_slant`                    | `INTEGER` | Passing plays using `SLANT` route                       |                   |                                      |
| `n_wheel`                    | `INTEGER` | Passing plays using `WHEEL` route                       |                   |                                      |
| `n_other_routes`            | `INTEGER` | Passing plays using other or unclassified routes        |                   |                                      |
| `avg_time_to_throw`         | `NUMERIC` | Average time (seconds) from snap to throw               |                   |                                      |
| `pressures_allowed`         | `INTEGER` | QB pressures allowed on offensive plays                 |                   |                                      |
| `cumulative_plays`          | `INTEGER` | Cumulative offensive plays through current week         |                   |                                      |
| `cumulative_pass`           | `INTEGER` | Cumulative passing plays through current week           |                   |                                      |
| `cumulative_run`            | `INTEGER` | Cumulative running plays through current week           |                   |                                      |
| `cumulative_pressures_allowed` | `INTEGER` | Cumulative QB pressures allowed through current week    |                   |                                      |
| `cumulative_screen`         | `INTEGER` | Cumulative screen routes used through current week      |                   |                                      |
| `cumulative_flat`           | `INTEGER` | Cumulative flat routes used through current week        |                   |                                      |
| `cumulative_other_routes`   | `INTEGER` | Cumulative other/unclassified routes through current week |                 |                                      |
| `avg_time_to_throw_to_date` | `NUMERIC` | Average time to throw across season to date             |                   |                                      |

# Season Offense Participation
| Column               | Type      | Description                                            | Key               | Source File                            |
| -------------------- | --------- | ------------------------------------------------------ | ----------------- | -------------------------------------- |
| `season`             | `INTEGER` | NFL season                                             | Primary Key (1/2) | `participation_offense_season.parquet` |
| `team`               | `TEXT`    | Team abbreviation (e.g., `ARI`)                        | Primary Key (2/2) |                                        |
| `n_plays`            | `INTEGER` | Total offensive plays                                  |                   |                                        |
| `n_pass`             | `INTEGER` | Total passing plays                                    |                   |                                        |
| `n_run`              | `INTEGER` | Total running plays                                    |                   |                                        |
| `n_empty`            | `INTEGER` | Plays using `EMPTY` formation                          |                   |                                        |
| `n_i_form`           | `INTEGER` | Plays using `I_FORM` formation                         |                   |                                        |
| `n_jumbo`            | `INTEGER` | Plays using `JUMBO` formation                          |                   |                                        |
| `n_pistol`           | `INTEGER` | Plays using `PISTOL` formation                         |                   |                                        |
| `n_shotgun`          | `INTEGER` | Plays using `SHOTGUN` formation                        |                   |                                        |
| `n_singleback`       | `INTEGER` | Plays using `SINGLEBACK` formation                     |                   |                                        |
| `n_wildcat`          | `INTEGER` | Plays using `WILDCAT` formation                        |                   |                                        |
| `n_other_formations` | `INTEGER` | Plays using other or unclassified formations           |                   |                                        |
| `n_angle`            | `INTEGER` | Passing plays using `ANGLE` route                      |                   |                                        |
| `n_corner`           | `INTEGER` | Passing plays using `CORNER` route                     |                   |                                        |
| `n_cross`            | `INTEGER` | Passing plays using `CROSS` route                      |                   |                                        |
| `n_flat`             | `INTEGER` | Passing plays using `FLAT` route                       |                   |                                        |
| `n_go`               | `INTEGER` | Passing plays using `GO` route                         |                   |                                        |
| `n_hitch`            | `INTEGER` | Passing plays using `HITCH` route                      |                   |                                        |
| `n_in`               | `INTEGER` | Passing plays using `IN` route                         |                   |                                        |
| `n_out`              | `INTEGER` | Passing plays using `OUT` route                        |                   |                                        |
| `n_post`             | `INTEGER` | Passing plays using `POST` route                       |                   |                                        |
| `n_screen`           | `INTEGER` | Passing plays using `SCREEN` route                     |                   |                                        |
| `n_slant`            | `INTEGER` | Passing plays using `SLANT` route                      |                   |                                        |
| `n_wheel`            | `INTEGER` | Passing plays using `WHEEL` route                      |                   |                                        |
| `n_other_routes`     | `INTEGER` | Passing plays using other or unclassified routes       |                   |                                        |
| `avg_time_to_throw`  | `NUMERIC` | Average time (seconds) from snap to throw              |                   |                                        |
| `pressures_allowed`  | `INTEGER` | Total QB pressures allowed across all offensive plays  |                   |                                        |

# PBP Defense Participation
| Column                   | Type      | Description                                                  | Key               | Source File                          |
| ------------------------ | --------- | ------------------------------------------------------------ | ----------------- | ------------------------------------ |
| `game_id`                | `TEXT`    | Unique game identifier (e.g., `2016_01_BUF_BAL`)             | Primary Key (1/2) | `participation_defense_pbp.parquet` |
| `play_id`                | `INTEGER` | Unique play identifier within game                           | Primary Key (2/2) |                                      |
| `season`                 | `INTEGER` | NFL season                                                   |                   |                                      |
| `week`                   | `INTEGER` | NFL week                                                     |                   |                                      |
| `defense_team`          | `TEXT`    | Defensive team abbreviation                                  |                   |                                      |
| `play_type`             | `TEXT`    | Type of play (`pass`, `run`, etc.)                           |                   |                                      |
| `possession_team`       | `TEXT`    | Team on offense during the play                              |                   |                                      |
| `defense_personnel`     | `TEXT`    | Personnel string for defensive unit (e.g., `4 DL, 2 LB, 5 DB`)|                   |                                      |
| `defenders_in_box`      | `INTEGER` | Number of defenders in the box                               |                   |                                      |
| `number_of_pass_rushers`| `INTEGER` | Number of defenders rushing the passer                       |                   |                                      |
| `defense_man_zone_type` | `TEXT`    | Binary classification of coverage (`man`, `zone`)            |                   |                                      |
| `defense_coverage_type` | `TEXT`    | Named coverage scheme (e.g., `Cover 3`, `Cover 1`)           |                   |                                      |
| `time_to_throw`         | `NUMERIC` | Time in seconds from snap to throw (if applicable)           |                   |                                      |
| `was_pressure`          | `BOOLEAN` | Whether the QB was pressured on the play                     |                   |                                      |
| `team1`                 | `TEXT`    | Home team abbreviation                                       |                   |                                      |
| `team2`                 | `TEXT`    | Away team abbreviation                                       |                   |                                      |
| `rush_bin`              | `TEXT`    | Categorized rush intensity (e.g., `low`, `standard`, `blitz`) |                  |                                      |
| `box_bin`               | `TEXT`    | Categorized box count (e.g., `light`, `standard`, `stacked`) |                   |                                      |
| `cumulative_pass`       | `INTEGER` | Cumulative pass plays seen by defense                        |                   |                                      |
| `cumulative_run`        | `INTEGER` | Cumulative run plays seen by defense                         |                   |                                      |
| `cumulative_low_rush`   | `INTEGER` | Cumulative plays with low pass rush                          |                   |                                      |
| `cumulative_standard_rush` | `INTEGER` | Cumulative plays with standard rush                        |                   |                                      |
| `cumulative_blitz`      | `INTEGER` | Cumulative blitz plays                                       |                   |                                      |
| `cumulative_heavy_blitz`| `INTEGER` | Cumulative heavy blitz plays                                 |                   |                                      |
| `cumulative_light_box`  | `INTEGER` | Cumulative plays with light box                              |                   |                                      |
| `cumulative_standard_box`| `INTEGER` | Cumulative plays with standard box                          |                   |                                      |
| `cumulative_stacked_box`| `INTEGER` | Cumulative plays with stacked box                            |                   |                                      |
| `cumulative_man`        | `INTEGER` | Cumulative man coverage plays                                |                   |                                      |
| `cumulative_zone`       | `INTEGER` | Cumulative zone coverage plays                               |                   |                                      |
| `cumulative_cover_0`    | `INTEGER` | Cumulative plays using Cover 0                               |                   |                                      |
| `cumulative_cover_1`    | `INTEGER` | Cumulative plays using Cover 1                               |                   |                                      |
| `cumulative_cover_2`    | `INTEGER` | Cumulative plays using Cover 2                               |                   |                                      |
| `cumulative_cover_3`    | `INTEGER` | Cumulative plays using Cover 3                               |                   |                                      |
| `cumulative_cover_4`    | `INTEGER` | Cumulative plays using Cover 4                               |                   |                                      |
| `cumulative_cover_6`    | `INTEGER` | Cumulative plays using Cover 6                               |                   |                                      |
| `cumulative_cover_2_man`| `INTEGER` | Cumulative plays using Cover 2-Man                           |                   |                                      |
| `cumulative_prevent`    | `INTEGER` | Cumulative prevent defense plays                             |                   |                                      |

# Game Defense Participation
| Column                        | Type      | Description                                               | Key               | Source File                          |
| ----------------------------- | --------- | --------------------------------------------------------- | ----------------- | ------------------------------------ |
| `game_id`                     | `TEXT`    | Unique game identifier (e.g., `2016_01_BUF_BAL`)          | Primary Key (1/2) | `participation_defense_game.parquet` |
| `defense_team`               | `TEXT`    | Defensive team abbreviation                               | Primary Key (2/2) |                                      |
| `season`                      | `INTEGER` | NFL season                                                |                   |                                      |
| `week`                        | `INTEGER` | NFL week                                                  |                   |                                      |
| `n_plays`                     | `INTEGER` | Total defensive plays                                     |                   |                                      |
| `n_pass`                      | `INTEGER` | Number of pass plays defended                             |                   |                                      |
| `n_run`                       | `INTEGER` | Number of run plays defended                              |                   |                                      |
| `n_low_rush`                  | `INTEGER` | Plays with low pass rush                                  |                   |                                      |
| `n_standard_rush`             | `INTEGER` | Plays with standard pass rush                             |                   |                                      |
| `n_blitz`                     | `INTEGER` | Plays with blitz                                          |                   |                                      |
| `n_heavy_blitz`              | `INTEGER` | Plays with heavy blitz                                    |                   |                                      |
| `n_light_box`                | `INTEGER` | Plays with light defensive box                            |                   |                                      |
| `n_standard_box`             | `INTEGER` | Plays with standard defensive box                         |                   |                                      |
| `n_stacked_box`              | `INTEGER` | Plays with stacked defensive box                          |                   |                                      |
| `n_man`                       | `INTEGER` | Man coverage plays                                        |                   |                                      |
| `n_zone`                      | `INTEGER` | Zone coverage plays                                       |                   |                                      |
| `n_cover_0`                   | `INTEGER` | Plays using Cover 0                                       |                   |                                      |
| `n_cover_1`                   | `INTEGER` | Plays using Cover 1                                       |                   |                                      |
| `n_cover_2`                   | `INTEGER` | Plays using Cover 2                                       |                   |                                      |
| `n_cover_3`                   | `INTEGER` | Plays using Cover 3                                       |                   |                                      |
| `n_cover_4`                   | `INTEGER` | Plays using Cover 4                                       |                   |                                      |
| `n_cover_6`                   | `INTEGER` | Plays using Cover 6                                       |                   |                                      |
| `n_cover_2_man`              | `INTEGER` | Plays using Cover 2-Man                                   |                   |                                      |
| `n_prevent`                   | `INTEGER` | Plays using prevent defense                               |                   |                                      |
| `n_pressures`                | `INTEGER` | Total QB pressures generated                              |                   |                                      |
| `avg_time_to_throw`          | `NUMERIC` | Average time (seconds) from snap to throw                 |                   |                                      |
| `cumulative_plays`           | `INTEGER` | Cumulative defensive plays through current week           |                   |                                      |
| `cumulative_pass`            | `INTEGER` | Cumulative pass plays through current week                |                   |                                      |
| `cumulative_run`             | `INTEGER` | Cumulative run plays through current week                 |                   |                                      |
| `cumulative_low_rush`        | `INTEGER` | Cumulative low pass rush plays                            |                   |                                      |
| `cumulative_standard_rush`   | `INTEGER` | Cumulative standard pass rush plays                       |                   |                                      |
| `cumulative_blitz`           | `INTEGER` | Cumulative blitz plays                                    |                   |                                      |
| `cumulative_heavy_blitz`     | `INTEGER` | Cumulative heavy blitz plays                              |                   |                                      |
| `cumulative_light_box`       | `INTEGER` | Cumulative light box plays                                |                   |                                      |
| `cumulative_standard_box`    | `INTEGER` | Cumulative standard box plays                             |                   |                                      |
| `cumulative_stacked_box`     | `INTEGER` | Cumulative stacked box plays                              |                   |                                      |
| `cumulative_man`             | `INTEGER` | Cumulative man coverage plays                             |                   |                                      |
| `cumulative_zone`            | `INTEGER` | Cumulative zone coverage plays                            |                   |                                      |
| `cumulative_cover_0`         | `INTEGER` | Cumulative Cover 0 plays                                  |                   |                                      |
| `cumulative_cover_1`         | `INTEGER` | Cumulative Cover 1 plays                                  |                   |                                      |
| `cumulative_cover_2`         | `INTEGER` | Cumulative Cover 2 plays                                  |                   |                                      |
| `cumulative_cover_3`         | `INTEGER` | Cumulative Cover 3 plays                                  |                   |                                      |
| `cumulative_cover_4`         | `INTEGER` | Cumulative Cover 4 plays                                  |                   |                                      |
| `cumulative_cover_6`         | `INTEGER` | Cumulative Cover 6 plays                                  |                   |                                      |
| `cumulative_cover_2_man`     | `INTEGER` | Cumulative Cover 2-Man plays                              |                   |                                      |
| `cumulative_prevent`         | `INTEGER` | Cumulative prevent defense plays                          |                   |                                      |
| `cumulative_pressures`       | `INTEGER` | Cumulative QB pressures generated                         |                   |                                      |
| `avg_time_to_throw_to_date`  | `NUMERIC` | Average time to throw across season to date               |                   |                                      |

# Season Defense Participation
| Column               | Type      | Description                                              | Key               | Source File                             |
| -------------------- | --------- | -------------------------------------------------------- | ----------------- | --------------------------------------- |
| `season`             | `INTEGER` | NFL season                                               | Primary Key (1/2) | `participation_defense_season.parquet` |
| `defense_team`       | `TEXT`    | Defensive team abbreviation                              | Primary Key (2/2) |                                         |
| `n_plays`            | `INTEGER` | Total defensive plays                                    |                   |                                         |
| `n_pass`             | `INTEGER` | Number of pass plays defended                            |                   |                                         |
| `n_run`              | `INTEGER` | Number of run plays defended                             |                   |                                         |
| `n_low_rush`         | `INTEGER` | Plays with low pass rush                                 |                   |                                         |
| `n_standard_rush`    | `INTEGER` | Plays with standard pass rush                            |                   |                                         |
| `n_blitz`            | `INTEGER` | Plays with blitz                                         |                   |                                         |
| `n_heavy_blitz`      | `INTEGER` | Plays with heavy blitz                                   |                   |                                         |
| `n_light_box`        | `INTEGER` | Plays with light defensive box                           |                   |                                         |
| `n_standard_box`     | `INTEGER` | Plays with standard defensive box                        |                   |                                         |
| `n_stacked_box`      | `INTEGER` | Plays with stacked defensive box                         |                   |                                         |
| `n_man`              | `INTEGER` | Man coverage plays                                       |                   |                                         |
| `n_zone`             | `INTEGER` | Zone coverage plays                                      |                   |                                         |
| `n_cover_0`          | `INTEGER` | Plays using Cover 0                                      |                   |                                         |
| `n_cover_1`          | `INTEGER` | Plays using Cover 1                                      |                   |                                         |
| `n_cover_2`          | `INTEGER` | Plays using Cover 2                                      |                   |                                         |
| `n_cover_3`          | `INTEGER` | Plays using Cover 3                                      |                   |                                         |
| `n_cover_4`          | `INTEGER` | Plays using Cover 4                                      |                   |                                         |
| `n_cover_6`          | `INTEGER` | Plays using Cover 6                                      |                   |                                         |
| `n_cover_2_man`      | `INTEGER` | Plays using Cover 2-Man                                  |                   |                                         |
| `n_prevent`          | `INTEGER` | Plays using prevent defense                              |                   |                                         |
| `n_pressures`        | `INTEGER` | Total QB pressures generated                             |                   |                                         |
| `avg_time_to_throw`  | `NUMERIC` | Average time (seconds) from snap to throw                |                   |                                         |

# Depth Chart Player Starts
| Column         | Type      | Description                                      | Key               | Source File                          |
| -------------- | --------- | ------------------------------------------------ | ----------------- | ------------------------------------ |
| `season`       | `INTEGER` | NFL season                                       | Primary Key (1/3) | `depth_charts_player_starts.parquet` |
| `team`         | `TEXT`    | Team abbreviation                                | Primary Key (2/3) |                                      |
| `position`     | `TEXT`    | Player position (e.g., `QB`, `WR`, `DL`)         | Primary Key (3/3) |                                      |
| `gsis_id`      | `TEXT`    | Unique player identifier                         |                   |                                      |
| `total_starts` | `INTEGER` | Total number of starts at this position & team   |                   |                                      |

# Depth Chart Position Stability
| Column                 | Type      | Description                                               | Key               | Source File                                 |
| ---------------------- | --------- | --------------------------------------------------------- | ----------------- | ------------------------------------------- |
| `season`               | `INTEGER` | NFL season                                                | Primary Key (1/4) | `depth_charts_position_stability.parquet`   |
| `week`                 | `INTEGER` | NFL week                                                  | Primary Key (2/4) |                                             |
| `team`                 | `TEXT`    | Team abbreviation                                          | Primary Key (3/4) |                                             |
| `position`             | `TEXT`    | Position group (e.g., `OL`, `WR`, `DL`)                   | Primary Key (4/4) |                                             |
| `position_group_score` | `NUMERIC` | Normalized stability score for position group that week   |                   |                                             |

# Depth Chart QB
| Column                 | Type      | Description                                          | Key               | Source File                          |
| ---------------------- | --------- | ---------------------------------------------------- | ----------------- | ------------------------------------ |
| `season`               | `INTEGER` | NFL season                                           | Primary Key (1/3) | `depth_charts_qb_team.parquet`       |
| `week`                 | `INTEGER` | NFL week                                             | Primary Key (2/3) |                                      |
| `team`                 | `TEXT`    | Team abbreviation                                    | Primary Key (3/3) |                                      |
| `distinct_qb_starters` | `INTEGER` | Number of distinct QB starters for team this season  |                   |                                      |

# Depth Chart Starters
| Column            | Type      | Description                                               | Key               | Source File                          |
| ----------------- | --------- | --------------------------------------------------------- | ----------------- | ------------------------------------ |
| `season`          | `INTEGER` | NFL season                                                | Primary Key (1/3) | `depth_charts_starters.parquet`      |
| `week`            | `INTEGER` | NFL week                                                  | Primary Key (2/3) |                                      |
| `team`            | `TEXT`    | Team abbreviation                                         | Primary Key (3/3) |                                      |
| `player`          | `TEXT`    | Player full name                                          |                   |                                      |
| `position`        | `TEXT`    | Player position (e.g., `QB`, `WR`, `DL`)                  |                   |                                      |
| `gsis_id`         | `TEXT`    | Unique player identifier                                  |                   |                                      |
| `position_group`  | `TEXT`    | Grouped position (e.g., `OL`, `DB`, `RB`)                 |                   |                                      |
| `player_starts`   | `INTEGER` | Number of previous starts by the player                  |                   |                                      |
| `is_new_starter`  | `BOOLEAN` | Whether player is starting for the first time this year  |                   |                                      |

# Special Team Weekly Stats
| Column                          | Type      | Description                                                | Key               | Source File                          |
| ------------------------------- | --------- | ---------------------------------------------------------- | ----------------- | ------------------------------------ |
| `season`                        | `INTEGER` | NFL season                                                 | Primary Key (1/3) | `st_player_stats_weekly.parquet`     |
| `week`                          | `INTEGER` | NFL week                                                   | Primary Key (2/3) |                                      |
| `season_type`                   | `TEXT`    | Type of season (`REG`, `POST`)                             | Primary Key (3/3) |                                      |
| `player_id`                     | `TEXT`    | Unique player identifier                                   |                   |                                      |
| `player_name`                   | `TEXT`    | Full player name                                           |                   |                                      |
| `team`                          | `TEXT`    | Team abbreviation                                           |                   |                                      |
| `position`                      | `TEXT`    | Player position (`K`, `P`, etc.)                           |                   |                                      |
| `games`                         | `INTEGER` | Games played this week                                     |                   |                                      |
| `fg_made_0_19`                  | `INTEGER` | Field goals made from 0–19 yards                          |                   |                                      |
| `fg_made_20_29`                 | `INTEGER` | Field goals made from 20–29 yards                         |                   |                                      |
| `fg_made_30_39`                 | `INTEGER` | Field goals made from 30–39 yards                         |                   |                                      |
| `fg_made_40_49`                 | `INTEGER` | Field goals made from 40–49 yards                         |                   |                                      |
| `fg_made_50_59`                 | `INTEGER` | Field goals made from 50–59 yards                         |                   |                                      |
| `fg_made_60`                    | `INTEGER` | Field goals made from 60+ yards                           |                   |                                      |
| `fg_missed_0_19`                | `INTEGER` | Field goals missed from 0–19 yards                        |                   |                                      |
| `fg_missed_20_29`               | `INTEGER` | Field goals missed from 20–29 yards                       |                   |                                      |
| `fg_missed_30_39`               | `INTEGER` | Field goals missed from 30–39 yards                       |                   |                                      |
| `fg_missed_40_49`               | `INTEGER` | Field goals missed from 40–49 yards                       |                   |                                      |
| `fg_missed_50_59`               | `INTEGER` | Field goals missed from 50–59 yards                       |                   |                                      |
| `fg_missed_60`                  | `INTEGER` | Field goals missed from 60+ yards                         |                   |                                      |
| `xp_made`                       | `INTEGER` | Extra points made                                          |                   |                                      |
| `xp_missed`                     | `INTEGER` | Extra points missed                                        |                   |                                      |
| `punt_avg`                      | `NUMERIC` | Average punt distance (yards)                              |                   |                                      |
| `punt_net_avg`                  | `NUMERIC` | Net average punt distance (yards)                          |                   |                                      |
| `punt_touchbacks`              | `INTEGER` | Number of punts resulting in touchbacks                   |                   |                                      |
| `punt_inside_20`               | `INTEGER` | Number of punts downed inside the 20-yard line            |                   |                                      |
| `punt_blocked`                 | `INTEGER` | Number of punts blocked                                   |                   |                                      |
| `fg_blocked`                   | `INTEGER` | Number of field goals blocked                             |                   |                                      |
| `cumulative_fg_made_0_19`      | `INTEGER` | Cumulative field goals made from 0–19 yards               |                   |                                      |
| `cumulative_fg_made_20_29`     | `INTEGER` | Cumulative field goals made from 20–29 yards              |                   |                                      |
| `cumulative_fg_made_30_39`     | `INTEGER` | Cumulative field goals made from 30–39 yards              |                   |                                      |
| `cumulative_fg_made_40_49`     | `INTEGER` | Cumulative field goals made from 40–49 yards              |                   |                                      |
| `cumulative_fg_made_50_59`     | `INTEGER` | Cumulative field goals made from 50–59 yards              |                   |                                      |
| `cumulative_fg_made_60`        | `INTEGER` | Cumulative field goals made from 60+ yards                |                   |                                      |
| `cumulative_fg_missed_0_19`    | `INTEGER` | Cumulative field goals missed from 0–19 yards             |                   |                                      |
| `cumulative_fg_missed_20_29`   | `INTEGER` | Cumulative field goals missed from 20–29 yards            |                   |                                      |
| `cumulative_fg_missed_30_39`   | `INTEGER` | Cumulative field goals missed from 30–39 yards            |                   |                                      |
| `cumulative_fg_missed_40_49`   | `INTEGER` | Cumulative field goals missed from 40–49 yards            |                   |                                      |
| `cumulative_fg_missed_50_59`   | `INTEGER` | Cumulative field goals missed from 50–59 yards            |                   |                                      |
| `cumulative_fg_missed_60`      | `INTEGER` | Cumulative field goals missed from 60+ yards              |                   |                                      |

# Special Team Season Stats
| Column               | Type      | Description                                              | Key               | Source File                          |
| -------------------- | --------- | -------------------------------------------------------- | ----------------- | ------------------------------------ |
| `season`             | `INTEGER` | NFL season                                               | Primary Key (1/2) | `st_player_stats_season.parquet`     |
| `player_id`          | `TEXT`    | Unique player identifier                                 | Primary Key (2/2) |                                      |
| `player_name`        | `TEXT`    | Full player name                                         |                   |                                      |
| `team`               | `TEXT`    | Team abbreviation                                        |                   |                                      |
| `position`           | `TEXT`    | Player position (`K`, `P`, etc.)                         |                   |                                      |
| `games_played`       | `INTEGER` | Number of games played                                   |                   |                                      |
| `fg_att`             | `INTEGER` | Field goals attempted                                    |                   |                                      |
| `fg_made`            | `INTEGER` | Field goals made                                         |                   |                                      |
| `fg_missed`          | `INTEGER` | Field goals missed                                       |                   |                                      |
| `fg_blocked`         | `INTEGER` | Field goals blocked                                      |                   |                                      |
| `fg_pct`             | `NUMERIC` | Field goal success rate                                  |                   |                                      |
| `fg_long`            | `NUMERIC` | Longest field goal made (yards)                          |                   |                                      |
| `fg_made_0_19`       | `INTEGER` | Field goals made from 0–19 yards                         |                   |                                      |
| `fg_made_20_29`      | `INTEGER` | Field goals made from 20–29 yards                        |                   |                                      |
| `fg_made_30_39`      | `INTEGER` | Field goals made from 30–39 yards                        |                   |                                      |
| `fg_made_40_49`      | `INTEGER` | Field goals made from 40–49 yards                        |                   |                                      |
| `fg_made_50_59`      | `INTEGER` | Field goals made from 50–59 yards                        |                   |                                      |
| `fg_made_60`         | `INTEGER` | Field goals made from 60+ yards                          |                   |                                      |
| `fg_missed_0_19`     | `INTEGER` | Field goals missed from 0–19 yards                       |                   |                                      |
| `fg_missed_20_29`    | `INTEGER` | Field goals missed from 20–29 yards                      |                   |                                      |
| `fg_missed_30_39`    | `INTEGER` | Field goals missed from 30–39 yards                      |                   |                                      |
| `fg_missed_40_49`    | `INTEGER` | Field goals missed from 40–49 yards                      |                   |                                      |
| `fg_missed_50_59`    | `INTEGER` | Field goals missed from 50–59 yards                      |                   |                                      |
| `fg_missed_60`       | `INTEGER` | Field goals missed from 60+ yards                        |                   |                                      |
| `fg_made_distance`   | `NUMERIC` | Average distance of made field goals                     |                   |                                      |
| `fg_missed_distance` | `NUMERIC` | Average distance of missed field goals                   |                   |                                      |
| `fg_blocked_distance`| `NUMERIC` | Average distance of blocked field goals                  |                   |                                      |
| `pat_att`            | `INTEGER` | Extra point attempts                                     |                   |                                      |
| `pat_made`           | `INTEGER` | Extra points made                                        |                   |                                      |
| `pat_missed`         | `INTEGER` | Extra points missed                                      |                   |                                      |
| `pat_blocked`        | `INTEGER` | Extra points blocked                                     |                   |                                      |
| `pat_pct`            | `NUMERIC` | Extra point success rate                                 |                   |                                      |
| `gwfg_att`           | `INTEGER` | Game-winning field goal attempts                         |                   |                                      |
| `gwfg_distance`      | `NUMERIC` | Average distance of game-winning field goal attempts     |                   |                                      |
| `gwfg_made`          | `INTEGER` | Game-winning field goals made                            |                   |                                      |
| `gwfg_missed`        | `INTEGER` | Game-winning field goals missed                          |                   |                                      |
| `gwfg_blocked`       | `INTEGER` | Game-winning field goals blocked                         |                   |                                      |

# Play-By-Play
| Column                | Type      | Description                                               | Key               | Source File              |
| ---------------------| --------- | --------------------------------------------------------- | ----------------- | ------------------------ |
| `game_id`            | `TEXT`    | Unique game identifier (e.g., `2016_01_BUF_BAL`)          | Primary Key (1/2) | `pbp_cleaned.parquet`    |
| `play_id`            | `INTEGER` | Unique play identifier within game                        | Primary Key (2/2) |                          |
| `qtr`                | `INTEGER` | Quarter of the game (1–5)                                 |                   |                          |
| `down`               | `INTEGER` | Current down (1–4)                                        |                   |                          |
| `ydstogo`            | `INTEGER` | Yards to go for first down                                |                   |                          |
| `yardline_100`       | `INTEGER` | Distance from end zone (own 0 = 100, opponent 0 = 0)      |                   |                          |
| `posteam`            | `TEXT`    | Possession team                                           |                   |                          |
| `defteam`            | `TEXT`    | Defensive team                                            |                   |                          |
| `play_type`          | `TEXT`    | Type of play (e.g., `pass`, `run`, `no_play`)             |                   |                          |
| `epa`                | `NUMERIC` | Expected points added on this play                        |                   |                          |
| `success`            | `INTEGER` | Whether the play was a success (1 = yes, 0 = no)          |                   |                          |
| `touchdown`          | `INTEGER` | Whether a touchdown was scored                            |                   |                          |
| `interception`       | `INTEGER` | Whether an interception occurred                          |                   |                          |
| `penalty`            | `INTEGER` | Whether a penalty occurred                                |                   |                          |
| `pass`               | `INTEGER` | Indicator for pass play                                   |                   |                          |
| `rush`               | `INTEGER` | Indicator for run play                                    |                   |                          |
| `special`            | `INTEGER` | Indicator for special teams play                          |                   |                          |
| `cum_epa_posteam`    | `NUMERIC` | Cumulative EPA for offensive team                         |                   |                          |
| `cum_success_posteam`| `INTEGER` | Cumulative successful plays for offensive team            |                   |                          |
| `cum_td_posteam`     | `INTEGER` | Cumulative touchdowns by offensive team                   |                   |                          |
| `cum_int_posteam`    | `INTEGER` | Cumulative interceptions thrown by offensive team         |                   |                          |
| `cum_penalty_posteam`| `INTEGER` | Cumulative penalties by offensive team                    |                   |                          |
| `cum_epa_defense`    | `NUMERIC` | Cumulative EPA allowed by defense                         |                   |                          |
| `cum_success_defense`| `INTEGER` | Cumulative successful plays allowed by defense            |                   |                          |
| `cum_td_allowed`     | `INTEGER` | Cumulative touchdowns allowed by defense                  |                   |                          |
| `cum_int_defense`    | `INTEGER` | Cumulative interceptions by defense                       |                   |                          |
| `cum_penalty_defense`| `INTEGER` | Cumulative penalties by defense                           |                   |                          |

# Roster Summary
| Column        | Type      | Description                                      | Key               | Source File              |
| ------------- | --------- | ------------------------------------------------ | ----------------- | ------------------------ |
| `season`      | `INTEGER` | NFL season                                       | Primary Key (1/2) | `roster_summary.parquet` |
| `team`        | `TEXT`    | Team abbreviation                                | Primary Key (2/2) |                          |
| `n_players`   | `INTEGER` | Total number of players on the roster            |                   |                          |
| `avg_age`     | `NUMERIC` | Average age of players                           |                   |                          |
| `avg_height`  | `NUMERIC` | Average height of players (inches)               |                   |                          |
| `avg_weight`  | `NUMERIC` | Average weight of players (pounds)               |                   |                          |
| `avg_exp`     | `NUMERIC` | Average years of experience                      |                   |                          |

# Roster Position Summary
| Column        | Type      | Description                                           | Key               | Source File                      |
| ------------- | --------- | ----------------------------------------------------- | ----------------- | -------------------------------- |
| `season`      | `INTEGER` | NFL season                                            | Primary Key (1/3) | `roster_position_summary.parquet` |
| `team`        | `TEXT`    | Team abbreviation                                     | Primary Key (2/3) |                                  |
| `position`    | `TEXT`    | Player position (e.g., `QB`, `WR`, `DL`)              | Primary Key (3/3) |                                  |
| `n_players`   | `INTEGER` | Total number of players at the position               |                   |                                  |
| `avg_age`     | `NUMERIC` | Average age of players at the position                |                   |                                  |
| `avg_height`  | `NUMERIC` | Average height of players at the position (inches)    |                   |                                  |
| `avg_weight`  | `NUMERIC` | Average weight of players at the position (pounds)    |                   |                                  |
| `avg_exp`     | `NUMERIC` | Average years of experience at the position           |                   |                                  |





