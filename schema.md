# NFL Analytics Schema (Normalized & Grain-Aware)

This schema is organized by analytical grain level: **PBP**, **Game**, **Season**, and **Career**.

All tables live in the `nfl` schema.

---

## Core Dimensions

| Table     | Primary Key                              | Description                  | Source File(s)                                |
|-----------|------------------------------------------|------------------------------|------------------------------------------------|
| games     | game_id                                  | Game metadata                | games.parquet                                  |
| teams     | team_id                                  | NFL teams                    | Derived from game/team references              |
| players   | player_id                                | Player master table          | rosters.parquet                                |
| rosters   | player_id, team_id, season, week         | Weekly player affiliation    | rosters.parquet, roster_summary.parquet, roster_position_summary.parquet |

---

## PBP-Level Tables

| Table                        | Primary Key                         | Description                            | Source File(s)                                |
|------------------------------|-------------------------------------|----------------------------------------|------------------------------------------------|
| pbp_cleaned                  | game_id, play_id                    | Cleaned play-by-play data              | pbp_cleaned.parquet                            |
| offense_participation_pbp   | game_id, play_id, player_id         | Offensive snap tracking per play      | participation_offense_pbp.parquet              |
| defense_participation_pbp   | game_id, play_id, player_id         | Defensive snap tracking per play      | participation_defense_pbp.parquet              |

---

## Game-Level Tables

| Table                           | Primary Key                          | Description                              | Source File(s)                                           |
|----------------------------------|--------------------------------------|------------------------------------------|-----------------------------------------------------------|
| offense_participation_game      | game_id, player_id                   | Offensive participation (game)           | participation_offense_game.parquet                       |
| defense_participation_game      | game_id, player_id                   | Defensive participation (game)           | participation_defense_game.parquet                       |
| special_teams_participation     | game_id, player_id                   | Special teams tracking                   | participation_special_teams_game.parquet *(TBD if exists)*|
| depth_chart_starters            | player_id, team_id, season, week     | Starting players                         | depth_charts_starters.parquet                            |
| qb_team_assignment              | team_id, season, week                | Weekly QB1 assignment                    | depth_charts_qb_team.parquet                             |
| depth_position_stability        | team_id, season, week, position      | Positional continuity                    | depth_charts_position_stability.parquet                  |
| depth_chart_player_starts       | player_id, team_id, season, week     | Total starts by position                 | depth_charts_player_starts.parquet                       |
| injuries_weekly                 | player_id, season, week              | Weekly injury status                     | injuries_weekly.parquet                                  |
| injuries_team_weekly           | team_id, season, week                | Team-level injury summary                | injuries_team_weekly.parquet                             |
| injuries_position_weekly       | team_id, position, season, week      | Position-level injury summary            | injuries_position_weekly.parquet                         |
| injuries_team_season           | team_id, season                      | Seasonal injury aggregation              | injuries_team_season.parquet                             |
| player_weekly_stats            | player_id, season, week              | Player stat lines                        | weekly_stats_qb/rb/wr/te.parquet                         |
| st_player_stats_weekly         | player_id, season, week              | Special teams stats                      | st_player_stats_weekly.parquet                           |
| nextgen_stats_player_weekly    | player_id, season, week              | Weekly advanced tracking data            | nextgen_stats_player_weekly.parquet                      |

---

## Season-Level Tables

| Table                           | Primary Key                         | Description                             | Source File(s)                                          |
|----------------------------------|-------------------------------------|-----------------------------------------|----------------------------------------------------------|
| player_season_stats             | player_id, season                   | Offensive player summaries              | season_stats_qb/rb/wr/te.parquet                        |
| def_player_stats_season         | player_id, season                   | Defensive season stats                  | def_player_stats_season.parquet                         |
| def_player_stats_weekly         | player_id, season, week             | Defensive game stats                    | def_player_stats_weekly.parquet                         |
| st_player_stats_season          | player_id, season                   | Special teams season stats              | st_player_stats_season.parquet                          |
| def_team_stats_season           | team_id, season                     | Team defense stats                      | def_team_stats_season.parquet                           |
| contracts_position_cap_pct      | team_id, season, position           | Position cap percentage                 | contracts_position_cap_pct.parquet                      |
| nextgen_stats_player_season     | player_id, season                   | Season-level tracking data              | nextgen_stats_player_season.parquet                     |

---

## Career-Level Tables

| Table                           | Primary Key                        | Description                             | Source File(s)                                          |
|----------------------------------|-----------------------------------|-----------------------------------------|----------------------------------------------------------|
| player_career_stats             | player_id                         | Offensive career summary                | career_stats_qb/rb/wr/te.parquet                        |
| def_player_stats_career         | player_id                         | Defensive career stats                  | def_player_stats_career.parquet                         |
| nextgen_stats_player_career     | player_id                         | Career tracking metrics                 | nextgen_stats_player_career.parquet                     |
| nextgen_stats_player_postseason | player_id, season, week           | Postseason tracking stats               | nextgen_stats_player_postseason.parquet                 |
| contracts_qb                    | player_id, contract_start         | QB contract details                     | contracts_qb.parquet                                    |

---
