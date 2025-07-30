# NFL Analytics Database Tables

This schema is organized by analytical domain and grain level. Each table includes a short description and its primary key(s).

## Table of Contents

- [Core Dimensions](#core-dimensions)
- [Offensive Player Stats](#offensive-player-stats)
  - [Weekly Grain](#weekly-grain)
  - [Season Grain](#season-grain)
  - [Career Grain](#career-grain)
- [Defensive Stats](#defensive-stats)
- [Next Gen Stats (NGS)](#next-gen-stats-ngs)
- [Contracts](#contracts)
- [Injuries](#injuries)
- [Participation](#participation)
- [Depth Charts and Stability](#depth-charts-and-stability)
- [Special Teams](#special-teams)
- [Play-by-Play](#play-by-play)
- [Roster Metadata](#roster-metadata)

---

## Core Dimensions

| Table        | Description                        | Primary Key(s)                           |
|--------------|------------------------------------|------------------------------------------|
| `players`    | Master list of players             | `player_id`                              |
| `teams`      | Master list of NFL teams           | `team_id`                                |
| `games`      | Game metadata                      | `game_id`                                |
| `seasons`    | One row per season and type        | `season`, `season_type`                  |
| `weeks`      | One row per week in each season    | `season`, `week`                         |
| `rosters`    | Weekly player-team affiliation     | `player_id`, `team_id`, `season`, `week` |

---

## Offensive Player Stats

### Weekly Grain

| Table                | Description                    | Primary Key(s)                              |
|----------------------|--------------------------------|---------------------------------------------|
| `weekly_stats_qb`    | Weekly QB stats                | `player_id`, `season`, `week`               |
| `weekly_stats_rb`    | Weekly RB stats                | `player_id`, `season`, `week`               |
| `weekly_stats_wr_te` | Weekly WR/TE receiving stats   | `player_id`, `season`, `week`, `position`   |

### Season Grain

| Table                | Description                      | Primary Key(s)                            |
|----------------------|----------------------------------|-------------------------------------------|
| `season_stats_qb`    | Season-level QB stats            | `player_id`, `season`                     |
| `season_stats_rb`    | Season-level RB stats            | `player_id`, `season`                     |
| `season_stats_wr_te` | Season-level WR/TE stats         | `player_id`, `season`, `position`         |

### Career Grain

| Table                | Description                     | Primary Key(s)            |
|----------------------|---------------------------------|---------------------------|
| `career_stats_qb`    | Career QB stats                 | `player_id`               |
| `career_stats_rb`    | Career RB stats                 | `player_id`               |
| `career_stats_wr_te` | Career WR/TE stats              | `player_id`, `position`   |

---

## Defensive Stats

| Table                      | Description                       | Primary Key(s)                           |
|----------------------------|-----------------------------------|------------------------------------------|
| `def_player_stats_weekly` | Weekly defensive player stats     | `player_id`, `season`, `week`            |
| `def_player_stats_season` | Season-level defensive stats      | `player_id`, `season`                    |
| `def_player_stats_career` | Career defensive player stats     | `player_id`                              |
| `def_team_stats_season`   | Season-level team defense stats   | `team_id`, `season`                      |

---

## Next Gen Stats (NGS)

| Table                             | Description                       | Primary Key(s)                     |
|----------------------------------|-----------------------------------|------------------------------------|
| `nextgen_stats_player_weekly`     | Weekly player NGS                | `player_id`, `season`, `week`      |
| `nextgen_stats_player_season`     | Season-level player NGS          | `player_id`, `season`              |
| `nextgen_stats_player_postseason` | Postseason player NGS            | `player_id`, `season`              |
| `nextgen_stats_player_career`     | Career player NGS                | `player_id`                        |

---

## Contracts

| Table                         | Description                            | Primary Key(s)                     |
|-------------------------------|----------------------------------------|------------------------------------|
| `contracts_qb`               | Detailed QB contract info              | `player_id`, `contract_start`      |
| `contracts_position_cap_pct`| % of cap by position and year          | `season`, `position`               |

---

## Injuries

| Table                      | Description                            | Primary Key(s)                              |
|----------------------------|----------------------------------------|---------------------------------------------|
| `injuries_weekly`          | Weekly injury reports                  | `player_id`, `season`, `week`               |
| `injuries_team_weekly`     | Weekly team-level injuries             | `team_id`, `season`, `week`                 |
| `injuries_team_season`     | Season-level team injuries             | `team_id`, `season`                         |
| `injuries_position_weekly` | Positional injury counts per week      | `team_id`, `position`, `season`, `week`     |

---

## Participation

| Table                                   | Description                                 | Primary Key(s)                              |
|----------------------------------------|---------------------------------------------|---------------------------------------------|
| `participation_offense_pbp`            | Play-level offensive participation          | `game_id`, `play_id`, `offense_team`        |
| `participation_offense_game`           | Game-level offensive participation          | `team_id`, `game_id`                         |
| `participation_offense_formation_game` | Game-level by formation                     | `team_id`, `game_id`, `formation`            |
| `participation_offense_season`         | Season-level offensive participation        | `team_id`, `season`                          |
| `participation_defense_pbp`            | Play-level defensive involvement            | `game_id`, `play_id`, `player_id`            |
| `participation_defense_game`           | Game-level defensive participation          | `team_id`, `game_id`                         |
| `participation_defense_season`         | Season-level defensive participation        | `team_id`, `season`                          |

---

## Depth Charts and Stability

| Table                             | Description                          | Primary Key(s)                            |
|-----------------------------------|--------------------------------------|-------------------------------------------|
| `depth_charts_player_starts`      | Starts per player                    | `player_id`, `season`, `week`             |
| `depth_charts_position_stability` | Positional consistency               | `team_id`, `position`, `season`, `week`   |
| `depth_charts_qb_team`            | QB stability over time               | `team_id`, `season`, `week`               |
| `depth_charts_starters`           | Starting lineup snapshots            | `team_id`, `season`, `week`, `position`   |

---

## Special Teams

| Table                    | Description                    | Primary Key(s)                     |
|--------------------------|--------------------------------|------------------------------------|
| `st_player_stats_weekly` | Weekly special teams stats     | `player_id`, `season`, `week`      |
| `st_player_stats_season` | Season special teams stats     | `player_id`, `season`              |

---

## Play-by-Play

| Table         | Description                 | Primary Key(s)        |
|---------------|-----------------------------|------------------------|
| `pbp_cleaned` | Full play-by-play dataset   | `game_id`, `play_id`  |

---

## Roster Metadata

| Table                      | Description                          | Primary Key(s)                            |
|----------------------------|--------------------------------------|-------------------------------------------|
| `roster_summary`           | Aggregated roster info               | `player_id`, `season`, `week`             |
| `roster_position_summary` | Positional summary per team-week     | `team_id`, `position`, `season`, `week`   |
