"""
Games Router
------------
Endpoints for weekly game listings and game detail (with season/week membership validation).

Base path: /games
Tags: ["games"]

Notes
-----
- List endpoint returns one row per game for a given season/week.
- Detail endpoint validates that the provided game_id belongs to the given season/week (404 otherwise).
- Column semantics (for UI):
  * 'line' is a STRING from the HOME perspective: negative = home favored; positive (no '+') = home underdog.
  * 'pred_margin' is absolute and rounded to 1 decimal.
  * 'pred_total' is rounded to 1 decimal.
  * 'pred_winner_team' is the model winner (abbr) derived from pred_home_win (1=Home, 0=Away).
  * 'winning_team' is the actual result based on final score ('TIE' if equal), else NULL when not played.
  * 'kickoff' is emitted as ISO-like "YYYY-MM-DDTHH:MM:SS" (no timezone) for your ET-localize helper.
  * Records "(W-L-T)" are composed from team_weekly_tbl; Week 1 falls back to "0-0-0" if missing.
"""

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.db import AsyncSessionLocal

# --- Router setup -------------------------------------------------------------
router = APIRouter(prefix="/games", tags=["games"])

# IMPORTANT: Ensure this table name matches your DB:
#   prod.pregame_margin_abs_prediction_tbl   (singular "prediction", not "predictions")
LIST_QUERY = """
WITH rec AS (
  SELECT
    season,
    week,
    team,
    MAX(CASE WHEN stat_name = 'wins_entering'   THEN value END)::int AS wins_entering,
    MAX(CASE WHEN stat_name = 'losses_entering' THEN value END)::int AS losses_entering,
    MAX(CASE WHEN stat_name = 'ties_entering'   THEN value END)::int AS ties_entering
  FROM prod.team_weekly_tbl
  WHERE season = :season
    AND week   = :week
  GROUP BY season, week, team
)
SELECT
  g.game_id,
  g.season,
  g.week,
  g.home_team,
  g.away_team,

  /* Records entering week (W-L-T). Week 1 -> fallback to 0-0-0 if missing. */
  CASE
    WHEN g.week = 1 THEN
      CONCAT(COALESCE(rh.wins_entering, 0), '-', COALESCE(rh.losses_entering, 0), '-', COALESCE(rh.ties_entering, 0))
    WHEN rh.wins_entering IS NULL OR rh.losses_entering IS NULL OR rh.ties_entering IS NULL THEN NULL
    ELSE CONCAT(rh.wins_entering, '-', rh.losses_entering, '-', rh.ties_entering)
  END AS home_record,
  CASE
    WHEN g.week = 1 THEN
      CONCAT(COALESCE(ra.wins_entering, 0), '-', COALESCE(ra.losses_entering, 0), '-', COALESCE(ra.ties_entering, 0))
    WHEN ra.wins_entering IS NULL OR ra.losses_entering IS NULL OR ra.ties_entering IS NULL THEN NULL
    ELSE CONCAT(ra.wins_entering, '-', ra.losses_entering, '-', ra.ties_entering)
  END AS away_record,

  /* ISO-like timestamp for UI's ET-localize helper */
  TO_CHAR(g.kickoff, 'YYYY-MM-DD"T"HH24:MI:SS') AS kickoff,

  g.stadium,

  /* Home-perspective line: negative = home favored; positive (no '+') = home underdog */
  CASE
    WHEN g.favored_team IS NULL OR g.spread_line IS NULL THEN NULL
    WHEN g.favored_team = g.home_team THEN '-' || TO_CHAR(ROUND(ABS(g.spread_line)::numeric, 1), 'FM990.0')
    ELSE TO_CHAR(ROUND(ABS(g.spread_line)::numeric, 1), 'FM990.0')
  END AS line,

  g.total_line AS vegas_total,

  /* Predictions */
  ROUND(t.pred_vote::numeric, 1) AS pred_total,
  ROUND(m.pred_vote::numeric, 1) AS pred_margin,

  CASE
    WHEN p.pred_home_win::int = 1 THEN g.home_team
    WHEN p.pred_home_win::int = 0 THEN g.away_team
    ELSE NULL
  END AS pred_winner_team,

  /* Actual winner (when final); 'TIE' if equal; NULL if not played / missing scores */
  CASE
    WHEN g.home_score > g.away_score THEN g.home_team
    WHEN g.home_score < g.away_score THEN g.away_team
    WHEN g.home_score = g.away_score THEN 'TIE'
    ELSE NULL
  END AS winning_team,

  g.home_score,
  g.away_score

FROM prod.games_tbl g
LEFT JOIN rec rh ON rh.season = g.season AND rh.week = g.week AND rh.team = g.home_team
LEFT JOIN rec ra ON ra.season = g.season AND ra.week = g.week AND ra.team = g.away_team
LEFT JOIN prod.pregame_total_predictions_tbl      t ON t.game_id = g.game_id
LEFT JOIN prod.pregame_margin_abs_predictions_tbl  m ON m.game_id = g.game_id
LEFT JOIN prod.pregame_predictions_tbl            p ON p.game_id = g.game_id
WHERE g.season = :season
  AND g.week   = :week
ORDER BY (g.kickoff IS NULL), g.kickoff ASC;
"""

DETAIL_QUERY = """
/* Same shape as LIST_QUERY but scoped to a single game_id and validates membership */
WITH rec AS (
  SELECT
    season,
    week,
    team,
    MAX(CASE WHEN stat_name = 'wins_entering'   THEN value END)::int AS wins_entering,
    MAX(CASE WHEN stat_name = 'losses_entering' THEN value END)::int AS losses_entering,
    MAX(CASE WHEN stat_name = 'ties_entering'   THEN value END)::int AS ties_entering
  FROM prod.team_weekly_tbl
  WHERE season = :season
    AND week   = :week
  GROUP BY season, week, team
)
SELECT
  g.game_id,
  g.season,
  g.week,
  g.home_team,
  g.away_team,

  CASE
    WHEN g.week = 1 THEN
      CONCAT(COALESCE(rh.wins_entering, 0), '-', COALESCE(rh.losses_entering, 0), '-', COALESCE(rh.ties_entering, 0))
    WHEN rh.wins_entering IS NULL OR rh.losses_entering IS NULL OR rh.ties_entering IS NULL THEN NULL
    ELSE CONCAT(rh.wins_entering, '-', rh.losses_entering, '-', rh.ties_entering)
  END AS home_record,
  CASE
    WHEN g.week = 1 THEN
      CONCAT(COALESCE(ra.wins_entering, 0), '-', COALESCE(ra.losses_entering, 0), '-', COALESCE(ra.ties_entering, 0))
    WHEN ra.wins_entering IS NULL OR ra.losses_entering IS NULL OR ra.ties_entering IS NULL THEN NULL
    ELSE CONCAT(ra.wins_entering, '-', ra.losses_entering, '-', ra.ties_entering)
  END AS away_record,

  TO_CHAR(g.kickoff, 'YYYY-MM-DD"T"HH24:MI:SS') AS kickoff,

  g.stadium,

  CASE
    WHEN g.favored_team IS NULL OR g.spread_line IS NULL THEN NULL
    WHEN g.favored_team = g.home_team THEN '-' || TO_CHAR(ROUND(ABS(g.spread_line)::numeric, 1), 'FM990.0')
    ELSE TO_CHAR(ROUND(ABS(g.spread_line)::numeric, 1), 'FM990.0')
  END AS line,

  g.total_line AS vegas_total,

  ROUND(t.pred_vote::numeric, 1) AS pred_total,
  ROUND(m.pred_vote::numeric, 1) AS pred_margin,

  CASE
    WHEN p.pred_home_win::int = 1 THEN g.home_team
    WHEN p.pred_home_win::int = 0 THEN g.away_team
    ELSE NULL
  END AS pred_winner_team,

  CASE
    WHEN g.home_score > g.away_score THEN g.home_team
    WHEN g.home_score < g.away_score THEN g.away_team
    WHEN g.home_score = g.away_score THEN 'TIE'
    ELSE NULL
  END AS winning_team,

  g.home_score,
  g.away_score

FROM prod.games_tbl g
LEFT JOIN rec rh ON rh.season = g.season AND rh.week = g.week AND rh.team = g.home_team
LEFT JOIN rec ra ON ra.season = g.season AND ra.week = g.week AND ra.team = g.away_team
LEFT JOIN prod.pregame_total_predictions_tbl      t ON t.game_id = g.game_id
LEFT JOIN prod.pregame_margin_abs_predictions_tbl  m ON m.game_id = g.game_id
LEFT JOIN prod.pregame_predictions_tbl            p ON p.game_id = g.game_id
WHERE g.season = :season
  AND g.week   = :week
  AND g.game_id = :game_id
ORDER BY (g.kickoff IS NULL), g.kickoff ASC;
"""

@router.get("/{season}/{week}")
async def list_games(season: int, week: int):
    """Return all games for a given season/week, sorted by kickoff (nulls last).

    Shape (per row):
        game_id, season, week,
        home_team, away_team,
        home_record, away_record,        -- "(W-L-T)" strings (Week 1 falls back to 0-0-0)
        kickoff,                         -- "YYYY-MM-DDTHH:MM:SS" (no tz)
        stadium,
        line,                            -- string; negative = home favored, positive = home underdog (no '+')
        vegas_total,                     -- numeric or null
        pred_total,                      -- numeric or null (rounded 1 dp)
        pred_margin,                     -- numeric or null (abs, rounded 1 dp)
        pred_winner_team,                -- model's winner abbr or null
        winning_team,                    -- actual winner abbr / 'TIE' / null
        home_score, away_score           -- numeric or null
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(LIST_QUERY), {"season": season, "week": week})
        rows = result.mappings().all()
        return [dict(r) for r in rows]

@router.get("/{season}/{week}/{game_id}")
async def get_game_detail(season: int, week: int, game_id: str):
    """Return a single game's detail for season/week, validating membership (404 if mismatched)."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(DETAIL_QUERY),
            {"season": season, "week": week, "game_id": game_id},
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Game not found for given season/week")
        return dict(row)
      
  # In your existing api/app/routers/games.py (same file as the other /games endpoints)

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.db import AsyncSessionLocal

# ... keep your existing router + endpoints ...

# --- New: Game Detail Stats (for Offense/Defense/Special toggles) ------------

STATS_QUERY = """
WITH g AS (
  SELECT game_id, season, week, home_team, away_team, home_score, away_score
  FROM prod.games_tbl
  WHERE season = :season AND week = :week AND game_id = :game_id
),
agg AS (
  SELECT
    team,
    MAX(CASE WHEN stat_name = 'attempts'                 THEN value END) AS attempts,
    MAX(CASE WHEN stat_name = 'completions'              THEN value END) AS completions,
    MAX(CASE WHEN stat_name = 'passing_yards'            THEN value END) AS passing_yards,
    MAX(CASE WHEN stat_name = 'passing_tds'              THEN value END) AS passing_tds,
    MAX(CASE WHEN stat_name = 'interceptions'            THEN value END) AS interceptions,
    MAX(CASE WHEN stat_name = 'passing_first_downs'      THEN value END) AS passing_first_downs,
    MAX(CASE WHEN stat_name = 'sacks'                    THEN value END) AS sacks,
    MAX(CASE WHEN stat_name = 'sack_yards'               THEN value END) AS sack_yards,

    MAX(CASE WHEN stat_name = 'carries'                  THEN value END) AS carries,
    MAX(CASE WHEN stat_name = 'rushing_yards'            THEN value END) AS rushing_yards,
    MAX(CASE WHEN stat_name = 'rushing_tds'              THEN value END) AS rushing_tds,
    MAX(CASE WHEN stat_name = 'rushing_first_downs'      THEN value END) AS rushing_first_downs,
    MAX(CASE WHEN stat_name = 'rushing_fumbles'          THEN value END) AS rushing_fumbles,
    MAX(CASE WHEN stat_name = 'rushing_fumbles_lost'     THEN value END) AS rushing_fumbles_lost,

    -- Team penalties (dataset exposes totals via "def_*")
    MAX(CASE WHEN stat_name = 'def_penalty'              THEN value END) AS def_penalty,
    MAX(CASE WHEN stat_name = 'def_penalty_yards'        THEN value END) AS def_penalty_yards,

    -- Defense
    MAX(CASE WHEN stat_name = 'points_allowed'           THEN value END) AS points_allowed,
    MAX(CASE WHEN stat_name = 'def_sacks'                THEN value END) AS def_sacks,
    MAX(CASE WHEN stat_name = 'def_sack_yards'           THEN value END) AS def_sack_yards,
    MAX(CASE WHEN stat_name = 'def_qb_hits'              THEN value END) AS def_qb_hits,
    MAX(CASE WHEN stat_name = 'def_interceptions'        THEN value END) AS def_interceptions,
    MAX(CASE WHEN stat_name = 'def_pass_defended'        THEN value END) AS def_pass_defended,
    MAX(CASE WHEN stat_name = 'def_tackles'              THEN value END) AS def_tackles,
    MAX(CASE WHEN stat_name = 'def_tackles_for_loss'     THEN value END) AS def_tackles_for_loss,
    MAX(CASE WHEN stat_name = 'def_fumbles'              THEN value END) AS def_fumbles,
    MAX(CASE WHEN stat_name = 'def_fumbles_forced'       THEN value END) AS def_fumbles_forced,
    MAX(CASE WHEN stat_name = 'def_tds'                  THEN value END) AS def_tds,
    MAX(CASE WHEN stat_name = 'def_safety'               THEN value END) AS def_safety,

    -- Special Teams
    MAX(CASE WHEN stat_name = 'fg_att'                   THEN value END) AS fg_att,
    MAX(CASE WHEN stat_name = 'fg_made'                  THEN value END) AS fg_made,
    MAX(CASE WHEN stat_name = 'fg_blocked'               THEN value END) AS fg_blocked,
    MAX(CASE WHEN stat_name = 'pat_att'                  THEN value END) AS pat_att,
    MAX(CASE WHEN stat_name = 'pat_made'                 THEN value END) AS pat_made,
    MAX(CASE WHEN stat_name = 'pat_blocked'              THEN value END) AS pat_blocked,
    MAX(CASE WHEN stat_name = 'fg_made_0_19'             THEN value END) AS fg_made_0_19,
    MAX(CASE WHEN stat_name = 'fg_made_20_29'            THEN value END) AS fg_made_20_29,
    MAX(CASE WHEN stat_name = 'fg_made_30_39'            THEN value END) AS fg_made_30_39,
    MAX(CASE WHEN stat_name = 'fg_made_40_49'            THEN value END) AS fg_made_40_49,
    MAX(CASE WHEN stat_name = 'fg_made_50_59'            THEN value END) AS fg_made_50_59,
    MAX(CASE WHEN stat_name = 'fg_made_60'               THEN value END) AS fg_made_60
  FROM prod.team_weekly_tbl
  WHERE game_id = :game_id
    AND team IN ((SELECT home_team FROM g) UNION ALL (SELECT away_team FROM g))
  GROUP BY team
)
SELECT
  g.game_id, g.season, g.week, g.home_team, g.away_team, g.home_score, g.away_score,

  -- Home (prefix h_)
  h.attempts               AS h_attempts,
  h.completions            AS h_completions,
  h.passing_yards          AS h_passing_yards,
  h.passing_tds            AS h_passing_tds,
  h.interceptions          AS h_interceptions,
  h.passing_first_downs    AS h_passing_first_downs,
  h.sacks                  AS h_sacks,
  h.sack_yards             AS h_sack_yards,
  h.carries                AS h_carries,
  h.rushing_yards          AS h_rushing_yards,
  h.rushing_tds            AS h_rushing_tds,
  h.rushing_first_downs    AS h_rushing_first_downs,
  h.rushing_fumbles        AS h_rushing_fumbles,
  h.rushing_fumbles_lost   AS h_rushing_fumbles_lost,
  h.def_penalty            AS h_penalties,
  h.def_penalty_yards      AS h_penalty_yards,

  h.def_sacks              AS h_def_sacks,
  h.def_sack_yards         AS h_def_sack_yards,
  h.def_qb_hits            AS h_def_qb_hits,
  h.def_interceptions      AS h_def_interceptions,
  h.def_pass_defended      AS h_def_pass_defended,
  h.def_tackles            AS h_def_tackles,
  h.def_tackles_for_loss   AS h_def_tackles_for_loss,
  h.def_fumbles            AS h_def_fumbles,
  h.def_fumbles_forced     AS h_def_fumbles_forced,
  h.def_tds                AS h_def_tds,
  h.def_safety             AS h_def_safety,

  h.fg_att                 AS h_fg_att,
  h.fg_made                AS h_fg_made,
  h.fg_blocked             AS h_fg_blocked,
  h.pat_att                AS h_pat_att,
  h.pat_made               AS h_pat_made,
  h.pat_blocked            AS h_pat_blocked,
  h.fg_made_0_19           AS h_fg_made_0_19,
  h.fg_made_20_29          AS h_fg_made_20_29,
  h.fg_made_30_39          AS h_fg_made_30_39,
  h.fg_made_40_49          AS h_fg_made_40_49,
  h.fg_made_50_59          AS h_fg_made_50_59,
  h.fg_made_60             AS h_fg_made_60,

  -- Away (prefix a_)
  a.attempts               AS a_attempts,
  a.completions            AS a_completions,
  a.passing_yards          AS a_passing_yards,
  a.passing_tds            AS a_passing_tds,
  a.interceptions          AS a_interceptions,
  a.passing_first_downs    AS a_passing_first_downs,
  a.sacks                  AS a_sacks,
  a.sack_yards             AS a_sack_yards,
  a.carries                AS a_carries,
  a.rushing_yards          AS a_rushing_yards,
  a.rushing_tds            AS a_rushing_tds,
  a.rushing_first_downs    AS a_rushing_first_downs,
  a.rushing_fumbles        AS a_rushing_fumbles,
  a.rushing_fumbles_lost   AS a_rushing_fumbles_lost,
  a.def_penalty            AS a_penalties,
  a.def_penalty_yards      AS a_penalty_yards,

  a.def_sacks              AS a_def_sacks,
  a.def_sack_yards         AS a_def_sack_yards,
  a.def_qb_hits            AS a_def_qb_hits,
  a.def_interceptions      AS a_def_interceptions,
  a.def_pass_defended      AS a_def_pass_defended,
  a.def_tackles            AS a_def_tackles,
  a.def_tackles_for_loss   AS a_def_tackles_for_loss,
  a.def_fumbles            AS a_def_fumbles,
  a.def_fumbles_forced     AS a_def_fumbles_forced,
  a.def_tds                AS a_def_tds,
  a.def_safety             AS a_def_safety,

  a.fg_att                 AS a_fg_att,
  a.fg_made                AS a_fg_made,
  a.fg_blocked             AS a_fg_blocked,
  a.pat_att                AS a_pat_att,
  a.pat_made               AS a_pat_made,
  a.pat_blocked            AS a_pat_blocked,
  a.fg_made_0_19           AS a_fg_made_0_19,
  a.fg_made_20_29          AS a_fg_made_20_29,
  a.fg_made_30_39          AS a_fg_made_30_39,
  a.fg_made_40_49          AS a_fg_made_40_49,
  a.fg_made_50_59          AS a_fg_made_50_59,
  a.fg_made_60             AS a_fg_made_60

FROM g
LEFT JOIN agg h ON h.team = g.home_team
LEFT JOIN agg a ON a.team = g.away_team
"""

@router.get("/{season}/{week}/{game_id}/stats")
async def get_game_stats(season: int, week: int, game_id: str):
    """
    Game stats for Offense / Defense / Special Teams (rows) with Home/Away values (columns).

    Returns:
        {
          "game_id": str, "season": int, "week": int,
          "home_team": str, "away_team": str,
          "offense": [ { "metric": str, "home": number|null, "away": number|null }, ... ],
          "defense": [ ... ],
          "special": [ ... ]
        }
    """
    async with AsyncSessionLocal() as session:
        res = await session.execute(text(STATS_QUERY), {"season": season, "week": week, "game_id": game_id})
        row = res.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Game not found for given season/week")

        r = dict(row)

        # Helpers
        def n(x):  # pass through numbers, keep None as None
            return None if x is None else x

        # --- Offense (Totals + Passing + Rushing) ---
        h_total_yards = (r.get("h_passing_yards") or 0) + (r.get("h_rushing_yards") or 0)
        a_total_yards = (r.get("a_passing_yards") or 0) + (r.get("a_rushing_yards") or 0)

        h_total_fd = (r.get("h_passing_first_downs") or 0) + (r.get("h_rushing_first_downs") or 0)
        a_total_fd = (r.get("a_passing_first_downs") or 0) + (r.get("a_rushing_first_downs") or 0)

        h_total_tds = (r.get("h_passing_tds") or 0) + (r.get("h_rushing_tds") or 0)
        a_total_tds = (r.get("a_passing_tds") or 0) + (r.get("a_rushing_tds") or 0)

        h_turnovers = (r.get("h_interceptions") or 0) + (r.get("h_rushing_fumbles_lost") or 0)
        a_turnovers = (r.get("a_interceptions") or 0) + (r.get("a_rushing_fumbles_lost") or 0)

        offense = [
            {"metric": "Total Yards",           "home": n(h_total_yards), "away": n(a_total_yards)},
            {"metric": "Total First Downs",     "home": n(h_total_fd),    "away": n(a_total_fd)},
            {"metric": "Total TDs (Offense)",   "home": n(h_total_tds),   "away": n(a_total_tds)},
            {"metric": "Total Turnovers (Off.)","home": n(h_turnovers),   "away": n(a_turnovers)},
            {"metric": "Total Penalties",       "home": n(r.get("h_penalties")),      "away": n(r.get("a_penalties"))},
            {"metric": "Total Penalty Yards",   "home": n(r.get("h_penalty_yards")),  "away": n(r.get("a_penalty_yards"))},

            {"metric": "Pass Attempts",         "home": n(r.get("h_attempts")),             "away": n(r.get("a_attempts"))},
            {"metric": "Pass Completions",      "home": n(r.get("h_completions")),          "away": n(r.get("a_completions"))},
            {"metric": "Passing Yards",         "home": n(r.get("h_passing_yards")),        "away": n(r.get("a_passing_yards"))},
            {"metric": "Passing TDs",           "home": n(r.get("h_passing_tds")),          "away": n(r.get("a_passing_tds"))},
            {"metric": "Interceptions",         "home": n(r.get("h_interceptions")),        "away": n(r.get("a_interceptions"))},
            {"metric": "Passing 1st Downs",     "home": n(r.get("h_passing_first_downs")),  "away": n(r.get("a_passing_first_downs"))},
            {"metric": "Sacks Taken",           "home": n(r.get("h_sacks")),                "away": n(r.get("a_sacks"))},
            {"metric": "Sack Yards Lost",       "home": n(r.get("h_sack_yards")),           "away": n(r.get("a_sack_yards"))},

            {"metric": "Carries",               "home": n(r.get("h_carries")),              "away": n(r.get("a_carries"))},
            {"metric": "Rushing Yards",         "home": n(r.get("h_rushing_yards")),        "away": n(r.get("a_rushing_yards"))},
            {"metric": "Rushing TDs",           "home": n(r.get("h_rushing_tds")),          "away": n(r.get("a_rushing_tds"))},
            {"metric": "Rushing 1st Downs",     "home": n(r.get("h_rushing_first_downs")),  "away": n(r.get("a_rushing_first_downs"))},
            {"metric": "Rushing Fumbles",       "home": n(r.get("h_rushing_fumbles")),      "away": n(r.get("a_rushing_fumbles"))},
            {"metric": "Rushing Fumbles Lost",  "home": n(r.get("h_rushing_fumbles_lost")), "away": n(r.get("a_rushing_fumbles_lost"))},
        ]

        # --- Defense ---
        defense = [
            {"metric": "Def. Sacks",             "home": n(r.get("h_def_sacks")),               "away": n(r.get("a_def_sacks"))},
            {"metric": "QB Hits",                "home": n(r.get("h_def_qb_hits")),             "away": n(r.get("a_def_qb_hits"))},
            {"metric": "Interceptions",          "home": n(r.get("h_def_interceptions")),       "away": n(r.get("a_def_interceptions"))},
            {"metric": "Passes Defended",        "home": n(r.get("h_def_pass_defended")),       "away": n(r.get("a_def_pass_defended"))},
            {"metric": "Tackles (Total)",        "home": n(r.get("h_def_tackles")),             "away": n(r.get("a_def_tackles"))},
            {"metric": "TFL",                    "home": n(r.get("h_def_tackles_for_loss")),    "away": n(r.get("a_def_tackles_for_loss"))},
            {"metric": "Forced Fumbles",         "home": n(r.get("h_def_fumbles_forced")),      "away": n(r.get("a_def_fumbles_forced"))},
            {"metric": "Def. Fumbles",           "home": n(r.get("h_def_fumbles")),             "away": n(r.get("a_def_fumbles"))},
            {"metric": "Def. TDs",               "home": n(r.get("h_def_tds")),                 "away": n(r.get("a_def_tds"))},
            {"metric": "Safeties",               "home": n(r.get("h_def_safety")),              "away": n(r.get("a_def_safety"))},
            {"metric": "Penalties",              "home": n(r.get("h_penalties")),               "away": n(r.get("a_penalties"))},
            {"metric": "Penalty Yards",          "home": n(r.get("h_penalty_yards")),           "away": n(r.get("a_penalty_yards"))},
        ]

        # --- Special Teams ---
        special = [
            {"metric": "FG Att",                  "home": n(r.get("h_fg_att")),               "away": n(r.get("a_fg_att"))},
            {"metric": "FG Made",                 "home": n(r.get("h_fg_made")),              "away": n(r.get("a_fg_made"))},
            {"metric": "FG Blocked",              "home": n(r.get("h_fg_blocked")),           "away": n(r.get("a_fg_blocked"))},
            
            {"metric": "PAT Att",                 "home": n(r.get("h_pat_att")),              "away": n(r.get("a_pat_att"))},
            {"metric": "PAT Made",                "home": n(r.get("h_pat_made")),             "away": n(r.get("a_pat_made"))},
            {"metric": "PAT Blocked",             "home": n(r.get("h_pat_blocked")),          "away": n(r.get("a_pat_blocked"))},

            {"metric": "FG Made 0-19",            "home": n(r.get("h_fg_made_0_19")),         "away": n(r.get("a_fg_made_0_19"))},
            {"metric": "FG Made 20-29",           "home": n(r.get("h_fg_made_20_29")),        "away": n(r.get("a_fg_made_20_29"))},
            {"metric": "FG Made 30-39",           "home": n(r.get("h_fg_made_30_39")),        "away": n(r.get("a_fg_made_30_39"))},
            {"metric": "FG Made 40-49",           "home": n(r.get("h_fg_made_40_49")),        "away": n(r.get("a_fg_made_40_49"))},
            {"metric": "FG Made 50-59",           "home": n(r.get("h_fg_made_50_59")),        "away": n(r.get("a_fg_made_50_59"))},
            {"metric": "FG Made 60+",             "home": n(r.get("h_fg_made_60")),           "away": n(r.get("a_fg_made_60"))},
        ]

        return {
            "game_id": r["game_id"],
            "season": r["season"],
            "week": r["week"],
            "home_team": r["home_team"],
            "away_team": r["away_team"],
            "offense": offense,
            "defense": defense,
            "special": special,
        }

