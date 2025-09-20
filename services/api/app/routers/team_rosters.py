"""
Team Rosters Router
-------------------
Lookups for team rosters, position-group summaries, and weekly depth-chart starters.

Base path: /team_rosters
Tags: ["team_rosters"]

Notes
-----
- Response shapes: all endpoints return lists of mappings (converted to dicts for JSON).
- Case handling: team and position inputs are uppercased server-side.
- Errors: this router returns simple {"error": "..."} payloads instead of raising HTTP 4xx,
  by design to keep client handling consistent with existing code.
- No functional changes here — documentation and comments only.
"""
from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/team_rosters", tags=["team_rosters"])

@router.get("/{team_abbr}/{season}")
async def get_team_roster(team_abbr: str, season: int):
    """
    Full roster for a team/season.
    Columns: season, team, name, position, status, age, weight, height, college, years_exp, rookie_year
    """
    query = """
        SELECT 
            rt.season, 
            rt.team       AS team,           -- FIX: team (not team_id)
            rt.full_name  AS name,
            rt.position,
            rt.status,
            rt.age,
            rt.weight,
            rt.height,
            rt.college,
            rt.years_exp,
            rt.rookie_year
        FROM prod.rosters_tbl rt
        WHERE rt.season = :season
          AND rt.team   = :team_abbr
        ORDER BY rt.position NULLS LAST, rt.full_name;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"team_abbr": team_abbr.upper(), "season": season}
        )
        rows = result.mappings().all()
        if not rows:
            return {"error": "No roster found"}
        return [dict(r) for r in rows]

@router.get("/{team_abbr}/{season}/positions/{position}")
async def get_team_position_summary(team_abbr: str, season: int, position: str):
    """
    Position-group summary for a team/season.
    Filters on the *mapped* position group (TEAM/QB/RB/WR/TE/OL/DL/LB/DB/ST/OTHER).
    """
    query = """
        WITH base AS (
            SELECT 
                rpst.season,
                rpst.team,
                -- FIX: map inside subquery so we can filter on it
                CASE 
                    WHEN rpst.position IN ('TEAM') THEN 'TEAM'
                    WHEN rpst.position IN ('QB') THEN 'QB'
                    WHEN rpst.position IN ('RB','FB') THEN 'RB'
                    WHEN rpst.position IN ('WR') THEN 'WR'
                    WHEN rpst.position IN ('TE') THEN 'TE'
                    WHEN rpst.position IN ('C','G','T','OL') THEN 'OL'
                    WHEN rpst.position IN ('DT','DE','NT','DL') THEN 'DL'
                    WHEN rpst.position IN ('LB','OLB','ILB','MLB') THEN 'LB'
                    WHEN rpst.position IN ('CB','DB','S','SS','FS') THEN 'DB'
                    WHEN rpst.position IN ('K','P','LS','KR','PR','SPEC') THEN 'ST'
                    ELSE 'OTHER'
                END AS mapped_position,
                ROUND(rpst.avg_age::numeric, 2)    AS average_age,
                ROUND(rpst.avg_height::numeric, 2) AS average_height,
                ROUND(rpst.avg_weight::numeric, 2) AS average_weight,
                ROUND(rpst.avg_exp::numeric, 2)    AS average_exp,
                dcpst.position_group_score,
                1 AS sort_order
            FROM prod.roster_position_summary_tbl rpst
            LEFT JOIN prod.depth_charts_position_stability_tbl dcpst 
                   ON dcpst.season = rpst.season
                  AND dcpst.team   = rpst.team
                  AND dcpst.position = rpst.position

            UNION ALL

            SELECT
                rst.season,
                rst.team,
                'TEAM' AS mapped_position,
                ROUND(rst.avg_age::numeric, 2)    AS average_age,
                ROUND(rst.avg_height::numeric, 2) AS average_height,
                ROUND(rst.avg_weight::numeric, 2) AS average_weight,
                ROUND(rst.avg_exp::numeric, 2)    AS average_exp,
                ROUND(t.avg_position_group_score::numeric, 2) AS position_group_score,
                2 AS sort_order
            FROM prod.roster_summary_tbl rst
            LEFT JOIN (
                SELECT season, team, AVG(position_group_score) AS avg_position_group_score
                FROM prod.depth_charts_position_stability_tbl
                GROUP BY season, team
            ) t
            ON t.season = rst.season AND t.team = rst.team
        )
        SELECT DISTINCT ON (season, team, mapped_position)
               season,
               team,
               mapped_position AS position,
               average_age,
               average_height,
               average_weight,
               average_exp,
               position_group_score
        FROM base
        WHERE season = :season
          AND team   = :team_abbr
          AND mapped_position = :position   -- FIX: filter on mapped group
        ORDER BY season, team, mapped_position, sort_order;
    """
    async with AsyncSessionLocal() as session:
        params = {
            "team_abbr": team_abbr.upper(),
            "season": season,
            "position": position.upper(),
        }
        result = await session.execute(text(query), params)
        rows = result.mappings().all()
        if not rows:
            return {"error": "No position summary found"}
        return [dict(r) for r in rows]

@router.get("/{team_abbr}/{season}/weeks/{week}")
async def get_team_depth_chart_starters(team_abbr: str, season: int, week: int):
    """
    Weekly depth-chart starters for a team.
    Columns: season, week, team, player, position, position_group, starts, new_starter
    """
    query = """
        SELECT 
            dcst.season, 
            dcst.week,
            dcst.team,
            dcst.player,
            dcst.position,
            dcst.position_group,
            dcst.player_starts    AS starts,
            dcst.is_new_starter   AS new_starter
        FROM prod.depth_charts_starters_tbl dcst   -- FIX: qualify schema
        WHERE dcst.season = :season
          AND dcst.team   = :team_abbr
          AND dcst.week   = :week
        ORDER BY dcst.position_group, dcst.player;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query),
            {"team_abbr": team_abbr.upper(), "season": season, "week": week},
        )
        rows = result.mappings().all()
        if not rows:
            return {"error": "No depth chart starters found"}
        return [dict(r) for r in rows]
