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

# --- Router setup -------------------------------------------------------------
router = APIRouter(prefix="/team_rosters", tags=["team_rosters"])

@router.get("/{team_abbr}/{season}")
async def get_team_roster(team_abbr: str, season: int):
    """Return the full roster for a team in a given season.

    Returns rows with: season, team (abbr), name, position, status, age, weight, height,
    college, years_exp, rookie_year.

    Ordering:
        position ASC, then name ASC.
    """
    query = """
        select 
        	rt.season, 
        	rt.team_id as team,
        	rt.full_name as name,
        	rt.position,
        	rt.status,
        	rt.age,
        	rt.weight,
        	rt.height,
        	rt.college,
        	rt.years_exp,
        	rt.rookie_year
        from prod.rosters_tbl rt
        WHERE rt.season = :season
          AND rt.team_id = :team_abbr
        order by position, name;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"team_abbr": team_abbr.upper(), "season": season}
        )
        row = result.mappings().all()
        if not row:
            return {"error": "Bad query"}
        return [dict(r) for r in row]

@router.get("/{team_abbr}/{season}/positions/{position}")
async def get_team_position_summary(team_abbr: str, season: int, position: str):
    """Return summary metrics for a position group (and TEAM aggregate) for a team/season.

    Position grouping rules (CASE mapping):
        TEAM -> TEAM
        QB -> QB
        RB, FB -> RB
        WR -> WR
        TE -> TE
        C, G, T, OL -> OL
        DT, DE, NT, DL -> DL
        LB, OLB, ILB, MLB -> LB
        CB, DB, S, SS, FS -> DB
        K, P, LS, KR, PR, SPEC -> ST
        everything else -> OTHER

    Output columns:
        season, team, position (mapped), average_age, average_height, average_weight,
        average_exp, position_group_score

    Notes:
        - DISTINCT ON enforces one row per (season, team, position).
        - The subquery unions position-level and TEAM-level aggregates, then filters to requested position.
    """
    query = """
        SELECT DISTINCT ON (season, team, position)
               season,
               team,
               CASE 
                    WHEN position IN ('TEAM') THEN 'TEAM'
                    WHEN position IN ('QB') THEN 'QB'
                    WHEN position IN ('RB','FB') THEN 'RB'
                    WHEN position IN ('WR') THEN 'WR'
                    WHEN position IN ('TE') THEN 'TE'
                    WHEN position IN ('C','G','T','OL') THEN 'OL'
                    WHEN position IN ('DT','DE','NT','DL') THEN 'DL'
                    WHEN position IN ('LB','OLB','ILB','MLB') THEN 'LB'
                    WHEN position IN ('CB','DB','S','SS','FS') THEN 'DB'
                    WHEN position IN ('K','P','LS','KR','PR','SPEC') THEN 'ST'
                    ELSE 'OTHER'
                END AS position,
               average_age,
               average_height,
               average_weight,
               average_exp,
               position_group_score
        FROM (
            select 
                season, team, position,
                round(avg_age::numeric, 2)    as average_age,
                round(avg_height::numeric, 2) as average_height,
                round(avg_weight::numeric, 2) as average_weight,
                round(avg_exp::numeric, 2)    as average_exp,
                dcpst.position_group_score,
                1 as sort_order
            from prod.roster_position_summary_tbl rpst
            left join prod.depth_charts_position_stability_tbl dcpst 
                   using(season, team, position)
        
            union all
        
            select 
                season, team, 'TEAM' as position,
                round(avg_age::numeric, 2)    as average_age,
                round(avg_height::numeric, 2) as average_height,
                round(avg_weight::numeric, 2) as average_weight,
                round(avg_exp::numeric, 2)    as average_exp,
                round(t.avg_position_group_score::numeric, 2) as position_group_score,
                2 as sort_order
            from prod.roster_summary_tbl rst
            left join (
                select season, team, avg(position_group_score) as avg_position_group_score
                from prod.depth_charts_position_stability_tbl
                group by season, team
            ) t using(season, team)
        ) q
        WHERE q.season = :season
          AND q.team = :team_abbr
          AND q.position = :position
        ORDER BY season, team, position, sort_order;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"team_abbr": team_abbr.upper(), "season": season, "position": position.upper()}
        )
        row = result.mappings().all()
        if not row:
            return {"error": "Bad query"}
        return [dict(r) for r in row]

@router.get("/{team_abbr}/{season}/weeks/{week}")
async def get_team_depth_chart_starters(team_abbr: str, season: int, week: int):
    """Return weekly depth-chart starters for a team (per position group).

    Output columns:
        season, week, team, player, position, position_group, starts, new_starter

    Notes:
        - Table: depth_charts_starters_tbl
        - One row per (team, week, player, position_group) as produced upstream.
    """
    query = """
        select 
            season, 
            week,
            team,
            player,
            position,
            position_group,
            player_starts as starts,
            is_new_starter as new_starter
        from depth_charts_starters_tbl dcst
        WHERE dcst.season = :season
          AND dcst.team = :team_abbr
          AND dcst.week = :week;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"team_abbr": team_abbr.upper(), "season": season, "week": week}
        )
        row = result.mappings().all()
        if not row:
            return {"error": "No data found"}
        return [dict(r) for r in row]

