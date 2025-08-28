"""
Team Injuries Router
--------------------
Aggregated team injury summaries and per-player injury listings.

Base path: /team_injuries
Tags: ["team_injuries"]

Notes
-----
- Position mapping collapses roles into groups:
  TOTAL; QB; RB(FB); WR; TE; OL(C,G,T,OL); DL(DT,DE,NT,DL); LB(LB,OLB,ILB,MLB);
  DB(CB,DB,S,SS,FS); ST(K,P,LS,KR,PR,SPEC); OTHER otherwise.
- Responses return lists of dicts; on empty, this router intentionally returns {"error": "..."}
  (not HTTP 4xx) to match existing client behaviour.
- No functional changes — documentation and comments only.
"""

from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

# --- Router setup -------------------------------------------------------------
router = APIRouter(prefix="/team_injuries", tags=["team_injuries"])

@router.get("/{team}/injuries/team/{season}/{week}/{position}")
async def get_team_injury_summary(team: str, season: int, week: int, position: str):
    """Return team injury counts (weekly and season-to-date) for a position group.

    Args:
        team: Team abbreviation (case-insensitive), uppercased server-side.
        season: NFL season (e.g., 2024).
        week: Target week (filters both position/week tables).
        position: One of the mapped groups (e.g., QB, RB, WR, DL, LB, DB, ST, TEAM/TOTAL).

    Shape:
        A list of mappings with: week, position, weekly_injuries, season_injuries.
        UNION logic combines:
          - position-level counts from injuries_position_weekly_tbl (mapped to groups), and
          - team-level totals from injuries_team_weekly_tbl as position='TOTAL'.
        DISTINCT isn’t needed; GROUP BY + sort_order keeps the two sources clear.

    Notes:
        - Ordering by week, position, sort_order; client can re-order as needed.
        - Returns {"error":"Bad query"} if no rows (kept for client compatibility).
    """
    query = """
        select
          q.week, 
          q.position,
          sum(q.weekly_injuries) as weekly_injuries, 
          sum(q.season_injuries) as season_injuries
        from
        (select 
        	team,
        	season,
        	week, 
        	CASE 
                    WHEN ipwt.position IN ('TOTAL') THEN 'TOTAL'
                    WHEN ipwt.position IN ('QB') THEN 'QB'
                    WHEN ipwt.position IN ('RB','FB') THEN 'RB'
                    WHEN ipwt.position IN ('WR') THEN 'WR'
                    WHEN ipwt.position IN ('TE') THEN 'TE'
                    WHEN ipwt.position IN ('C','G','T','OL') THEN 'OL'
                    WHEN ipwt.position IN ('DT','DE','NT','DL') THEN 'DL'
                    WHEN ipwt.position IN ('LB','OLB','ILB','MLB') THEN 'LB'
                    WHEN ipwt.position IN ('CB','DB','S','SS','FS') THEN 'DB'
                    WHEN ipwt.position IN ('K','P','LS','KR','PR','SPEC') THEN 'ST'
                    ELSE 'OTHER'
                END AS position, 
        	position_injuries as weekly_injuries, 
        	cumulative_position_injuries as season_injuries,
        	1 as sort_order
        from 
        	public.injuries_position_weekly_tbl ipwt 
        union ALL
        select 
        	team,
        	season,
        	week, 
        	'TOTAL' as position, 
        	weekly_injuries, 
        	cumulative_injuries as season_injuries,
        	2 as sort_order
        from 
        	public.injuries_team_weekly_tbl itwt) q
        WHERE q.season = :season
          AND q.week = :week
          AND q.team = :team
          AND UPPER(q.position) = :position
        group by
        q.week, 
        q.position,
        q.sort_order
        order by q.week, q.position, q.sort_order;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"team": team.upper(), "season": season, 
                          "week": week, "position": position.upper()}
        )
        row = result.mappings().all()
        if not row:
            return {"error": "Bad query"}
        return [dict(r) for r in row]

@router.get("/{team}/injuries/player/{season}/{week}/{position}")
async def get_player_injuries(team: str, season: int, week: int, position: str):
    """Return per-player injury detail for a position group, team, and week.

    Args:
        team: Team abbreviation (case-insensitive), uppercased server-side.
        season: NFL season.
        week: Week number.
        position: Mapped position group (QB, RB, WR, TE, OL, DL, LB, DB, ST, TOTAL/TEAM).

    Output columns:
        name, position (mapped), report_status, practice_status, injury_reported, did_not_practice.

    Notes:
        - Mapping mirrors the team summary endpoint to ensure grouping parity.
        - Ordered by position then name for stable UI rendering.
        - Returns {"error":"Bad query"} if no rows found (kept as-is).
    """
    query = """
        select 
        	name, position, report_status, practice_status, injury_reported, did_not_practice
        from (
        	select
        	full_name as name,
        	CASE 
              WHEN position IN ('TOTAL') THEN 'TOTAL'
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
        	report_status,
        	practice_status,
        	injury_reported,
        	did_not_practice,
        	season, 
        	team, 
        	week
        from public.injuries_weekly_tbl iwt) q
        where season = :season
        and team = :team
        and week = :week
        and UPPER(position) = :position
        order by position, name;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"team": team.upper(), "season": season, 
                          "week": week, "position": position.upper()}
        )
        row = result.mappings().all()
        if not row:
            return {"error": "Bad query"}
        return [dict(r) for r in row]
