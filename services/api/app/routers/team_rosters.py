from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/team_rosters", tags=["team_rosters"])

@router.get("/{team_abbr}/{season}")
async def get_team_roster(team_abbr: str, season: int):
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
        from public.rosters_tbl rt
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
            from public.roster_position_summary_tbl rpst
            left join public.depth_charts_position_stability_tbl dcpst 
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
            from public.roster_summary_tbl rst
            left join (
                select season, team, avg(position_group_score) as avg_position_group_score
                from public.depth_charts_position_stability_tbl
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

