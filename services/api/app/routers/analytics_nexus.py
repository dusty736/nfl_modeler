from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/analytics_nexus", tags=["analytics_nexus"])

@router.get("/player/trajectories/{season}/{season_type}/{stat_name}/{position}/{top_n}")
async def get_player_weekly_trajectories(
    season: int,
    season_type: str,
    stat_name: str,
    position: str,
    top_n: int,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base"
):
    """
    Return Top-N player weekly stat trajectories (optionally cumulative).
    """
    query = """
    WITH top_players AS (
        SELECT player_id
        FROM public.player_weekly_tbl pwt
        WHERE season = :season
          AND season_type = :season_type
          AND stat_name = :stat_name
          AND stat_type = :stat_type
          AND (:position IS NULL OR position = :position)
        GROUP BY player_id
        ORDER BY SUM(value) DESC
        LIMIT :top_n
    )
    SELECT pwt.player_id,
           pwt.name,
           pwt.team,
           pwt.season,
           pwt.season_type,
           pwt.week,
           pwt.position,
           pwt.stat_name,
           pwt.value,
           tmt.team_color,
           tmt.team_color2
    FROM public.player_weekly_tbl pwt
    LEFT JOIN public.team_metadata_tbl tmt
           ON pwt.team = tmt.team_abbr
    WHERE season = :season
      AND season_type = :season_type
      AND stat_name = :stat_name
      AND stat_type = :stat_type
      AND (:position IS NULL OR position = :position)
      AND week BETWEEN :week_start AND :week_end
      AND pwt.player_id IN (SELECT player_id FROM top_players)
    ORDER BY pwt.player_id, pwt.week;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query),
            {
                "season": season,
                "season_type": season_type.upper(),
                "stat_name": stat_name,
                "stat_type": stat_type,
                "position": None if position.upper() == "ALL" else position.upper(),
                "top_n": top_n,
                "week_start": week_start,
                "week_end": week_end,
            }
        )
        rows = result.mappings().all()
        if not rows:
            return {"error": "No data found"}
        return [dict(r) for r in rows]



