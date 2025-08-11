import os
from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

# define the router FIRST
router = APIRouter(prefix="/api", tags=["standings"])

SEASON = int(os.getenv("SEASON", "2025"))

@router.get("/ping")
async def ping():
    return {"ok": True, "service": "standings"}

@router.get("/dbcheck")
async def dbcheck():
    async with AsyncSessionLocal() as s:
        row = (await s.execute(text("SELECT 1 AS ok"))).mappings().first()
    return {"db_ok": bool(row["ok"])}

@router.get("/standings")
async def get_standings():
    """
    Your proven join:
    team_metadata_tbl LEFT JOIN season_results_tbl ON team_abbr = team_id AND season = :season
    """
    sql = text("""
        SELECT
            COALESCE(srt.team_id, tmt.team_abbr) AS team_id,
            tmt.team_name                         AS team_name,
            tmt.team_division                     AS division,
            tmt.team_color                        AS team_color,
            tmt.team_color2                       AS team_color2,
            COALESCE(srt.wins, 0)                 AS wins,
            COALESCE(srt.losses, 0)               AS losses,
            COALESCE(srt.ties, 0)                 AS ties,
            COALESCE(srt.points_for, 0)           AS points_for,
            COALESCE(srt.points_against, 0)       AS points_against,
            COALESCE(srt.point_diff, 0)           AS point_diff
        FROM public.team_metadata_tbl tmt
        INNER JOIN public.season_results_tbl srt
               ON tmt.team_abbr = srt.team_id
              AND srt.season = :season
        ORDER BY tmt.team_division, wins DESC, point_diff DESC, team_id;
    """)
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(sql, {"season": SEASON})).mappings().all()
    return {"season": SEASON, "items": rows}


