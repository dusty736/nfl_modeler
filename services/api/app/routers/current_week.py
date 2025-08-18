from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/api", tags=["current_week"])

@router.get("/season-week")
async def get_current_season_week():
    """
    Return the current season and week where the game has no result.
    """
    sql = text("""
        SELECT MIN(season) AS season, MIN(week) AS week
        FROM public.games_tbl
        WHERE result IS NULL
    """)
    async with AsyncSessionLocal() as session:
        row = (await session.execute(sql)).mappings().first()
        if not row or not row["season"] or not row["week"]:
            return {"error": "No upcoming games found."}
        return {
            "season": row["season"],
            "week": row["week"]
        }

@router.get("/max-week/{season}")
async def get_max_week(season: int):
    """
    Return the maximum completed week for a given season.
    """
    sql = text("""
        SELECT MAX(week) AS max_week
        FROM public.schedule_tbl
        WHERE season = :season
    """)
    async with AsyncSessionLocal() as session:
        row = (await session.execute(sql, {"season": season})).mappings().first()
        return {"max_week": row["max_week"] if row and row["max_week"] else 18}
