from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/api", tags=["primetime"])

@router.get("/primetime-games")
async def get_primetime_games():
    """
    Returns all primetime games for the current season/week.
    Primetime = any non-Sunday, or Sunday 18:00+ (Europe/London).
    """
    # Step 1: Determine current season and week
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            SELECT MIN(season) AS season, MIN(week) AS week
            FROM public.games_tbl
            WHERE result IS NULL
        """))
        current = result.mappings().first()
        season, week = current["season"], current["week"]

        if not season or not week:
            return {"error": "No upcoming games found."}

        # Step 2: Select primetime games
        query = text("""
            SELECT *
            FROM public.games_tbl
            WHERE season = :season
              AND week = :week
              AND (
                EXTRACT(DOW FROM kickoff AT TIME ZONE 'Europe/London') != 0
                OR EXTRACT(HOUR FROM kickoff AT TIME ZONE 'Europe/London') >= 18
              )
            ORDER BY kickoff
        """)

        games = (await session.execute(query, {"season": season, "week": week})).mappings().all()

        return {
            "season": season,
            "week": week,
            "games": list(games)
        }
