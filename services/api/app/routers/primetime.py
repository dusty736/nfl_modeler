"""
Primetime Router
----------------
Endpoints for retrieving primetime NFL games for the next unplayed week.

Base path: /api
Tags: ["primetime"]

Definition of "primetime":
- Europe/London timezone
- Any kickoff not on Sunday (DOW != 0 in Postgres), OR
- Sunday kickoffs at or after 18:00 local time.

Week selection:
- Chooses the earliest (MIN) season/week from games with NULL result (i.e., unplayed).
- Assumes games_tbl reflects the current competition context; if historical/future seasons coexist,
  MIN(...) should still resolve to the *next* unplayed week in practice.

No functional changes are introduced; this file only adds documentation and comments.
"""

from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

# --- Router setup -------------------------------------------------------------
router = APIRouter(prefix="/api", tags=["primetime"])

@router.get("/primetime-games")
async def get_primetime_games():
    """Return all primetime games for the next unplayed (season, week).

    Primetime rule (Europe/London):
    - Non-Sunday kickoffs, OR
    - Sunday kickoff time >= 18:00.

    Notes
    -----
    - Week detection uses MIN(season), MIN(week) where result IS NULL (unplayed).
    - Sorting is by kickoff ascending.
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
