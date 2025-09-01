"""
Current Week Router
-------------------
Utility endpoints for determining the current (season, week) context and
the latest completed week, globally or by team.

Base path: /api
Tags: ["current_week"]

Notes
-----
- 'Current' uses unplayed games (result IS NULL) to infer the next week.
- MIN(season) and MIN(week) are computed independently by design here.
  If multiple seasons with NULL results coexist, this may span seasons;
  we are NOT changing that logic today—just documenting it.
- Fallbacks for max-week endpoints default to 18 if no data exist.
"""

from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

# --- Router setup -------------------------------------------------------------
router = APIRouter(prefix="/api", tags=["current_week"])

@router.get("/season-week")
async def get_current_season_week():
    """Return the 'current' season and week inferred from unplayed games.

    Definition:
        - Current = MIN(season), MIN(week) over rows where result IS NULL in games_tbl.
        - MIN(season) and MIN(week) are independent aggregates (documented behaviour).

    Returns:
        {"season": int, "week": int} or {"error": "..."} if none found.
    """
    sql = text("""
        SELECT MIN(season) AS season, MIN(week) AS week
        FROM prod.games_tbl
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

@router.get("/max-week-team/{season}/{team}")
async def get_max_week_team(season: int, team: str):
    """Return the maximum completed week for a given season and team.

    Args:
        season: NFL season (e.g., 2024).
        team: Team abbreviation (case-insensitive), uppercased server-side.

    Returns:
        {"max_week": int} where absent data falls back to 18 (documented default).
    """
    sql = text("""
        SELECT MAX(week) AS max_week
        FROM prod.weekly_results_tbl
        WHERE season = :season
          AND team_id = :team
    """)
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(
                sql,
                {"season": season, "team": team.upper()}
            )
        ).mappings().first()
        return {"max_week": row["max_week"] if row and row["max_week"] else 18}


@router.get("/max-week/{season}")
async def get_max_week(season: int):
    """Return the maximum completed week for a given season (any team).

    Args:
        season: NFL season (e.g., 2024).

    Returns:
        {"max_week": int} with a fallback to 18 when no rows are present.
    """
    sql = text("""
        SELECT MAX(week) AS max_week
        FROM prod.weekly_results_tbl
        WHERE season = :season
    """)
    async with AsyncSessionLocal() as session:
        row = (await session.execute(sql, {"season": season})).mappings().first()
        return {"max_week": row["max_week"] if row and row["max_week"] else 18}
