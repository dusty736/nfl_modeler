from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/api", tags=["season"])

@router.get("/ping")
async def ping():
    return {"ok": True, "service": "season"}

@router.get("/dbcheck")
async def dbcheck():
    async with AsyncSessionLocal() as s:
        row = (await s.execute(text("SELECT 1 AS ok"))).mappings().first()
    return {"db_ok": bool(row["ok"])}

@router.get("/standings")
async def standings():
    sql = text("""
        SELECT team_id, team_name, division, wins, losses, ties,
               points_for, points_against,
               (points_for - points_against) AS point_diff
        FROM nfl.vw_team_season_summary_2025
        ORDER BY division, wins DESC, point_diff DESC
    """)
    async with AsyncSessionLocal() as s:
        rows = (await s.execute(sql)).mappings().all()
    return {"items": rows}
