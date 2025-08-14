from fastapi import APIRouter, HTTPException
import os
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/teams", tags=["teams"])

@router.get("/")
async def get_all_teams():
    query = """
        SELECT team_name, team_abbr, team_division
        FROM public.team_metadata_tbl
        WHERE team_abbr not in ('OAK', 'STL', 'SD')
        ORDER BY team_division, team_name;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(query))
        rows = result.mappings().all()
        return list(rows)

@router.get("/{team_abbr}")
async def get_team_by_abbr(team_abbr: str):
    query = """
        SELECT team_name, team_abbr, team_division
        FROM public.team_metadata_tbl
        WHERE team_abbr = :abbr;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(query), {"abbr": team_abbr.upper()})
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Team not found")
        return row

