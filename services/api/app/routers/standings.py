"""
Standings Router
----------------
Read-only FastAPI endpoints for team standings and simple DB liveness checks.

Base path: /api
Tags: ["standings"]

Environment:
- SEASON (int, default 2025): The season used to filter standings queries.

Notes:
- No functional changes beyond documentation and comments.
- Join type: currently INNER JOIN to season_results_tbl (requires a row for the given season).
  If you intended to show teams without a season row, you'd use LEFT JOIN instead; we are NOT changing it.
"""

import os
from fastapi import APIRouter
from sqlalchemy import text
from app.db import AsyncSessionLocal

# --- Router setup & globals ---------------------------------------------------
# Keep prefix consistent with other routers. Do not alter.

router = APIRouter(prefix="/api", tags=["standings"])

# Season context is sourced from env var to keep API stateless and deploy-friendly.
# This is read at import-time intentionally (fast and deterministic for the container).
SEASON = int(os.getenv("SEASON", "2025"))

@router.get("/ping")
async def ping():
    """Lightweight healthcheck for the standings service."""
    return {"ok": True, "service": "standings"}

@router.get("/dbcheck")
async def dbcheck():
    """Verify DB connectivity with a trivial SELECT 1."""
    async with AsyncSessionLocal() as s:
        # Using RowMapping for clear key access; first() guarantees a single mapping or None
        row = (await s.execute(text("SELECT 1 AS ok"))).mappings().first()
    return {"db_ok": bool(row["ok"])}

@router.get("/standings")
async def get_standings():
    """Return divisional standings for the configured SEASON.

    Join semantics
    --------------
    Currently uses an INNER JOIN:
        team_metadata_tbl  INNER JOIN  season_results_tbl
        ON tmt.team_abbr = srt.team_id AND srt.season = :season

    Rationale: Requires a results row for the given season; omits teams with no season rows.
    (If you want to include teams with no season rows, switch to LEFT JOIN — not doing that here.)
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
        FROM prod.team_metadata_tbl tmt
        INNER JOIN prod.season_results_tbl srt
               ON tmt.team_abbr = srt.team_id
              AND srt.season = :season
        ORDER BY tmt.team_division, wins DESC, point_diff DESC, team_id;
    """)
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(sql, {"season": SEASON})).mappings().all()
    return {"season": SEASON, "items": rows}

@router.get("/standings/conference")
async def get_standings_conference():
    """Return AFC and NFC standings, split by conference and sorted by record.

    Conference split
    ----------------
    We infer conference via team_division string prefix:
    - "AFC..." -> AFC
    - "NFC..." -> NFC
    This is string-based by design; if the schema changes, update the prefix logic.
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
        FROM prod.team_metadata_tbl tmt
        INNER JOIN prod.season_results_tbl srt
               ON tmt.team_abbr = srt.team_id
              AND srt.season = :season
        ORDER BY 
            CASE WHEN tmt.team_division LIKE 'AFC%' THEN 0 ELSE 1 END,
            wins DESC,
            losses ASC,
            ties DESC,
            point_diff DESC,
            team_id;
    """)
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(sql, {"season": SEASON})).mappings().all()

    afc = [dict(row) for row in rows if str(row["division"]).strip().upper().startswith("AFC")]
    nfc = [dict(row) for row in rows if str(row["division"]).strip().upper().startswith("NFC")]

    return {"season": SEASON, "afc": afc, "nfc": nfc}

