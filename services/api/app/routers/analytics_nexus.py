from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/analytics_nexus", tags=["analytics_nexus"])

ALLOWED_POSITIONS = {"QB", "RB", "WR", "TE"}
MV_MAP = {
    "QB": "public.player_weekly_qb_mv",
    "RB": "public.player_weekly_rb_mv",
    "WR": "public.player_weekly_wr_mv",
    "TE": "public.player_weekly_te_mv",
}
MIN_WEEK, MAX_WEEK_HARD = 1, 22  # (REG <= 18; POST small; safe upper bound)

def _normalize_rank_by(rank_by: str) -> str:
    rb = (rank_by or "sum").strip().lower()
    if rb in {"sum"}:
        return "SUM"
    if rb in {"mean", "avg", "average"}:
        return "AVG"
    raise HTTPException(status_code=400, detail="rank_by must be 'sum' or 'mean'")

def _normalize_season_type(season_type: str) -> str:
    st = (season_type or "").strip().upper()
    if st not in {"REG", "POST", "ALL"}:
        raise HTTPException(status_code=400, detail="season_type must be one of REG, POST, ALL")
    return st

def _normalize_position(position: str) -> str:
    pos = (position or "").strip().upper()
    if pos not in ALLOWED_POSITIONS:
        raise HTTPException(status_code=400, detail=f"position must be one of {sorted(ALLOWED_POSITIONS)}")
    return pos

def _normalize_series_type(stat_type: str) -> str:
    st = (stat_type or "base").strip().lower()
    if st not in {"base", "cumulative"}:
        raise HTTPException(status_code=400, detail="stat_type must be 'base' or 'cumulative'")
    return st

def _clamp_weeks(week_start: int, week_end: int) -> tuple[int, int]:
    ws = max(MIN_WEEK, min(MAX_WEEK_HARD, int(week_start)))
    we = max(MIN_WEEK, min(MAX_WEEK_HARD, int(week_end)))
    if we < ws:
        raise HTTPException(status_code=400, detail="week_end must be >= week_start")
    return ws, we

def _pick_source_table(season: int, position: str) -> tuple[str, bool]:
    """
    Return (table_name, uses_mv). Use MV only for seasons 2019–2025; otherwise fall back to raw table.
    """
    if 2019 <= int(season) <= 2025:
        return MV_MAP[position], True
    return "public.player_weekly_tbl", False

@router.get("/player/trajectories/{season}/{season_type}/{stat_name}/{position}/{top_n}")
async def get_player_weekly_trajectories(
    season: int,
    season_type: str,
    stat_name: str,
    position: str,
    top_n: int,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",        # 'base' or 'cumulative' (use existing long data)
    rank_by: str = "sum",           # 'sum' or 'mean'
    min_games: int = 0,             # require at least this many non-NULL weeks in range
):
    """
    Top-N player weekly trajectories for a stat.

    - Uses position-specific MVs (2019–2025) when available; otherwise falls back to raw table.
    - stat_type: 'base' (weekly values) or 'cumulative' (use cumulative rows already in long data).
    - rank_by: 'sum' or 'mean' of weekly `value` (NULLs ignored by SUM/AVG).
    - min_games: floor on COUNT(value) (counts non-NULL weeks within filters).
    - Results ordered by player_rank, then week.
    """
    pos = _normalize_position(position)
    st = _normalize_season_type(season_type)
    series_type = _normalize_series_type(stat_type)
    agg_func = _normalize_rank_by(rank_by)
    ws, we = _clamp_weeks(week_start, week_end)
    mg = max(0, int(min_games))

    source_table, uses_mv = _pick_source_table(season, pos)

    if uses_mv:
        # MV already includes team_color columns
        query = f"""
        WITH filtered AS (
            SELECT
                player_id, name, team, season, season_type, week, position,
                stat_name, stat_type, value, team_color, team_color2
            FROM {source_table}
            WHERE season = :season
              AND (:season_type = 'ALL' OR season_type = :season_type)
              AND stat_name = :stat_name
              AND stat_type = :stat_type
              AND position = :position
              AND week BETWEEN :week_start AND :week_end
        ),
        agg AS (
            SELECT
                player_id,
                COUNT(value) AS games_played,
                {agg_func}(value) AS agg_value
            FROM filtered
            GROUP BY player_id
            HAVING COUNT(value) >= :min_games
        ),
        ranks AS (
            SELECT player_id,
                   RANK() OVER (ORDER BY agg_value DESC, player_id) AS player_rank
            FROM agg
            ORDER BY agg_value DESC, player_id
            LIMIT :top_n
        )
        SELECT f.player_id, f.name, f.team, f.season, f.season_type, f.week,
               f.position, f.stat_name, f.stat_type, f.value,
               f.team_color, f.team_color2,
               r.player_rank
        FROM filtered f
        JOIN ranks r USING (player_id)
        ORDER BY r.player_rank, f.week;
        """
    else:
        # Raw table; join colors
        query = f"""
        WITH filtered AS (
            SELECT
                pwt.player_id, pwt.name, pwt.team, pwt.season, pwt.season_type, pwt.week,
                pwt.position, pwt.stat_name, pwt.stat_type, pwt.value,
                tmt.team_color, tmt.team_color2
            FROM public.player_weekly_tbl pwt
            LEFT JOIN public.team_metadata_tbl tmt
              ON pwt.team = tmt.team_abbr
            WHERE pwt.season = :season
              AND (:season_type = 'ALL' OR pwt.season_type = :season_type)
              AND pwt.stat_name = :stat_name
              AND pwt.stat_type = :stat_type
              AND pwt.position = :position
              AND pwt.week BETWEEN :week_start AND :week_end
        ),
        agg AS (
            SELECT
                player_id,
                COUNT(value) AS games_played,
                {agg_func}(value) AS agg_value
            FROM filtered
            GROUP BY player_id
            HAVING COUNT(value) >= :min_games
        ),
        ranks AS (
            SELECT player_id,
                   RANK() OVER (ORDER BY agg_value DESC, player_id) AS player_rank
            FROM agg
            ORDER BY agg_value DESC, player_id
            LIMIT :top_n
        )
        SELECT f.player_id, f.name, f.team, f.season, f.season_type, f.week,
               f.position, f.stat_name, f.stat_type, f.value,
               f.team_color, f.team_color2,
               r.player_rank
        FROM filtered f
        JOIN ranks r USING (player_id)
        ORDER BY r.player_rank, f.week;
        """

    params = {
        "season": int(season),
        "season_type": st,
        "stat_name": stat_name,
        "stat_type": series_type,  # 'base' or 'cumulative' from the long data
        "position": pos,
        "week_start": ws,
        "week_end": we,
        "top_n": int(top_n),
        "min_games": mg,
    }

    async with AsyncSessionLocal() as session:
        result = await session.execute(text(query), params)
        rows = result.mappings().all()

    if not rows:
        return {"error": "No data found"}

    return [dict(r) for r in rows]

