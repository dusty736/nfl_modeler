"""
Analytics Nexus Router
----------------------
High-level analytics endpoints for players and teams:
- Weekly trajectories
- Consistency/volatility violins (+ badges)
- Quadrant scatter plots (derived metrics + gating)
- Rolling percentiles (form) for small multiples

Conventions
-----------
- Seasons: supports multi-season windows via flexible query parsing (?seasons=…).
- Season types: REG | POST | ALL (ALL applies filter as OR).
- Stat series: 'base' for weekly values; 'cumulative' reads pre-aggregated long rows
  for players and computes window SUM for teams (documented in each endpoint).
- MV usage: For 2019–2025, position-specific materialized views (MV_MAP) are used;
  otherwise we fall back to the raw long table. Table names are chosen from fixed
  constants only (no user-tainted identifiers).

Safety & Shapes
---------------
- All user inputs feed bound parameters (no SQL injection). The only f-strings with
  identifiers come from internal constants MV_MAP and are not user-controlled.
- Return shapes are stable and documented in each endpoint.
- This file adds documentation and removes duplicated helpers; NO functional changes.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import text
from app.db import AsyncSessionLocal

# --- Router setup & globals ---------------------------------------------------
router = APIRouter(prefix="/analytics_nexus", tags=["analytics_nexus"])

ALLOWED_POSITIONS = {"QB", "RB", "WR", "TE"}
MV_MAP = {
    "QB": "public.player_weekly_qb_mv",
    "RB": "public.player_weekly_rb_mv",
    "WR": "public.player_weekly_wr_mv",
    "TE": "public.player_weekly_te_mv",
}
MIN_WEEK, MAX_WEEK_HARD = 1, 22  # (REG <= 18; POST small; safe upper bound)

ALLOWED_TOP_BY = {"combined", "x_gate", "y_gate", "x_value", "y_value"}

# Weeks are clamped defensively within [MIN_WEEK, MAX_WEEK_HARD].
# MAX_WEEK_HARD=22 allows REG up to 18 plus small POST windows without surprises.

# --- Normalizers & helpers (canonical copies; do not re-define below) --------
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

# === Player: Weekly Trajectories =================================================
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
    """Top-N player weekly trajectories for a single stat.
    
    Selects the top players over a week window and returns their week-by-week values
    for plotting. Uses position-specific materialized views for seasons 2019–2025
    and falls back to the raw long table otherwise.
    
    Args:
        season (int): Season year (e.g., 2024).
        season_type (str): One of {"REG","POST","ALL"}; "ALL" keeps both.
        stat_name (str): Stat identifier in storage (e.g., "rushing_yards").
        position (str): One of {"QB","RB","WR","TE"}.
        top_n (int): Number of players to include (ranked by `rank_by` aggregate).
        week_start (int, optional): Inclusive week lower bound. Clamped to [1,22]. Default 1.
        week_end (int, optional): Inclusive week upper bound. Clamped to [1,22]. Default 18.
        stat_type (str, optional): "base" (weekly values) or "cumulative" (pre-computed rows). Default "base".
        rank_by (str, optional): Aggregate used to rank players: "sum" or "mean". Default "sum".
        min_games (int, optional): Minimum non-NULL weeks within [week_start, week_end]. Default 0.
    
    Returns:
        List[dict]: Rows ordered by player_rank then week with keys:
            [
              {
                "player_id": str, "name": str, "team": str,
                "season": int, "season_type": str, "week": int,
                "position": str, "stat_name": str, "stat_type": str,
                "value": float|None,
                "team_color": str, "team_color2": str,
                "player_rank": int
              },
              ...
            ]
        If no data match, returns {"error": "No data found"}.
    
    Raises:
        HTTPException: 400 on invalid inputs (position/season_type/stat_type/rank_by or bad weeks).
    
    Notes:
        - SUM/AVG ignore NULLs. `min_games` applies to COUNT(value).
        - Weeks are clamped defensively to [1,22] before querying.
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

def _normalize_season_type(season_type: str) -> str:
    st = (season_type or "").strip().upper()
    if st not in {"REG", "POST", "ALL"}:
        raise HTTPException(status_code=400, detail="season_type must be one of REG, POST, ALL")
    return st

def _normalize_order_by(order_by: str) -> str:
    ob = (order_by or "rCV").strip()
    ob_norm = ob.lower()
    if ob_norm in {"rcv", "rcV", "Rcv"}:
        return "rCV"
    if ob_norm == "iqr":
        return "IQR"
    if ob_norm == "median":
        return "median"
    raise HTTPException(status_code=400, detail="order_by must be one of rCV, IQR, median")

def _clamp_weeks(week_start: int, week_end: int) -> tuple[int, int]:
    ws = max(MIN_WEEK, min(MAX_WEEK_HARD, int(week_start)))
    we = max(MIN_WEEK, min(MAX_WEEK_HARD, int(week_end)))
    if we < ws:
        raise HTTPException(status_code=400, detail="week_end must be >= week_start")
    return ws, we

def _split_mv_raw_seasons(seasons: List[int]) -> tuple[List[int], List[int]]:
    mv_seasons = [s for s in seasons if 2019 <= int(s) <= 2025]
    raw_seasons = [s for s in seasons if s not in mv_seasons]
    return mv_seasons, raw_seasons

def _parse_seasons_from_request(request: Request) -> list[int]:
    """
    Accepts any of the following (mix-and-match):
      ?seasons=2023
      ?seasons=2023&seasons=2024
      ?seasons=2023,2024
      ?seasons[]=2023&seasons[]=2024
      ?season=2024
      ?seasons=[2023, 2024]
      Ranges: 2023-2025, 2023:2025, 2023–2025 (en dash)
    Returns a sorted, de-duplicated list of ints.
    """
    raw_vals: list[str] = []
    # common patterns
    raw_vals += request.query_params.getlist("seasons")
    raw_vals += request.query_params.getlist("seasons[]")
    raw_vals += request.query_params.getlist("season")  # allow repeatable "season"
    single = request.query_params.get("season")
    if single:
        raw_vals.append(single)

    out: list[int] = []
    if not raw_vals:
        raise HTTPException(status_code=400,
                            detail="At least one season must be provided via seasons=YYYY (repeatable or CSV)")

    def _emit_range(a: int, b: int):
        lo, hi = (a, b) if a <= b else (b, a)
        out.extend(range(lo, hi + 1))

    for v in raw_vals:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        # strip JSON-y brackets
        if s.startswith("[") and s.endswith("]"):
            s = s[1:-1]
        # normalise separators to commas
        s = s.replace(";", ",").replace(" ", ",")
        # split on comma, then handle ranges per token
        for tok in filter(None, (t.strip() for t in s.split(","))):
            # handle ranges using -, :, or en/em dashes
            for sep in ("-", ":", "–", "—"):
                if sep in tok:
                    a, b = tok.split(sep, 1)
                    try:
                        _emit_range(int(a), int(b))
                    except ValueError:
                        raise HTTPException(status_code=400, detail=f"Invalid season range: {tok}")
                    break
            else:
                try:
                    out.append(int(tok))
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"Invalid season value: {tok}")

    if not out:
        raise HTTPException(status_code=400,
                            detail="At least one season must be provided via seasons=YYYY (repeatable or CSV)")
    return sorted(set(out))

# === Player: Violins (consistency/volatility) ====================================
# Note: dynamic UNION picks MV vs raw per-season to avoid empty ANY(:param) binds.
@router.get("/player/violins/{stat_name}/{position}/{top_n}")
async def get_player_violins(
    request: Request,
    stat_name: str,
    position: str,
    top_n: int,
    season_type: str = Query("REG", description="REG | POST | ALL"),
    stat_type: str = Query("base", description="base | cumulative"),
    week_start: int = Query(1, ge=1, le=22),
    week_end: int = Query(18, ge=1, le=22),
    order_by: str = Query("rCV", description="rCV | IQR | median"),
    min_games_for_badges: int = Query(6, ge=0),
    debug: Optional[bool] = Query(False),
):
    """Consistency/volatility violin data for Top-N players over multi-season windows.
    
    Ranks players by pooled total (SUM of values) across the selected seasons/weeks,
    then returns per-player weekly points (for violins) and summary dispersion stats.
    Also emits simple "badges" for most consistent/volatile among adequately sampled players.
    
    Path:
        /analytics_nexus/player/violins/{stat_name}/{position}/{top_n}
    
    Args:
        request (Request): Used to parse flexible ?seasons inputs (repeatable/CSV/ranges).
        stat_name (str): Stat to analyze (storage identifier).
        position (str): {"QB","RB","WR","TE"}.
        top_n (int): Number of players to include (1..50).
        season_type (str, query): "REG" | "POST" | "ALL". Default "REG".
        stat_type (str, query): "base" | "cumulative". Default "base".
        week_start (int, query): Inclusive lower week (1..22). Default 1.
        week_end (int, query): Inclusive upper week (1..22). Default 18.
        order_by (str, query): "rCV" | "IQR" | "median". Controls sort in summary table. Default "rCV".
        min_games_for_badges (int, query): Minimum n for badge eligibility. Default 6.
        debug (bool, query): If True, includes extra meta/debug fields.
    
    Returns:
        dict: {
          "weekly": [ {player_id,name,team,season,season_type,week,position,stat_name,stat_type,value,team_color2,player_order} ],
          "summary": [
              {
                "player_id", "name", "team_mode", "team_color_major",
                "n_games", "q25","q50","q75","IQR","MAD","rCV","small_n",
                "order_by","order_metric","player_order"
              }
          ],
          "badges": {"most_consistent": list| "—", "most_volatile": list| "—"},
          "meta": {
              "position","stat_name","stat_type","season_type","seasons",
              "week_start","week_end","order_by","top_n","min_games_for_badges"
          }
        }
        If seasons resolve to no sources or query returns empty, arrays are empty and badges are "—".
    
    Raises:
        HTTPException: 400 on invalid inputs (position/season_type/stat_type/order_by/top_n) or missing seasons.
    
    Notes:
        - Seasons are parsed from multiple syntaxes (?seasons=2023,2024, ranges like 2023-2025, etc.).
        - Ranking pool uses SUM(value) across the window; violin points exclude NULL values.
        - rCV = MAD / |median|; badge pool excludes small_n and NaNs.
    """
    pos = _normalize_position(position)
    stype = _normalize_series_type(stat_type)
    st = _normalize_season_type(season_type)
    ob = _normalize_order_by(order_by)
    ws, we = _clamp_weeks(week_start, week_end)
    top_n = int(top_n)
    if top_n < 1 or top_n > 50:
        # Soft sanity cap to avoid absurd payloads; tweak as desired.
        raise HTTPException(status_code=400, detail="top_n must be between 1 and 50")

    seasons = _parse_seasons_from_request(request)
    mv_seasons, raw_seasons = _split_mv_raw_seasons(seasons)

    # Build the UNIONed filtered CTE dynamically so we don't reference empty bind params.
    filtered_parts = []
    params = {
        "season_type": st,
        "stat_name": stat_name,
        "stat_type": stype,
        "position": pos,
        "week_start": ws,
        "week_end": we,
        "top_n": top_n,
        "min_games_for_badges": int(min_games_for_badges),
        "order_by": ob,
    }

    if mv_seasons:
        params["seasons_mv"] = mv_seasons
        filtered_parts.append(f"""
            SELECT
                player_id, name, team, season, season_type, week, position,
                stat_name, stat_type, value, team_color, team_color2
            FROM {MV_MAP[pos]}
            WHERE season = ANY(:seasons_mv)
              AND (:season_type = 'ALL' OR season_type = :season_type)
              AND stat_name = :stat_name
              AND stat_type = :stat_type
              AND position = :position
              AND week BETWEEN :week_start AND :week_end
        """)

    if raw_seasons:
        params["seasons_raw"] = raw_seasons
        filtered_parts.append("""
            SELECT
                pwt.player_id, pwt.name, pwt.team, pwt.season, pwt.season_type, pwt.week,
                pwt.position, pwt.stat_name, pwt.stat_type, pwt.value,
                tmt.team_color, tmt.team_color2
            FROM public.player_weekly_tbl pwt
            LEFT JOIN public.team_metadata_tbl tmt
              ON pwt.team = tmt.team_abbr
            WHERE pwt.season = ANY(:seasons_raw)
              AND (:season_type = 'ALL' OR pwt.season_type = :season_type)
              AND pwt.stat_name = :stat_name
              AND pwt.stat_type = :stat_type
              AND pwt.position = :position
              AND pwt.week BETWEEN :week_start AND :week_end
        """)

    if not filtered_parts:
        # This would be odd (no seasons?), but guard anyway.
        return {
            "weekly": [],
            "summary": [],
            "badges": {"most_consistent": "—", "most_volatile": "—"},
            "meta": {
                "position": pos, "stat_name": stat_name, "stat_type": stype, "season_type": st,
                "seasons": seasons, "week_start": ws, "week_end": we,
                "order_by": ob, "top_n": top_n, "min_games_for_badges": int(min_games_for_badges),
            },
        }

    filtered_sql = " UNION ALL ".join(filtered_parts)

    # Core SQL. Notes:
    # - Top-N by SUM(value) over pooled window (NULLs ignored by SUM).
    # - Plot rows exclude NULL value.
    # - Dominant team via mode (count desc, tie team asc).
    # - Percentiles via percentile_cont; MAD via median of absolute deviations from per-player median.
    # - Ordering per 'order_by' and stable tie-break on player_id.
    query = f"""
    WITH filtered AS (
        {filtered_sql}
    ),
    top_players AS (
        SELECT player_id,
               MAX(name) AS name,
               SUM(value) AS total_value
        FROM filtered
        GROUP BY player_id
        ORDER BY total_value DESC NULLS LAST, player_id
        LIMIT :top_n
    ),
    plot_rows AS (
        SELECT f.*
        FROM filtered f
        JOIN top_players tp USING (player_id)
        WHERE f.value IS NOT NULL
    ),
    dominant_team AS (
        SELECT player_id, team AS team_mode, team_color AS team_color_major
        FROM (
            SELECT player_id, team, team_color, cnt,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY cnt DESC, team ASC) AS rn
            FROM (
                SELECT player_id, team, team_color, COUNT(*) AS cnt
                FROM plot_rows
                GROUP BY player_id, team, team_color
            ) c
        ) r
        WHERE rn = 1
    ),
    percentiles AS (
        SELECT
            player_id,
            COUNT(value) AS n_games,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY value) AS q25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY value) AS q50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY value) AS q75
        FROM plot_rows
        GROUP BY player_id
    ),
    mad_calc AS (
        SELECT
            pr.player_id,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY ABS(pr.value - p.q50)) AS mad
        FROM plot_rows pr
        JOIN percentiles p USING (player_id)
        GROUP BY pr.player_id
    ),
    summaries AS (
        SELECT
            p.player_id,
            tp.name,
            COALESCE(dt.team_mode, MIN(pr.team)) AS team_mode,   -- fallback if no dominant row
            COALESCE(dt.team_color_major, MIN(pr.team_color)) AS team_color_major,
            p.n_games, p.q25, p.q50, p.q75,
            (p.q75 - p.q25) AS iqr,
            m.mad,
            CASE
                WHEN p.q50 IS NULL OR p.q50 = 0 OR m.mad IS NULL THEN NULL
                ELSE m.mad / ABS(p.q50)
            END AS rcv,
            (p.n_games < :min_games_for_badges) AS small_n
        FROM percentiles p
        JOIN top_players tp USING (player_id)
        LEFT JOIN mad_calc m USING (player_id)
        LEFT JOIN dominant_team dt USING (player_id)
        LEFT JOIN plot_rows pr USING (player_id)
        GROUP BY p.player_id, tp.name, dt.team_mode, dt.team_color_major, p.n_games, p.q25, p.q50, p.q75, m.mad
    ),
    ordered AS (
        SELECT
            s.*,
            CASE
                WHEN :order_by = 'median' THEN -s.q50
                WHEN :order_by = 'IQR'    THEN s.iqr
                ELSE s.rcv
            END AS order_metric
        FROM summaries s
    ),
    ordered_ranked AS (
        SELECT
            o.*,
            ROW_NUMBER() OVER (ORDER BY
                CASE
                    WHEN :order_by = 'median' THEN -o.q50
                    WHEN :order_by = 'IQR'    THEN o.iqr
                    ELSE o.rcv
                END ASC NULLS LAST,
                o.player_id
            ) AS player_order
        FROM ordered o
    )
    SELECT
        -- Section tags so we can split results cleanly in Python
        'WEEKLY' AS section,
        pr.player_id, pr.name, pr.team, pr.season, pr.season_type, pr.week,
        pr.position, pr.stat_name, pr.stat_type, pr.value, pr.team_color2,
        orr.player_order
    FROM plot_rows pr
    JOIN ordered_ranked orr USING (player_id)
    UNION ALL
    SELECT
        'SUMMARY' AS section,
        orr.player_id, orr.name, orr.team_mode AS team, NULL::int AS season, NULL::text AS season_type, NULL::int AS week,
        :position AS position, :stat_name AS stat_name, :stat_type AS stat_type, NULL::double precision AS value, orr.team_color_major AS team_color2,
        orr.player_order
    FROM ordered_ranked orr
    ORDER BY 1, 13, 6 NULLS FIRST;  -- section, player_order, week
    """

    params.update({
        "position": pos,
        "stat_name": stat_name,
        "stat_type": stype,
    })

    async with AsyncSessionLocal() as session:
        result = await session.execute(text(query), params)
        rows = [dict(r) for r in result.mappings().all()]

    # Split into weekly/summary and build badges/meta.
    weekly: list[dict] = []
    summary_raw: list[dict] = []
    for r in rows:
        sect = r.pop("section")
        if sect == "WEEKLY":
            weekly.append(r)
        else:
            # Rename fields to match spec for summary
            summary_raw.append({
                "player_id": r["player_id"],
                "name": r["name"],
                "team_mode": r["team"],
                "team_color_major": r["team_color2"],
                "player_order": r["player_order"],
                # Placeholders; we’ll enrich below by refetching from ordered_ranked if needed.
            })

    # Enrich summary with stats: re-query just the ordered_ranked CTE results (or infer from existing)
    # To avoid a second DB trip, we stashed only name/color/order in the UNION; so rebuild from first set:
    # We'll compute badges & attach stats by re-joining via player_id from an in-memory map.
    # For that, run a tiny SELECT over ordered_ranked only:
    query_summaries = f"""
    WITH filtered AS (
        {filtered_sql}
    ),
    top_players AS (
        SELECT player_id,
               MAX(name) AS name,
               SUM(value) AS total_value
        FROM filtered
        GROUP BY player_id
        ORDER BY total_value DESC NULLS LAST, player_id
        LIMIT :top_n
    ),
    plot_rows AS (
        SELECT f.* FROM filtered f JOIN top_players tp USING (player_id) WHERE f.value IS NOT NULL
    ),
    percentiles AS (
        SELECT
            player_id,
            COUNT(value) AS n_games,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY value) AS q25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY value) AS q50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY value) AS q75
        FROM plot_rows
        GROUP BY player_id
    ),
    mad_calc AS (
        SELECT
            pr.player_id,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY ABS(pr.value - p.q50)) AS mad
        FROM plot_rows pr
        JOIN percentiles p USING (player_id)
        GROUP BY pr.player_id
    ),
    summaries AS (
        SELECT
            p.player_id, MAX(tp.name) AS name,
            COUNT(*) FILTER (WHERE pr.value IS NOT NULL) AS n_obs,
            p.n_games, p.q25, p.q50, p.q75,
            (p.q75 - p.q25) AS iqr,
            m.mad,
            CASE WHEN p.q50 IS NULL OR p.q50 = 0 OR m.mad IS NULL THEN NULL ELSE m.mad / ABS(p.q50) END AS rcv
        FROM percentiles p
        JOIN top_players tp USING (player_id)
        JOIN plot_rows pr USING (player_id)
        LEFT JOIN mad_calc m USING (player_id)
        GROUP BY p.player_id, p.n_games, p.q25, p.q50, p.q75, m.mad
    ),
    ordered_ranked AS (
        SELECT
            s.*,
            ROW_NUMBER() OVER (ORDER BY
                CASE
                    WHEN :order_by = 'median' THEN -s.q50
                    WHEN :order_by = 'IQR'    THEN s.iqr
                    ELSE s.rcv
                END ASC NULLS LAST,
                s.player_id
            ) AS player_order,
            (s.n_games < :min_games_for_badges) AS small_n
        FROM summaries s
    )
    SELECT * FROM ordered_ranked;
    """

    async with AsyncSessionLocal() as session:
        res2 = await session.execute(text(query_summaries), params)
        ord_rows = [dict(r) for r in res2.mappings().all()]

    # Merge color/name/order with full stats (build dict by player_id)
    ord_map = {r["player_id"]: r for r in ord_rows}
    summary: list[dict] = []
    for s in summary_raw:
        stats = ord_map.get(s["player_id"], {})
        summary.append({
            "player_id": s["player_id"],
            "name": s["name"],
            "team_mode": s["team_mode"],
            "team_color_major": s["team_color_major"],
            "n_games": stats.get("n_games"),
            "q25": stats.get("q25"),
            "q50": stats.get("q50"),
            "q75": stats.get("q75"),
            "IQR": stats.get("iqr"),
            "MAD": stats.get("mad"),
            "rCV": stats.get("rcv"),
            "small_n": bool(stats.get("small_n", False)),
            "order_by": ob,
            "order_metric": (
                (-stats["q50"]) if ob == "median"
                else (stats["iqr"] if ob == "IQR" else stats.get("rcv"))
            ) if stats else None,
            "player_order": s["player_order"],
        })

    # Badges (most consistent/volatile) from adequate sample pool
    pool = [s for s in summary if not s["small_n"] and s.get("rCV") is not None]
    most_consistent = [s["name"] for s in sorted(pool, key=lambda x: (x["rCV"], x["player_id"]))[:3]] or ["—"]
    most_volatile = [s["name"] for s in sorted(pool, key=lambda x: (-x["rCV"], x["player_id"]))[:3]] or ["—"]

    payload = {
        "weekly": weekly,
        "summary": summary,
        "badges": {
            "most_consistent": most_consistent if most_consistent != ["—"] else "—",
            "most_volatile": most_volatile if most_volatile != ["—"] else "—",
        },
        "meta": {
            "position": pos,
            "stat_name": stat_name,
            "stat_type": stype,
            "season_type": st,
            "seasons": seasons,
            "week_start": ws,
            "week_end": we,
            "order_by": ob,
            "top_n": top_n,
            "min_games_for_badges": int(min_games_for_badges),
        },
    }
    return payload
  
# ===== Player Quadrant Scatter — helpers ========================================
def _normalize_bool(v: Optional[str | bool]) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"1", "true", "t", "yes", "y", "on"}

def _normalize_top_by(top_by: str) -> str:
    tb = (top_by or "combined").strip().lower()
    if tb not in ALLOWED_TOP_BY:
        raise HTTPException(status_code=400, detail=f"top_by must be one of {sorted(ALLOWED_TOP_BY)}")
    return tb

# metrics: return label and list of required stats; SQL exprs refer to aggregated columns
def _metric_plan(metric: str, position: str) -> dict:
    m = (metric or "").strip()
    qb = (position.upper() == "QB")

    # derived metrics
    derived = {
        "passing_epa_per_dropback": dict(
            label="EPA per Dropback",
            required=["attempts", "sacks", "passing_epa"],
            value="CASE WHEN COALESCE(attempts,0)+COALESCE(sacks,0) > 0 "
                  "THEN COALESCE(passing_epa,0)::double precision / (COALESCE(attempts,0)+COALESCE(sacks,0)) "
                  "ELSE NULL END",
            gate="COALESCE(passing_epa,0)"
        ),
        "passing_anya": dict(
            label="ANY/A",
            required=["attempts", "sacks", "sack_yards", "passing_yards", "passing_tds", "interceptions"],
            value="CASE WHEN COALESCE(attempts,0)+COALESCE(sacks,0) > 0 "
                  "THEN (COALESCE(passing_yards,0) + 20*COALESCE(passing_tds,0) "
                  "- 45*COALESCE(interceptions,0) - COALESCE(sack_yards,0))::double precision "
                  "/ (COALESCE(attempts,0)+COALESCE(sacks,0)) "
                  "ELSE NULL END",
            gate="COALESCE(passing_yards,0)"
        ),
        "rushing_epa_per_carry": dict(
            label="EPA per Rush",
            required=["carries","rushing_epa"],
            value="CASE WHEN COALESCE(carries,0) > 0 "
                  "THEN COALESCE(rushing_epa,0)::double precision / COALESCE(carries,0) "
                  "ELSE NULL END",
            gate="COALESCE(rushing_epa,0)"
        ),
        "receiving_epa_per_target": dict(
            label="EPA per Target",
            required=["targets","receiving_epa"],
            value="CASE WHEN COALESCE(targets,0) > 0 "
                  "THEN COALESCE(receiving_epa,0)::double precision / COALESCE(targets,0) "
                  "ELSE NULL END",
            gate="COALESCE(receiving_epa,0)"
        ),
        "total_epa_per_opportunity": dict(
            label="Total EPA per Opportunity",
            required=["attempts","sacks","carries","targets","passing_epa","rushing_epa","receiving_epa"],
            value=("CASE WHEN "
                   f"{'(COALESCE(attempts,0)+COALESCE(sacks,0))+COALESCE(carries,0)' if qb else '(COALESCE(targets,0)+COALESCE(carries,0))'} > 0 "
                   "THEN "
                   f"{'(COALESCE(passing_epa,0)+COALESCE(rushing_epa,0))' if qb else '(COALESCE(receiving_epa,0)+COALESCE(rushing_epa,0))'}"
                   "::double precision / "
                   f"{'(COALESCE(attempts,0)+COALESCE(sacks,0))+COALESCE(carries,0)' if qb else '(COALESCE(targets,0)+COALESCE(carries,0))'} "
                   "ELSE NULL END"),
            gate=(f"(COALESCE(passing_epa,0)+COALESCE(rushing_epa,0))" if qb
                  else "(COALESCE(receiving_epa,0)+COALESCE(rushing_epa,0))")
        ),
        "yards_per_opportunity": dict(
            label="Yards per Opportunity",
            required=["attempts","sacks","carries","targets","passing_yards","rushing_yards","receiving_yards"],
            value=("CASE WHEN "
                   f"{'(COALESCE(attempts,0)+COALESCE(sacks,0))+COALESCE(carries,0)' if qb else '(COALESCE(targets,0)+COALESCE(carries,0))'} > 0 "
                   "THEN "
                   f"{'(COALESCE(passing_yards,0)+COALESCE(rushing_yards,0))' if qb else '(COALESCE(receiving_yards,0)+COALESCE(rushing_yards,0))'}"
                   "::double precision / "
                   f"{'(COALESCE(attempts,0)+COALESCE(sacks,0))+COALESCE(carries,0)' if qb else '(COALESCE(targets,0)+COALESCE(carries,0))'} "
                   "ELSE NULL END"),
            gate=(f"(COALESCE(passing_yards,0)+COALESCE(rushing_yards,0))" if qb
                  else "(COALESCE(receiving_yards,0)+COALESCE(rushing_yards,0))")
        ),
    }

    if m in derived:
        return derived[m]

    # raw sum fallback
    nice = m.replace("_", " ").title()
    ident = m  # stat column name
    return dict(
        label=nice,
        required=[ident],
        value=f"COALESCE({ident},0)::double precision",
        gate=f"ABS(COALESCE({ident},0))"
    )

def _split_mv_raw_seasons(seasons: list[int]) -> tuple[list[int], list[int]]:
    mv = [s for s in seasons if 2019 <= s <= 2025]
    raw = [s for s in seasons if s < 2019 or s > 2025]
    return mv, raw

# === Player: Quadrant Scatter ====================================================
@router.get("/player/scatter/{metric_x}/{metric_y}/{position}/{top_n}")
async def get_player_scatter_quadrants(
    request: Request,
    metric_x: str,
    metric_y: str,
    position: str,
    top_n: int,
    season_type: str = Query("REG", description="REG | POST | ALL"),
    week_start: int = Query(1, ge=1, le=22),
    week_end: int = Query(18, ge=1, le=22),
    stat_type: str = Query("base", description="Use 'base' (weekly) values only"),
    top_by: str = Query("combined", description="combined | x_gate | y_gate | x_value | y_value"),
    log_x: bool | str = Query(False),
    log_y: bool | str = Query(False),
    label_all_points: bool | str = Query(True),
    debug: bool = Query(False),
):
    """Quadrant scatter for players with derived/raw X/Y metrics over a pooled window.
    
    Builds ratio-of-sums metrics from weekly *base* values, applies optional log filters
    (x>0 / y>0) before ranking, selects Top-N by `top_by`, and returns points plus
    median reference lines.
    
    Path:
        /analytics_nexus/player/scatter/{metric_x}/{metric_y}/{position}/{top_n}
    
    Args:
        request (Request): For flexible ?seasons parsing.
        metric_x (str): Metric identifier for X (see `_metric_plan`).
        metric_y (str): Metric identifier for Y.
        position (str): {"QB","RB","WR","TE"}.
        top_n (int): Number of players (1..100).
        season_type (str, query): "REG" | "POST" | "ALL". Default "REG".
        week_start (int, query): 1..22, default 1.
        week_end (int, query): 1..22, default 18.
        stat_type (str, query): Must be "base". Enforced.
        top_by (str, query): {"combined","x_gate","y_gate","x_value","y_value"}. Default "combined".
        log_x (bool|str, query): Truthy values ("1","true","yes","on") require x>0.
        log_y (bool|str, query): Truthy values require y>0.
        label_all_points (bool|str, query): Passed through in meta; labeling is a frontend concern.
        debug (bool, query): Adds light debug in meta when true.
    
    Returns:
        dict: {
          "points": [
            {"player_id","name","team","team_color","team_color2","x_value","y_value"}
          ],
          "meta": {
            "position","metric_x","metric_y","label_x","label_y",
            "seasons","season_type","week_start","week_end","stat_type",
            "top_by","top_n","log_x","log_y","label_all_points",
            "median_x","median_y"
          }
        }
        Empty result returns points=[], with medians set to None.
    
    Raises:
        HTTPException: 400 on invalid inputs (position/season_type/stat_type/top_by/top_n).
    
    Notes:
        - Metric definitions and gating columns are produced by `_metric_plan`.
        - Top-N order prioritizes rank_key, then gate_total, then combined value, then name.
    """
    pos = _normalize_position(position)
    st = _normalize_season_type(season_type)
    ws, we = _clamp_weeks(week_start, week_end)
    series_type = _normalize_series_type(stat_type)  # enforce 'base'
    if series_type != "base":
        raise HTTPException(status_code=400, detail="stat_type must be 'base' for scatter metrics")
    tb = _normalize_top_by(top_by)
    lx = _normalize_bool(log_x)
    ly = _normalize_bool(log_y)
    lap = _normalize_bool(label_all_points)  # surfaced in meta; labels are a frontend concern

    # seasons
    seasons = _parse_seasons_from_request(request)
    mv_seasons, raw_seasons = _split_mv_raw_seasons(seasons)

    # metric plans
    plan_x = _metric_plan(metric_x, pos)
    plan_y = _metric_plan(metric_y, pos)
    label_x, label_y = plan_x["label"], plan_y["label"]
    required_stats = sorted(set(plan_x["required"] + plan_y["required"]))

    if top_n < 1 or top_n > 100:
        raise HTTPException(status_code=400, detail="top_n must be between 1 and 100")

    # Build filtered UNION with stat_name in required set
    filtered_parts = []
    params: dict = {
        "season_type": st,
        "position": pos,
        "stat_type": "base",
        "week_start": ws,
        "week_end": we,
        "top_n": int(top_n),
        "required_stats": required_stats,
        "top_by": tb,
        "log_x": lx,
        "log_y": ly,
    }

    if mv_seasons:
        params["seasons_mv"] = mv_seasons
        filtered_parts.append(f"""
            SELECT
              player_id, name, team, season, season_type, week, position,
              stat_name, value, team_color, team_color2
            FROM {MV_MAP[pos]}
            WHERE season = ANY(:seasons_mv)
              AND (:season_type = 'ALL' OR season_type = :season_type)
              AND stat_type = :stat_type
              AND position = :position
              AND week BETWEEN :week_start AND :week_end
              AND stat_name = ANY(:required_stats)
        """)

    if raw_seasons:
        params["seasons_raw"] = raw_seasons
        filtered_parts.append("""
            SELECT
              pwt.player_id, pwt.name, pwt.team, pwt.season, pwt.season_type, pwt.week, pwt.position,
              pwt.stat_name, pwt.value, tmt.team_color, tmt.team_color2
            FROM public.player_weekly_tbl pwt
            LEFT JOIN public.team_metadata_tbl tmt
              ON pwt.team = tmt.team_abbr
            WHERE pwt.season = ANY(:seasons_raw)
              AND (:season_type = 'ALL' OR pwt.season_type = :season_type)
              AND pwt.stat_type = :stat_type
              AND pwt.position = :position
              AND pwt.week BETWEEN :week_start AND :week_end
              AND pwt.stat_name = ANY(:required_stats)
        """)

    if not filtered_parts:
        return {
            "points": [],
            "meta": {
                "position": pos, "metric_x": metric_x, "metric_y": metric_y,
                "label_x": label_x, "label_y": label_y,
                "seasons": seasons, "season_type": st,
                "week_start": ws, "week_end": we,
                "stat_type": "base",
                "top_by": tb, "top_n": top_n,
                "log_x": lx, "log_y": ly,
                "label_all_points": lap,
                "median_x": None, "median_y": None,
            },
        }

    filtered_sql = " UNION ALL ".join(filtered_parts)

    # Build dynamic per-stat SUM(...) FILTER columns safely via bound params
    # e.g., SUM(value) FILTER (WHERE stat_name = :stat_0) AS attempts
    stat_sums = []
    for i, stat in enumerate(required_stats):
        key = f"stat_{i}"
        params[key] = stat
        # alias = stat identifier (snake_case)
        stat_sums.append(f"SUM(value) FILTER (WHERE stat_name = :{key}) AS {stat}")

    sums_sql = ",\n              ".join(stat_sums)

    # SQL: aggregate -> compute metric values -> apply logs -> rank/select -> medians
    query = f"""
    WITH filtered AS (
        {filtered_sql}
    ),
    wide AS (
        SELECT
          player_id,
          MAX(name) AS name,
          team,
          COALESCE(MAX(team_color), '#888888')    AS team_color,
          COALESCE(MAX(team_color2), '#AAAAAA')   AS team_color2,
          {sums_sql}
        FROM filtered
        GROUP BY player_id, team
    ),
    metrics AS (
        SELECT
          player_id, name, team, team_color, team_color2,
          {plan_x["value"]} AS x_value,
          {plan_x["gate"]}  AS gate_x,
          {plan_y["value"]} AS y_value,
          {plan_y["gate"]}  AS gate_y
        FROM wide
    ),
    filtered_metrics AS (
        SELECT *
        FROM metrics
        WHERE x_value IS NOT NULL AND y_value IS NOT NULL
          { "AND x_value > 0" if lx else "" }
          { "AND y_value > 0" if ly else "" }
    ),
    ranked AS (
        SELECT
          *,
          (COALESCE(gate_x,0) + COALESCE(gate_y,0)) AS gate_total,
          CASE
            WHEN :top_by = 'combined' THEN (COALESCE(gate_x,0) + COALESCE(gate_y,0))
            WHEN :top_by = 'x_gate'   THEN COALESCE(gate_x,0)
            WHEN :top_by = 'y_gate'   THEN COALESCE(gate_y,0)
            WHEN :top_by = 'x_value'  THEN COALESCE(x_value,0)
            ELSE COALESCE(y_value,0)
          END AS rank_key
        FROM filtered_metrics
    ),
    topn AS (
        SELECT *
        FROM ranked
        ORDER BY rank_key DESC, gate_total DESC, (COALESCE(x_value,0)+COALESCE(y_value,0)) DESC, name
        LIMIT :top_n
    ),
    medians AS (
        SELECT
          percentile_cont(0.5) WITHIN GROUP (ORDER BY x_value) AS med_x,
          percentile_cont(0.5) WITHIN GROUP (ORDER BY y_value) AS med_y
        FROM topn
    )
    SELECT
      'POINT' AS section,
      t.player_id, t.name, t.team, t.team_color, t.team_color2,
      t.x_value, t.y_value,
      m.med_x, m.med_y
    FROM topn t
    CROSS JOIN medians m
    ORDER BY t.rank_key DESC, t.gate_total DESC, (COALESCE(t.x_value,0)+COALESCE(t.y_value,0)) DESC, t.name;
    """

    async with AsyncSessionLocal() as session:
        res = await session.execute(text(query), params)
        rows = [dict(r) for r in res.mappings().all()]

    if not rows:
        return {
            "points": [],
            "meta": {
                "position": pos,
                "metric_x": metric_x, "metric_y": metric_y,
                "label_x": label_x, "label_y": label_y,
                "seasons": seasons, "season_type": st,
                "week_start": ws, "week_end": we,
                "stat_type": "base",
                "top_by": tb, "top_n": top_n,
                "log_x": lx, "log_y": ly,
                "label_all_points": lap,
                "median_x": None, "median_y": None,
            },
        }

    # Build payload
    med_x = rows[0].get("med_x")
    med_y = rows[0].get("med_y")
    points = [
        {
            "player_id": r["player_id"],
            "name": r["name"],
            "team": r["team"],
            "team_color": r["team_color"],
            "team_color2": r["team_color2"],
            "x_value": r["x_value"],
            "y_value": r["y_value"],
        }
        for r in rows
    ]

    return {
        "points": points,
        "meta": {
            "position": pos,
            "metric_x": metric_x, "metric_y": metric_y,
            "label_x": label_x, "label_y": label_y,
            "seasons": seasons, "season_type": st,
            "week_start": ws, "week_end": we,
            "stat_type": "base",
            "top_by": tb, "top_n": top_n,
            "log_x": lx, "log_y": ly,
            "label_all_points": lap,
            "median_x": med_x, "median_y": med_y,
        },
    }

# === Player: Rolling Percentiles (form over time) =================================
@router.get("/player/rolling_percentiles/{metric}/{position}/{top_n}")
async def get_player_rolling_percentiles(
    request: Request,
    metric: str,                            # matches {metric} in the path
    position: str,
    top_n: int,
    seasons: Optional[List[int]] = Query(None),   # <-- accept seasons here
    season_type: str = Query("REG", description="REG | POST | ALL"),
    stat_type: str = Query("base", description="base | cumulative"),
    week_start: int = Query(1, ge=1, le=22),
    week_end: int = Query(18, ge=1, le=22),
    rolling_window: int = Query(4, ge=1),
    debug: Optional[bool] = Query(False),
):
    """Rolling form percentiles for Top-N players across seasons × weeks.
    
    Selects Top-N players by pooled SUM(value) of the chosen metric, then computes
    percentiles among those Top-N for each (season, season_type, week). Builds a
    unified time index (REG before POST) and a rolling mean over `rolling_window`.
    
    Path:
        /analytics_nexus/player/rolling_percentiles/{metric}/{position}/{top_n}
    
    Args:
        request (Request): For flexible ?seasons parsing (here also available as query param).
        metric (str): Stat identifier in storage (e.g., "receiving_yards").
        position (str): {"QB","RB","WR","TE"}.
        top_n (int): 1..48.
        seasons (List[int], query): Required; repeatable ?seasons=YYYY.
        season_type (str, query): "REG" | "POST" | "ALL". Default "REG".
        stat_type (str, query): "base" | "cumulative". Default "base".
        week_start (int, query): 1..22, default 1.
        week_end (int, query): 1..22, default 18.
        rolling_window (int, query): Window (k) for rolling mean of percentiles. Default 4.
        debug (bool, query): Adds debug fields when true.
    
    Returns:
        dict: {
          "series": [
            {"player_id","name","team","season","season_type","week",
             "t_idx","pct","pct_roll","team_color","team_color2","player_order"}
          ],
          "players": [
            {"player_id","name","team","team_color","team_color2","last_pct","player_order"}
          ],
          "meta": {
            "position","metric","metric_label","stat_type","season_type","seasons",
            "week_start","week_end","top_n","rolling_window"
          },
          "debug": {...}  # present only if debug=True
        }
    
    Raises:
        HTTPException: 400 on invalid inputs or if `seasons` is missing.
    
    Notes:
        - Percentiles are computed only among the Top-N pool per week.
        - Panel ordering uses last rolling percentile (desc) with stable tiebreakers.
    """
    pos = _normalize_position(position)
    stype = _normalize_series_type(stat_type)
    st = _normalize_season_type(season_type)
    ws, we = _clamp_weeks(week_start, week_end)

    top_n = int(top_n)
    if top_n < 1 or top_n > 48:
        raise HTTPException(status_code=400, detail="top_n must be between 1 and 48")

    if not seasons:
      raise HTTPException(status_code=400, detail="Query param 'seasons' is required")
    mv_seasons, raw_seasons = _split_mv_raw_seasons(seasons)

    # Build filtered UNION like other endpoints (only the chosen metric)
    filtered_parts = []
    params = {
        "season_type": st,
        "metric_name": metric,
        "stat_type": stype,
        "position": pos,
        "week_start": ws,
        "week_end": we,
        "top_n": top_n,
        "k": int(rolling_window),
    }

    if mv_seasons:
        params["seasons_mv"] = mv_seasons
        filtered_parts.append(f"""
            SELECT
                player_id, name, team, season, season_type, week, position,
                stat_name, stat_type, value, team_color, team_color2
            FROM {MV_MAP[pos]}
            WHERE season = ANY(:seasons_mv)
              AND (:season_type = 'ALL' OR season_type = :season_type)
              AND stat_name = :metric_name
              AND stat_type = :stat_type
              AND position = :position
              AND week BETWEEN :week_start AND :week_end
        """)

    if raw_seasons:
        params["seasons_raw"] = raw_seasons
        filtered_parts.append("""
            SELECT
                pwt.player_id, pwt.name, pwt.team, pwt.season, pwt.season_type, pwt.week,
                pwt.position, pwt.stat_name, pwt.stat_type, pwt.value,
                tmt.team_color, tmt.team_color2
            FROM public.player_weekly_tbl pwt
            LEFT JOIN public.team_metadata_tbl tmt
              ON pwt.team = tmt.team_abbr
            WHERE pwt.season = ANY(:seasons_raw)
              AND (:season_type = 'ALL' OR pwt.season_type = :season_type)
              AND pwt.stat_name = :metric_name
              AND pwt.stat_type = :stat_type
              AND pwt.position = :position
              AND pwt.week BETWEEN :week_start AND :week_end
        """)

    if not filtered_parts:
        return {
            "series": [],
            "players": [],
            "meta": {
                "position": pos, "metric": metric, "metric_label": metric.replace("_", " ").title(),
                "season_type": st, "seasons": seasons,
                "week_start": ws, "week_end": we,
                "top_n": top_n, "rolling_window": int(rolling_window),
            },
        }

    filtered_sql = " UNION ALL ".join(filtered_parts)

    # SQL pipeline:
    # - filtered: rows in window for the metric
    # - top_players: Top-N by SUM(value)
    # - plot_rows: rows from filtered but only Top-N, value NOT NULL
    # - dom_team: dominant team/color (mode) per player
    # - time_map: distinct (season,phase,week) for Top-N, ordered -> t_idx
    # - pct_rows: weekly percentiles among the Top-N for each (season,phase,week)
    # - roll_rows: rolling mean over k within (player, season, phase) by t_idx
    # - last_idx: last t_idx per player; last_vals to fetch last pct_roll
    # - ordered_players: order panels by last_pct_roll desc, then player_id
    # Final SELECT:
    #   • SERIES rows: one per player-week with t_idx, pct, pct_roll, colors, player_order
    #   • PLAYERS rows: player summary with last_pct_roll and colors/order
    query = f"""
    WITH filtered AS (
        {filtered_sql}
    ),
    top_players AS (
        SELECT player_id, MAX(name) AS name, SUM(value) AS total_value
        FROM filtered
        WHERE value IS NOT NULL
        GROUP BY player_id
        ORDER BY total_value DESC NULLS LAST, player_id
        LIMIT :top_n
    ),
    plot_rows AS (
        SELECT f.*
        FROM filtered f
        JOIN top_players tp USING (player_id)
        WHERE f.value IS NOT NULL
    ),
    dom_team AS (
        SELECT player_id, team AS team_mode, team_color AS team_color_major, team_color2 AS team_color2_major
        FROM (
            SELECT player_id, team, team_color, team_color2,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY cnt DESC, team ASC) AS rn
            FROM (
                SELECT player_id, team, team_color, team_color2, COUNT(*) AS cnt
                FROM plot_rows
                GROUP BY player_id, team, team_color, team_color2
            ) c
        ) r
        WHERE rn = 1
    ),
    -- phase ordering: REG before POST
    time_map AS (
        SELECT season, season_type, week,
               ROW_NUMBER() OVER (
                   ORDER BY season, CASE WHEN season_type='REG' THEN 1 ELSE 2 END, week
               ) AS t_idx
        FROM (
            SELECT DISTINCT season, season_type, week
            FROM plot_rows
        ) u
    ),
    base_pct AS (
        SELECT
            pr.player_id, pr.name, pr.season, pr.season_type, pr.week, pr.value,
            tm.t_idx,
            COUNT(*) OVER (PARTITION BY pr.season, pr.season_type, pr.week) AS n_obs,
            PERCENT_RANK() OVER (
                PARTITION BY pr.season, pr.season_type, pr.week
                ORDER BY pr.value
            ) * 100.0 AS pct_raw
        FROM plot_rows pr
        JOIN time_map tm USING (season, season_type, week)
    ),
    pct_rows AS (
        SELECT
            player_id, name, season, season_type, week, t_idx,
            CASE WHEN n_obs > 1 THEN pct_raw ELSE 50.0 END AS pct
        FROM base_pct
    ),
    roll_rows AS (
        SELECT
            p.*,
            -- rolling mean within (player, season, season_type) by t_idx (k-1 PRECEDING .. CURRENT ROW)
            AVG(p.pct) OVER (
                PARTITION BY p.player_id, p.season, p.season_type
                ORDER BY p.t_idx
                ROWS BETWEEN {rolling_window - 1} PRECEDING AND CURRENT ROW
            ) AS pct_roll
        FROM pct_rows p
    ),
    last_idx AS (
        SELECT player_id, MAX(t_idx) AS last_t FROM roll_rows GROUP BY player_id
    ),
    last_vals AS (
        SELECT r.player_id, r.pct_roll AS last_pct
        FROM roll_rows r
        JOIN last_idx li ON li.player_id = r.player_id AND li.last_t = r.t_idx
    ),
    ordered_players AS (
        SELECT
            tp.player_id,
            tp.name,
            COALESCE(dt.team_mode, MIN(pr.team)) AS team_mode,
            COALESCE(dt.team_color_major, MIN(pr.team_color)) AS team_color_major,
            COALESCE(dt.team_color2_major, MIN(pr.team_color2)) AS team_color2_major,
            lv.last_pct,
            ROW_NUMBER() OVER (ORDER BY lv.last_pct DESC NULLS LAST, tp.player_id) AS player_order
        FROM top_players tp
        LEFT JOIN dom_team dt ON dt.player_id = tp.player_id
        LEFT JOIN plot_rows pr ON pr.player_id = tp.player_id
        LEFT JOIN last_vals lv ON lv.player_id = tp.player_id
        GROUP BY tp.player_id, tp.name, dt.team_mode, dt.team_color_major, dt.team_color2_major, lv.last_pct
    )
    SELECT
        'SERIES' AS section,
        r.player_id, op.name, op.team_mode AS team, r.season, r.season_type, r.week,
        r.t_idx, r.pct, r.pct_roll,
        op.team_color_major, op.team_color2_major,
        op.player_order
    FROM roll_rows r
    JOIN ordered_players op USING (player_id)
    UNION ALL
    SELECT
        'PLAYERS' AS section,
        op.player_id, op.name, op.team_mode AS team, NULL::int, NULL::text, NULL::int,
        NULL::int, NULL::double precision, op.last_pct,
        op.team_color_major, op.team_color2_major,
        op.player_order
    FROM ordered_players op
    ORDER BY 1, 13, 7 NULLS FIRST;  -- section, player_order, t_idx
    """

    async with AsyncSessionLocal() as session:
        res = await session.execute(text(query), params)
        rows = [dict(r) for r in res.mappings().all()]

    series: list[dict] = []
    players: list[dict] = []
    for r in rows:
        sect = r.pop("section")
        if sect == "SERIES":
            series.append({
                "player_id": r["player_id"],
                "name": r["name"],
                "team": r["team"],
                "season": r["season"],
                "season_type": r["season_type"],
                "week": r["week"],
                "t_idx": r["t_idx"],
                "pct": r["pct"],
                "pct_roll": r["pct_roll"],
                "team_color": r["team_color_major"],
                "team_color2": r["team_color2_major"],
                "player_order": r["player_order"],
            })
        else:  # PLAYERS
            players.append({
                "player_id": r["player_id"],
                "name": r["name"],
                "team": r["team"],
                "team_color": r["team_color_major"],
                "team_color2": r["team_color2_major"],
                "last_pct": r["pct_roll"],   # selected as last_pct in SQL
                "player_order": r["player_order"],
            })

    payload = {
        "series": series,
        "players": sorted(players, key=lambda x: x["player_order"]),
        "meta": {
            "position": pos,
            "metric": metric,
            "metric_label": metric.replace("_", " ").title(),
            "stat_type": stype,
            "season_type": st,
            "seasons": seasons,
            "week_start": ws,
            "week_end": we,
            "top_n": top_n,
            "rolling_window": int(rolling_window),
        },
    }
    if debug:
        payload["debug"] = {
            "seasons_mv": mv_seasons,
            "seasons_raw": raw_seasons,
        }
    return payload
  
  
# === Team: Weekly Trajectories ====================================================
@router.get("/team/trajectories/{stat_name}/{top_n}")
async def get_team_weekly_trajectories(
    request: Request,
    stat_name: str,
    top_n: int,
    season_type: str = Query("REG", description="REG | POST | ALL"),
    week_start: int = Query(1, ge=1, le=22),
    week_end: int = Query(18, ge=1, le=22),
    rank_by: str = Query("sum", description="sum | mean"),
    stat_type: str = Query("base", description="base | cumulative"),
):
    """Top-N team weekly trajectories for a stat across selected seasons.
    
    Reads base weekly rows and optionally returns a cumulative view via window SUM.
    Top-N selection occurs per season using SUM/AVG over the filtered weeks.
    
    Path:
        /analytics_nexus/team/trajectories/{stat_name}/{top_n}
    
    Args:
        request (Request): For flexible ?seasons parsing.
        stat_name (str): Stat identifier in storage (e.g., "rushing_epa").
        top_n (int): 1..32.
        season_type (str, query): "REG" | "POST" | "ALL". Default "REG".
        week_start (int, query): 1..22, default 1.
        week_end (int, query): 1..22, default 18.
        rank_by (str, query): "sum" | "mean". Controls Top-N selection per season. Default "sum".
        stat_type (str, query): "base" (weekly) or "cumulative" (window SUM view). Default "base".
    
    Returns:
        List[dict]: Rows ordered by season, team_rank, week with keys:
            {
              "team": str, "season": int, "season_type": str, "week": int,
              "stat_name": str, "stat_type": "base",  # logical label; values may be cumulative when requested
              "value": float|None,
              "team_color": str, "team_color2": str,
              "team_rank": int,
              # optionally "is_highlight": bool when ?highlight=ALL or specific teams provided
            }
        If no data match, returns {"error": "No data found"}.
    
    Raises:
        HTTPException: 400 on invalid inputs or missing seasons.
    
    Notes:
        - Highlights: ?highlight=ALL flags all rows; ?highlight=KC&highlight=DET flags matches.
        - Series rendering uses a single "value" key; cumulative is computed on the fly.
    """
    st = _normalize_season_type(season_type)                # REG | POST | ALL
    series_mode = _normalize_series_type(stat_type)         # 'base' | 'cumulative'
    agg_func = _normalize_rank_by(rank_by)                  # SQL: SUM or AVG
    ws, we = _clamp_weeks(week_start, week_end)

    seasons = _parse_seasons_from_request(request)
    if not seasons:
        raise HTTPException(status_code=400, detail="Provide at least one season via ?seasons=YYYY[,YYYY]")

    n = int(top_n)
    if n < 1 or n > 32:
        raise HTTPException(status_code=400, detail="top_n must be between 1 and 32")

    # Optional highlight param(s): ALL or CSV / repeated tokens
    raw_h = request.query_params.getlist("highlight")
    hl_set = set()
    highlight_all = False
    for tok in raw_h:
        if not tok:
            continue
        s = str(tok).strip().upper()
        if not s:
            continue
        if s == "ALL":
            highlight_all = True
            hl_set.clear()
            break
        for part in s.split(","):
            p = part.strip().upper()
            if p:
                hl_set.add(p)

    sql = f"""
    WITH filtered AS (
        SELECT
            twt.team,
            twt.season,
            twt.season_type,
            twt.week,
            twt.stat_name,
            twt.value,                         -- base weekly value in storage
            COALESCE(tmt.team_color,  '#888888') AS team_color,
            COALESCE(tmt.team_color2, '#AAAAAA') AS team_color2
        FROM public.team_weekly_tbl twt
        LEFT JOIN public.team_metadata_tbl tmt
          ON twt.team = tmt.team_abbr
        WHERE twt.season = ANY(:seasons)
          AND (:season_type = 'ALL' OR twt.season_type = :season_type)
          AND twt.stat_name = :stat_name
          AND twt.stat_type = 'base'                -- always read base
          AND twt.week BETWEEN :week_start AND :week_end
    ),
    agg AS (
        SELECT
            season,
            team,
            {agg_func}(value) AS agg_value
        FROM filtered
        GROUP BY season, team
    ),
    ranked AS (
        SELECT
            season,
            team,
            RANK() OVER (PARTITION BY season ORDER BY agg_value DESC NULLS LAST, team) AS team_rank
        FROM agg
    ),
    selected AS (
        SELECT season, team, team_rank
        FROM ranked
        WHERE team_rank <= :top_n
    ),
    plot AS (
        SELECT
            f.season,
            f.season_type,
            f.week,
            f.team,
            f.stat_name,
            CASE
                WHEN :series_mode = 'cumulative'
                THEN SUM(COALESCE(f.value,0)) OVER (
                        PARTITION BY f.season, f.team
                        ORDER BY f.week
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     )
                ELSE f.value
            END AS value,                         -- return under a single 'value' key
            f.team_color,
            f.team_color2
        FROM filtered f
        JOIN selected s
          ON s.season = f.season AND s.team = f.team
    )
    SELECT
        p.team,
        p.season,
        p.season_type,
        p.week,
        p.stat_name,
        'base'::text AS stat_type,                -- logical view controlled by series_mode
        p.value,
        p.team_color,
        p.team_color2,
        s.team_rank
    FROM plot p
    JOIN selected s
      ON s.season = p.season AND s.team = p.team
    ORDER BY p.season, s.team_rank, p.week;
    """

    params = {
        "seasons": seasons,
        "season_type": st,
        "stat_name": str(stat_name),
        "week_start": ws,
        "week_end": we,
        "top_n": n,
        "series_mode": series_mode,
    }

    async with AsyncSessionLocal() as session:
        res = await session.execute(text(sql), params)
        rows = [dict(r) for r in res.mappings().all()]

    if not rows:
        return {"error": "No data found"}

    if highlight_all or hl_set:
        for r in rows:
            r["is_highlight"] = True if highlight_all or (str(r.get("team","")).upper() in hl_set) else False

    return rows

# === Team: Violins (consistency/volatility) ======================================
@router.get("/team/violins/{stat_name}/{top_n}")
async def get_team_violins(
    request: Request,
    stat_name: str,
    top_n: int,
    seasons: list[int] = Query(..., description="Repeatable ?seasons=YYYY"),
    season_type: str = Query("REG", description="REG | POST | ALL"),
    week_start: int = Query(1, ge=1, le=22),
    week_end: int = Query(18, ge=1, le=22),
    stat_type: str = Query("base", description="base | cumulative"),
    order_by: str = Query("rCV", description="rCV | IQR | median"),
    min_games_for_badges: int = Query(6, ge=0),
):
    """Team violin data (consistency/volatility) over pooled multi-season windows.
    
    Selects Top-N teams by pooled total of the effective series (base or cumulative)
    and returns weekly points plus dispersion summaries and badges.
    
    Path:
        /analytics_nexus/team/violins/{stat_name}/{top_n}
    
    Args:
        request (Request): For repeatable ?seasons parsing.
        stat_name (str): Stat identifier (team_weekly).
        top_n (int): 1..32.
        seasons (List[int], query): Required; repeatable ?seasons=YYYY.
        season_type (str, query): "REG" | "POST" | "ALL". Default "REG".
        week_start (int, query): 1..22, default 1.
        week_end (int, query): 1..22, default 18.
        stat_type (str, query): "base" | "cumulative". Default "base".
        order_by (str, query): "rCV" | "IQR" | "median". Default "rCV".
        min_games_for_badges (int, query): Badge eligibility minimum. Default 6.
    
    Returns:
        dict: {
          "weekly": [ {"team","season","week","value","team_color","team_color2"} ],
          "summary": [
            {"team","n_games","q25","q50","q75","IQR","MAD","rCV","small_n","team_color_major","team_order"}
          ],
          "badges": {"most_consistent": list| "—", "most_volatile": list| "—"},
          "meta": {
            "stat_name","stat_type","season_type","seasons",
            "week_start","week_end","order_by","top_n","min_games_for_badges"
          }
        }
        Empty results produce the same structure with empty arrays and "—" badges.
    
    Raises:
        HTTPException: 400 on invalid inputs or missing seasons.
    
    Notes:
        - Effective series `v_eff` = base or window-cumulative depending on `stat_type`.
        - Order metric aligns with UI semantics: median desc, or IQR asc, or rCV asc.
    """

    try:
        # --- Normalize inputs ---
        n = int(top_n)
        if n < 1 or n > 32:
            raise HTTPException(status_code=400, detail="top_n must be between 1 and 32")

        st = _normalize_season_type(season_type)      # REG | POST | ALL
        series_mode = _normalize_series_type(stat_type)  # 'base' | 'cumulative'
        ws, we = _clamp_weeks(week_start, week_end)

        # seasons from query param (repeatable)
        if not seasons:
            raise HTTPException(status_code=400, detail="Provide at least one ?seasons=YYYY")
        seasons = sorted({int(s) for s in seasons})

        ob = (order_by or "rCV").strip()
        if ob.lower() not in {"rcv", "iqr", "median"}:
            raise HTTPException(status_code=400, detail="order_by must be one of rCV, IQR, median")
        # canonical casing
        ob = "rCV" if ob.lower() == "rcv" else ("IQR" if ob.lower() == "iqr" else "median")

        sql = """
        WITH filtered AS (
            SELECT
                twt.team,
                twt.season,
                twt.season_type,
                twt.week,
                twt.stat_name,
                -- base weekly from storage
                twt.value AS base_value,
                -- effective series value (base or season-to-date cumulative)
                CASE
                    WHEN :series_mode = 'cumulative'
                    THEN SUM(COALESCE(twt.value,0)) OVER (
                            PARTITION BY twt.season, twt.team
                            ORDER BY twt.week
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                         )
                    ELSE twt.value
                END AS v_eff,
                COALESCE(tmt.team_color,  '#888888') AS team_color,
                COALESCE(tmt.team_color2, '#AAAAAA') AS team_color2
            FROM public.team_weekly_tbl twt
            LEFT JOIN public.team_metadata_tbl tmt
              ON twt.team = tmt.team_abbr
            WHERE twt.season = ANY(:seasons)
              AND (:season_type = 'ALL' OR twt.season_type = :season_type)
              AND twt.stat_name = :stat_name
              AND twt.stat_type = 'base'      -- always read base; compute cum via window
              AND twt.week BETWEEN :week_start AND :week_end
        ),
        top_pool AS (
            -- Top-N teams by pooled total of effective series across all selected seasons+weeks
            SELECT team
            FROM (
                SELECT team, SUM(COALESCE(v_eff,0)) AS total_value
                FROM filtered
                GROUP BY team
            ) t
            ORDER BY total_value DESC, team
            LIMIT :top_n
        ),
        weekly AS (
            -- Weekly points for plotting (only Top-N teams)
            SELECT
                f.team, f.season, f.week,
                f.v_eff AS value,
                f.team_color, f.team_color2
            FROM filtered f
            JOIN top_pool tp ON tp.team = f.team
            WHERE f.v_eff IS NOT NULL
        ),
        dominant_color AS (
            -- "Mode" color per team across pooled window (defensive if color ever varies)
            SELECT team, team_color AS team_color_major
            FROM (
                SELECT
                    team, team_color,
                    COUNT(*) AS cnt,
                    ROW_NUMBER() OVER (PARTITION BY team ORDER BY COUNT(*) DESC, team_color) AS rn
                FROM weekly
                GROUP BY team, team_color
            ) x
            WHERE rn = 1
        ),
        quantiles AS (
            -- Per-team n, q25, q50, q75 using continuous percentiles
            SELECT
                team,
                COUNT(value)                       AS n_games,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) AS q25,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY value) AS q50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) AS q75
            FROM weekly
            GROUP BY team
        ),
        dev AS (
            -- Absolute deviations from team median
            SELECT w.team, ABS(w.value - q.q50) AS dev
            FROM weekly w
            JOIN quantiles q USING (team)
        ),
        mad AS (
            -- Median absolute deviation (unscaled)
            SELECT
                team,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY dev) AS mad
            FROM dev
            GROUP BY team
        ),
        summary AS (
            SELECT
                q.team,
                q.n_games,
                q.q25, q.q50, q.q75,
                (q.q75 - q.q25)                         AS iqr,
                m.mad,
                CASE
                    WHEN q.q50 IS NULL OR q.q50 = 0 THEN NULL
                    ELSE m.mad / ABS(q.q50)
                END AS rcv,
                (q.n_games < :min_games_for_badges)     AS small_n
            FROM quantiles q
            LEFT JOIN mad m USING (team)
        ),
        ordered AS (
            -- Compute an order metric that matches the UI semantics
            SELECT
                s.team,
                CASE
                    WHEN :order_by = 'median' THEN (-1) * COALESCE(s.q50, 0)        -- descending median
                    WHEN :order_by = 'IQR'    THEN COALESCE(s.iqr, 1e18)            -- ascending IQR
                    ELSE                              -- rCV ascending (NA -> last)
                        CASE WHEN s.rcv IS NULL OR NOT (s.rcv = s.rcv) THEN 1e18 ELSE s.rcv END
                END AS order_metric
            FROM summary s
        ),
        team_order AS (
            SELECT team,
                   ROW_NUMBER() OVER (ORDER BY order_metric ASC, team) AS team_order
            FROM ordered
        )
        SELECT
            -- Tag rows for the JSON builder using a kind discriminator
            'weekly'::text AS kind,
            w.team, w.season, w.week, w.value,
            w.team_color, w.team_color2,
            NULL::int AS n_games, NULL::float AS q25, NULL::float AS q50, NULL::float AS q75,
            NULL::float AS iqr, NULL::float AS mad, NULL::float AS rcv,
            NULL::boolean AS small_n,
            NULL::text AS team_color_major,
            NULL::int AS team_order
        FROM weekly w
        UNION ALL
        SELECT
            'summary'::text AS kind,
            s.team, NULL::int AS season, NULL::int AS week, NULL::float AS value,
            NULL::text AS team_color, NULL::text AS team_color2,
            s.n_games, s.q25, s.q50, s.q75,
            s.iqr, s.mad, s.rcv,
            s.small_n,
            dc.team_color_major,
            todr.team_order
        FROM summary s
        LEFT JOIN dominant_color dc USING (team)
        LEFT JOIN team_order todr USING (team)
        ORDER BY kind, team, season, week;
        """

        params = {
            "seasons": seasons,
            "season_type": st,
            "stat_name": str(stat_name),
            "week_start": ws,
            "week_end": we,
            "series_mode": series_mode,                # base | cumulative
            "top_n": n,
            "order_by": ob,                            # rCV | IQR | median
            "min_games_for_badges": int(min_games_for_badges),
        }

        async with AsyncSessionLocal() as session:
            res = await session.execute(text(sql), params)
            rows = [dict(r) for r in res.mappings().all()]

        if not rows:
            return {
                "weekly": [],
                "summary": [],
                "badges": {"most_consistent": "—", "most_volatile": "—"},
                "meta": {
                    "stat_name": stat_name,
                    "stat_type": series_mode,
                    "season_type": st,
                    "seasons": seasons,
                    "week_start": ws,
                    "week_end": we,
                    "order_by": ob,
                    "top_n": n,
                    "min_games_for_badges": int(min_games_for_badges),
                },
            }

        # ---- Split into weekly / summary and build badges ----
        weekly = []
        summary = []
        for r in rows:
            if r["kind"] == "weekly":
                weekly.append({
                    "team": r["team"],
                    "season": r["season"],
                    "week": r["week"],
                    "value": float(r["value"]) if r["value"] is not None else None,
                    "team_color": r["team_color"],
                    "team_color2": r["team_color2"],
                })
            else:
                summary.append({
                    "team": r["team"],
                    "n_games": int(r["n_games"]) if r["n_games"] is not None else 0,
                    "q25": float(r["q25"]) if r["q25"] is not None else None,
                    "q50": float(r["q50"]) if r["q50"] is not None else None,
                    "q75": float(r["q75"]) if r["q75"] is not None else None,
                    "IQR": float(r["iqr"]) if r["iqr"] is not None else None,
                    "MAD": float(r["mad"]) if r["mad"] is not None else None,
                    "rCV": float(r["rcv"]) if r["rcv"] is not None else None,
                    "small_n": bool(r["small_n"]) if r["small_n"] is not None else False,
                    "team_color_major": r.get("team_color_major") or "#888888",
                    "team_order": int(r["team_order"]) if r["team_order"] is not None else 10**9,
                })

        # badges: among adequate n (not small_n) with finite rCV
        pool = [s for s in summary if not s["small_n"] and s.get("rCV") is not None]
        most_consistent = [s["team"] for s in sorted(pool, key=lambda x: (x["rCV"], x["team"]))[:3]] or "—"
        most_volatile   = [s["team"] for s in sorted(pool, key=lambda x: (-x["rCV"], x["team"]))[:3]] or "—"

        payload = {
            "weekly": weekly,
            "summary": sorted(summary, key=lambda s: s.get("team_order", 10**9)),
            "badges": {
                "most_consistent": most_consistent,
                "most_volatile": most_volatile,
            },
            "meta": {
                "stat_name": stat_name,
                "stat_type": series_mode,
                "season_type": st,
                "seasons": seasons,
                "week_start": ws,
                "week_end": we,
                "order_by": ob,
                "top_n": n,
                "min_games_for_badges": int(min_games_for_badges),
            },
        }
        return payload

    except HTTPException:
        raise
    except Exception as e:
        # Surface a structured empty payload on error to keep the UI stable
        return {
            "weekly": [],
            "summary": [],
            "badges": {"most_consistent": "—", "most_volatile": "—"},
            "meta": {
                "stat_name": stat_name,
                "stat_type": (stat_type or "base"),
                "season_type": (season_type or "REG"),
                "seasons": seasons if seasons else [],
                "week_start": week_start,
                "week_end": week_end,
                "order_by": order_by,
                "top_n": top_n,
                "min_games_for_badges": min_games_for_badges,
                "error": str(e),
            },
        }
        
        
# ===== Team Quadrant Scatter — helpers ===========================================
def _team_metric_plan(metric: str) -> dict:
    """
    Return a metric plan for team-level scatter:
      - label: pretty axis label
      - required: list of stat_name identifiers to pull (SUM(value) FILTER WHERE stat_name=...)
      - value: SQL expression computed from wide SUM columns (ratio-of-sums for rates)
      - gate: SQL expression for ranking/gating (magnitude proxy; usually a volume or abs(value))
    """
    m = (metric or "").strip()

    derived = {
        # Passing (ratio of sums)
        "completion_pct": dict(
            label="Completion %",
            required=["completions", "attempts"],
            value="CASE WHEN COALESCE(attempts,0) > 0 "
                  "THEN COALESCE(completions,0)::double precision / COALESCE(attempts,0) "
                  "ELSE NULL END",
            gate="COALESCE(attempts,0)"
        ),
        "yards_per_attempt": dict(
            label="Yards per Attempt",
            required=["passing_yards", "attempts"],
            value="CASE WHEN COALESCE(attempts,0) > 0 "
                  "THEN COALESCE(passing_yards,0)::double precision / COALESCE(attempts,0) "
                  "ELSE NULL END",
            gate="COALESCE(attempts,0)"
        ),
        "passing_epa_per_dropback": dict(
            label="EPA per Dropback",
            required=["attempts", "sacks", "passing_epa"],
            value="CASE WHEN (COALESCE(attempts,0)+COALESCE(sacks,0)) > 0 "
                  "THEN COALESCE(passing_epa,0)::double precision / (COALESCE(attempts,0)+COALESCE(sacks,0)) "
                  "ELSE NULL END",
            gate="COALESCE(passing_epa,0)"
        ),
        "passing_anya": dict(
            label="ANY/A",
            required=["attempts","sacks","sack_yards","passing_yards","passing_tds","interceptions"],
            value=("CASE WHEN (COALESCE(attempts,0)+COALESCE(sacks,0)) > 0 "
                   "THEN (COALESCE(passing_yards,0) + 20*COALESCE(passing_tds,0) "
                   "- 45*COALESCE(interceptions,0) - COALESCE(sack_yards,0))::double precision "
                   "/ (COALESCE(attempts,0)+COALESCE(sacks,0)) "
                   "ELSE NULL END"),
            gate="COALESCE(passing_yards,0)"
        ),
        "sack_rate": dict(
            label="Sack Rate",
            required=["attempts","sacks"],
            value=("CASE WHEN (COALESCE(attempts,0)+COALESCE(sacks,0)) > 0 "
                   "THEN COALESCE(sacks,0)::double precision / (COALESCE(attempts,0)+COALESCE(sacks,0)) "
                   "ELSE NULL END"),
            gate="COALESCE(sacks,0)"
        ),
        "interception_rate": dict(
            label="INT Rate",
            required=["interceptions","attempts"],
            value="CASE WHEN COALESCE(attempts,0) > 0 "
                  "THEN COALESCE(interceptions,0)::double precision / COALESCE(attempts,0) "
                  "ELSE NULL END",
            gate="COALESCE(interceptions,0)"
        ),

        # Rushing / Receiving
        "yards_per_carry": dict(
            label="Yards per Carry",
            required=["rushing_yards","carries"],
            value="CASE WHEN COALESCE(carries,0) > 0 "
                  "THEN COALESCE(rushing_yards,0)::double precision / COALESCE(carries,0) "
                  "ELSE NULL END",
            gate="COALESCE(carries,0)"
        ),
        "receiving_epa_per_target": dict(
            label="EPA per Target",
            required=["targets","receiving_epa"],
            value="CASE WHEN COALESCE(targets,0) > 0 "
                  "THEN COALESCE(receiving_epa,0)::double precision / COALESCE(targets,0) "
                  "ELSE NULL END",
            gate="COALESCE(receiving_epa,0)"
        ),

        # Blended usage/efficiency (team-wide opportunities)
        "total_epa_per_opportunity": dict(
            label="Total EPA per Opportunity",
            required=["attempts","sacks","carries","targets","passing_epa","rushing_epa","receiving_epa"],
            value=("CASE WHEN (COALESCE(attempts,0)+COALESCE(sacks,0)+COALESCE(carries,0)+COALESCE(targets,0)) > 0 "
                   "THEN (COALESCE(passing_epa,0)+COALESCE(rushing_epa,0)+COALESCE(receiving_epa,0))::double precision "
                   "/ (COALESCE(attempts,0)+COALESCE(sacks,0)+COALESCE(carries,0)+COALESCE(targets,0)) "
                   "ELSE NULL END"),
            gate="(COALESCE(passing_epa,0)+COALESCE(rushing_epa,0)+COALESCE(receiving_epa,0))"
        ),
        "yards_per_opportunity": dict(
            label="Yards per Opportunity",
            required=["attempts","sacks","carries","targets","passing_yards","rushing_yards","receiving_yards"],
            value=("CASE WHEN (COALESCE(attempts,0)+COALESCE(sacks,0)+COALESCE(carries,0)+COALESCE(targets,0)) > 0 "
                   "THEN (COALESCE(passing_yards,0)+COALESCE(rushing_yards,0)+COALESCE(receiving_yards,0))::double precision "
                   "/ (COALESCE(attempts,0)+COALESCE(sacks,0)+COALESCE(carries,0)+COALESCE(targets,0)) "
                   "ELSE NULL END"),
            gate="(COALESCE(passing_yards,0)+COALESCE(rushing_yards,0)+COALESCE(receiving_yards,0))"
        ),
    }

    if m in derived:
        return derived[m]

    # raw-sum fallback
    nice = m.replace("_", " ").title()
    ident = m
    return dict(
        label=nice,
        required=[ident],
        value=f"COALESCE({ident},0)::double precision",
        gate=f"ABS(COALESCE({ident},0))"
    )

# === Team: Quadrant Scatter =======================================================
@router.get("/team/scatter/{metric_x}/{metric_y}/{top_n}")
async def get_team_scatter_quadrants(
    request: Request,
    metric_x: str,
    metric_y: str,
    top_n: int,
    season_type: str = Query("REG", description="REG | POST | ALL"),
    week_start: int = Query(1, ge=1, le=22),
    week_end: int = Query(18, ge=1, le=22),
    stat_type: str = Query("base", description="Use 'base' (weekly) values only"),
    top_by: str = Query("combined", description="combined | x_gate | y_gate | x_value | y_value"),
    log_x: bool | str = Query(False),
    log_y: bool | str = Query(False),
    label_all_points: bool | str = Query(True),
    debug: bool = Query(False),
):
    """Quadrant scatter for teams with derived/raw X/Y metrics.
    
    Aggregates weekly *base* values into ratio-of-sums metrics, applies optional
    log filters (x>0 / y>0) before ranking, selects Top-N by `top_by`, and returns
    points with global medians.
    
    Path:
        /analytics_nexus/team/scatter/{metric_x}/{metric_y}/{top_n}
    
    Args:
        request (Request): For repeatable ?seasons parsing.
        metric_x (str): Metric identifier for X (see `_team_metric_plan`).
        metric_y (str): Metric identifier for Y.
        top_n (int): 1..32.
        season_type (str, query): "REG" | "POST" | "ALL". Default "REG".
        week_start (int, query): 1..22, default 1.
        week_end (int, query): 1..22, default 18.
        stat_type (str, query): Must be "base". Enforced.
        top_by (str, query): {"combined","x_gate","y_gate","x_value","y_value"}. Default "combined".
        log_x (bool|str, query): Truthy strings accepted; filters require x>0.
        log_y (bool|str, query): Truthy strings accepted; filters require y>0.
        label_all_points (bool|str, query): Passed through in meta.
        debug (bool, query): Adds light debug to meta when true.
    
    Returns:
        dict: {
          "points": [ {"team","team_color","team_color2","x_value","y_value"} ],
          "meta": {
            "metric_x","metric_y","label_x","label_y",
            "seasons","season_type","week_start","week_end","stat_type",
            "top_by","top_n","log_x","log_y","label_all_points",
            "median_x","median_y"
          }
        }
        Empty result returns points=[], medians None.
    
    Raises:
        HTTPException: 400 on invalid inputs or missing seasons.
    
    Notes:
        - Required stats are derived from both metric plans; aggregation is in SQL.
        - Top-N order: rank_key, then gate_total, then (x+y), then team.
    """

    # Normalise params
    st = _normalize_season_type(season_type)
    ws, we = _clamp_weeks(week_start, week_end)
    series_type = _normalize_series_type(stat_type)
    if series_type != "base":
        raise HTTPException(status_code=400, detail="stat_type must be 'base' for scatter metrics")
    tb = _normalize_top_by(top_by)
    lx = _normalize_bool(log_x)
    ly = _normalize_bool(log_y)
    lap = _normalize_bool(label_all_points)

    # Seasons
    seasons = _parse_seasons_from_request(request)
    if not seasons:
        raise HTTPException(status_code=400, detail="Provide at least one season via ?seasons=YYYY")

    # Top-N guard
    n = int(top_n)
    if n < 1 or n > 32:
        raise HTTPException(status_code=400, detail="top_n must be between 1 and 32")

    # Metric plans
    plan_x = _team_metric_plan(metric_x)
    plan_y = _team_metric_plan(metric_y)
    label_x, label_y = plan_x["label"], plan_y["label"]
    required_stats = sorted(set(plan_x["required"] + plan_y["required"]))

    # Build filtered source
    params: dict = {
        "seasons": seasons,
        "season_type": st,
        "stat_type": "base",
        "week_start": ws,
        "week_end": we,
        "required_stats": required_stats,
        "top_by": tb,
        "top_n": n,
    }

    filtered_sql = """
        SELECT
          twt.team,
          twt.season,
          twt.season_type,
          twt.week,
          twt.stat_name,
          twt.value,
          COALESCE(tmt.team_color,  '#888888') AS team_color,
          COALESCE(tmt.team_color2, '#AAAAAA') AS team_color2
        FROM public.team_weekly_tbl twt
        LEFT JOIN public.team_metadata_tbl tmt
          ON twt.team = tmt.team_abbr
        WHERE twt.season = ANY(:seasons)
          AND (:season_type = 'ALL' OR twt.season_type = :season_type)
          AND twt.stat_type = :stat_type
          AND twt.week BETWEEN :week_start AND :week_end
          AND twt.stat_name = ANY(:required_stats)
    """

    # Dynamic SUM(...) FILTER columns (per required stat)
    stat_sums = []
    for i, stat in enumerate(required_stats):
        key = f"stat_{i}"
        params[key] = stat
        stat_sums.append(f"SUM(value) FILTER (WHERE stat_name = :{key}) AS {stat}")
    sums_sql = ",\n              ".join(stat_sums)

    # Main SQL pipeline
    query = f"""
    WITH filtered AS (
        {filtered_sql}
    ),
    wide AS (
        SELECT
          team,
          COALESCE(MAX(team_color),  '#888888') AS team_color,
          COALESCE(MAX(team_color2), '#AAAAAA') AS team_color2,
          {sums_sql}
        FROM filtered
        GROUP BY team
    ),
    metrics AS (
        SELECT
          team, team_color, team_color2,
          {plan_x["value"]} AS x_value,
          {plan_x["gate"]}  AS gate_x,
          {plan_y["value"]} AS y_value,
          {plan_y["gate"]}  AS gate_y
        FROM wide
    ),
    filtered_metrics AS (
        SELECT *
        FROM metrics
        WHERE x_value IS NOT NULL AND y_value IS NOT NULL
          { "AND x_value > 0" if lx else "" }
          { "AND y_value > 0" if ly else "" }
    ),
    ranked AS (
        SELECT
          *,
          (COALESCE(gate_x,0) + COALESCE(gate_y,0)) AS gate_total,
          CASE
            WHEN :top_by = 'combined' THEN (COALESCE(gate_x,0) + COALESCE(gate_y,0))
            WHEN :top_by = 'x_gate'   THEN COALESCE(gate_x,0)
            WHEN :top_by = 'y_gate'   THEN COALESCE(gate_y,0)
            WHEN :top_by = 'x_value'  THEN COALESCE(x_value,0)
            ELSE COALESCE(y_value,0)
          END AS rank_key
        FROM filtered_metrics
    ),
    topn AS (
        SELECT *
        FROM ranked
        ORDER BY rank_key DESC, gate_total DESC,
                 (COALESCE(x_value,0)+COALESCE(y_value,0)) DESC, team
        LIMIT :top_n
    ),
    medians AS (
        SELECT
          percentile_cont(0.5) WITHIN GROUP (ORDER BY x_value) AS med_x,
          percentile_cont(0.5) WITHIN GROUP (ORDER BY y_value) AS med_y
        FROM topn
    )
    SELECT
      t.team, t.team_color, t.team_color2,
      t.x_value, t.y_value,
      m.med_x, m.med_y
    FROM topn t
    CROSS JOIN medians m
    ORDER BY t.rank_key DESC, t.gate_total DESC,
             (COALESCE(t.x_value,0)+COALESCE(t.y_value,0)) DESC, t.team;
    """

    async with AsyncSessionLocal() as session:
        res = await session.execute(text(query), params)
        rows = [dict(r) for r in res.mappings().all()]

    if not rows:
        return {
            "points": [],
            "meta": {
                "metric_x": metric_x, "metric_y": metric_y,
                "label_x": label_x, "label_y": label_y,
                "seasons": seasons, "season_type": st,
                "week_start": ws, "week_end": we,
                "stat_type": "base",
                "top_by": tb, "top_n": n,
                "log_x": lx, "log_y": ly,
                "label_all_points": lap,
                "median_x": None, "median_y": None,
            },
        }

    med_x = rows[0].get("med_x")
    med_y = rows[0].get("med_y")
    points = [
        {
            "team": r["team"],
            "team_color": r["team_color"],
            "team_color2": r["team_color2"],
            "x_value": r["x_value"],
            "y_value": r["y_value"],
        }
        for r in rows
    ]

    return {
        "points": points,
        "meta": {
            "metric_x": metric_x, "metric_y": metric_y,
            "label_x": label_x, "label_y": label_y,
            "seasons": seasons, "season_type": st,
            "week_start": ws, "week_end": we,
            "stat_type": "base",
            "top_by": tb, "top_n": n,
            "log_x": lx, "log_y": ly,
            "label_all_points": lap,
            "median_x": med_x, "median_y": med_y,
        },
    }
    
# === Team: Rolling Percentiles (sparkline grid) ==================================
@router.get("/team/rolling_percentiles/{metric}/{top_n}")
async def get_team_rolling_percentiles(
    request: Request,
    metric: str,                              # stat_name in storage (e.g., "rushing_epa", "passing_yards")
    top_n: int,
    seasons: List[int] = Query(..., description="Repeatable ?seasons=YYYY"),
    season_type: str = Query("REG", description="REG | POST | ALL"),
    stat_type: str = Query("base", description="base | cumulative"),
    week_start: int = Query(1, ge=1, le=22),
    week_end: int = Query(18, ge=1, le=22),
    rolling_window: int = Query(4, ge=1),
    debug: Optional[bool] = Query(False),
):
    """Rolling form percentiles for Top-N teams across seasons × weeks.
    
    Filters team_weekly for a chosen metric, computes an effective series
    (base or cumulative), selects Top-N teams by pooled total, then computes
    weekly percentiles among those Top-N. Builds a unified time index and
    a rolling mean of percentiles over `rolling_window`.
    
    Path:
        /analytics_nexus/team/rolling_percentiles/{metric}/{top_n}
    
    Args:
        request (Request): For repeatable ?seasons parsing.
        metric (str): Stat identifier in team storage (e.g., "passing_yards").
        top_n (int): 1..32.
        seasons (List[int], query): Required; repeatable ?seasons=YYYY.
        season_type (str, query): "REG" | "POST" | "ALL". Default "REG".
        stat_type (str, query): "base" | "cumulative". Default "base".
        week_start (int, query): 1..22, default 1.
        week_end (int, query): 1..22, default 18.
        rolling_window (int, query): Window size k for rolling mean. Default 4.
        debug (bool, query): When true, includes light debug fields.
    
    Returns:
        dict: {
          "series": [
            {"team","season","season_type","week","t_idx","pct","pct_roll",
             "team_color","team_color2","team_order"}
          ],
          "teams": [
            {"team","team_color","team_color2","last_pct","team_order"}
          ],
          "meta": {
            "metric","metric_label","stat_type","season_type","seasons",
            "week_start","week_end","top_n","rolling_window"
          }
        }
        If no rows, returns empty arrays with meta.
    
    Raises:
        HTTPException: 400 on invalid inputs or missing seasons.
    
    Notes:
        - Unified timeline orders REG before POST within each season.
        - Panels ordered by last rolling percentile (desc) with team name tiebreaks in SQL.
    """

    # --- Normalize/validate ---
    st = _normalize_season_type(season_type)
    series_mode = _normalize_series_type(stat_type)  # 'base' | 'cumulative'
    ws, we = _clamp_weeks(week_start, week_end)

    if not seasons:
        raise HTTPException(status_code=400, detail="Provide at least one ?seasons=YYYY")
    seasons = sorted({int(s) for s in seasons})

    n = int(top_n)
    if n < 1 or n > 32:
        raise HTTPException(status_code=400, detail="top_n must be between 1 and 32")

    k = int(rolling_window)
    if k < 1:
        raise HTTPException(status_code=400, detail="rolling_window must be >= 1")

    # --- SQL pipeline (mirrors R semantics) ---
    # Notes:
    # - Always reads base rows; computes season-to-date when series_mode='cumulative'
    # - Top-N by pooled SUM(v_eff) across seasons×weeks×types
    # - Percentile per (season, season_type, week) among Top-N
    # - Rolling mean over k within (team, season, season_type), ordered by unified t_idx
    query = f"""
    WITH filtered AS (
        SELECT
            twt.team,
            twt.season,
            twt.season_type,
            twt.week,
            twt.stat_name,
            twt.value AS base_value,
            CASE
                WHEN :series_mode = 'cumulative'
                THEN SUM(COALESCE(twt.value,0)) OVER (
                        PARTITION BY twt.season, twt.team
                        ORDER BY twt.week
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     )
                ELSE twt.value
            END AS v_eff,
            COALESCE(tmt.team_color,  '#888888') AS team_color,
            COALESCE(tmt.team_color2, '#AAAAAA') AS team_color2
        FROM public.team_weekly_tbl twt
        LEFT JOIN public.team_metadata_tbl tmt
          ON twt.team = tmt.team_abbr
        WHERE twt.season = ANY(:seasons)
          AND (:season_type = 'ALL' OR twt.season_type = :season_type)
          AND twt.stat_name = :metric_name
          AND twt.stat_type = 'base'      -- read base; 'cumulative' computed above
          AND twt.week BETWEEN :week_start AND :week_end
    ),
    top_teams AS (
        SELECT team
        FROM (
            SELECT team, SUM(COALESCE(v_eff,0)) AS total_value
            FROM filtered
            GROUP BY team
        ) t
        ORDER BY total_value DESC, team
        LIMIT :top_n
    ),
    plot_rows AS (
        SELECT f.*
        FROM filtered f
        JOIN top_teams tt ON tt.team = f.team
        WHERE f.v_eff IS NOT NULL
    ),
    -- unified time index with REG before POST
    time_map AS (
        SELECT season, season_type, week,
               ROW_NUMBER() OVER (
                 ORDER BY season, CASE WHEN season_type='REG' THEN 1 ELSE 2 END, week
               ) AS t_idx
        FROM (SELECT DISTINCT season, season_type, week FROM plot_rows) u
    ),
    base_pct AS (
        SELECT
            pr.team, pr.season, pr.season_type, pr.week, pr.v_eff AS value,
            tm.t_idx,
            COUNT(*) OVER (PARTITION BY pr.season, pr.season_type, pr.week) AS n_obs,
            PERCENT_RANK() OVER (
                PARTITION BY pr.season, pr.season_type, pr.week
                ORDER BY pr.v_eff
            ) * 100.0 AS pct_raw,
            pr.team_color, pr.team_color2
        FROM plot_rows pr
        JOIN time_map tm USING (season, season_type, week)
    ),
    pct_rows AS (
        SELECT
            team, season, season_type, week, t_idx,
            CASE WHEN n_obs > 1 THEN pct_raw ELSE 50.0 END AS pct,
            team_color, team_color2
        FROM base_pct
    ),
    roll_rows AS (
        SELECT
            p.*,
            AVG(p.pct) OVER (
                PARTITION BY p.team, p.season, p.season_type
                ORDER BY p.t_idx
                ROWS BETWEEN {k - 1} PRECEDING AND CURRENT ROW
            ) AS pct_roll
        FROM pct_rows p
    ),
    last_idx AS (
        SELECT team, MAX(t_idx) AS last_t FROM roll_rows GROUP BY team
    ),
    last_vals AS (
        SELECT r.team, r.pct_roll AS last_pct
        FROM roll_rows r
        JOIN last_idx li ON li.team = r.team AND li.last_t = r.t_idx
    ),
    dom_color AS (
        SELECT team, team_color AS team_color_major, team_color2 AS team_color2_major
        FROM (
            SELECT
                team, team_color, team_color2,
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY COUNT(*) DESC, team_color) AS rn
            FROM roll_rows
            GROUP BY team, team_color, team_color2
        ) z
        WHERE rn = 1
    ),
    ordered_teams AS (
        SELECT
            r.team,
            COALESCE(dc.team_color_major,  '#888888') AS team_color_major,
            COALESCE(dc.team_color2_major, '#AAAAAA') AS team_color2_major,
            lv.last_pct,
            ROW_NUMBER() OVER (ORDER BY lv.last_pct DESC NULLS LAST, r.team) AS team_order
        FROM (SELECT DISTINCT team FROM roll_rows) r
        LEFT JOIN dom_color dc USING (team)
        LEFT JOIN last_vals lv USING (team)
    )
    SELECT
        'SERIES' AS section,
        rr.team, rr.season, rr.season_type, rr.week,
        rr.t_idx, rr.pct, rr.pct_roll,
        ot.team_color_major, ot.team_color2_major,
        ot.team_order
    FROM roll_rows rr
    JOIN ordered_teams ot USING (team)
    UNION ALL
    SELECT
        'TEAMS' AS section,
        ot.team, NULL::int, NULL::text, NULL::int,
        NULL::int, NULL::double precision, ot.last_pct,
        ot.team_color_major, ot.team_color2_major,
        ot.team_order
    FROM ordered_teams ot
    ORDER BY 1, 10, 6 NULLS FIRST;  -- section, team_order, t_idx
    """

    params = {
        "seasons": seasons,
        "season_type": st,
        "metric_name": metric,
        "series_mode": series_mode,
        "week_start": ws,
        "week_end": we,
        "top_n": n,
    }

    async with AsyncSessionLocal() as session:
        res = await session.execute(text(query), params)
        rows = [dict(r) for r in res.mappings().all()]

    if not rows:
        return {
            "series": [],
            "teams": [],
            "meta": {
                "metric": metric,
                "metric_label": metric.replace("_", " ").title(),
                "stat_type": series_mode,
                "season_type": st,
                "seasons": seasons,
                "week_start": ws,
                "week_end": we,
                "top_n": n,
                "rolling_window": k,
            },
        }

    # --- Split rows into series / teams and shape payload ---
    series: list[dict] = []
    teams: list[dict] = []
    for r in rows:
        sect = r.pop("section")
        if sect == "SERIES":
            series.append({
                "team": r["team"],
                "season": r["season"],
                "season_type": r["season_type"],
                "week": r["week"],
                "t_idx": r["t_idx"],
                "pct": r["pct"],
                "pct_roll": r["pct_roll"],
                "team_color": r["team_color_major"],
                "team_color2": r["team_color2_major"],
                "team_order": r["team_order"],
            })
        else:  # TEAMS
            teams.append({
                "team": r["team"],
                "team_color": r["team_color_major"],
                "team_color2": r["team_color2_major"],
                "last_pct": r["pct_roll"],   # selected as last_pct in SQL
                "team_order": r["team_order"],
            })

    payload = {
        "series": series,
        "teams": sorted(teams, key=lambda x: x["team_order"]),
        "meta": {
            "metric": metric,
            "metric_label": metric.replace("_", " ").title(),
            "stat_type": series_mode,
            "season_type": st,
            "seasons": seasons,
            "week_start": ws,
            "week_end": we,
            "top_n": n,
            "rolling_window": k,
        },
    }
    if debug:
        payload["debug"] = {"sql_k": k, "rowcount": len(rows)}
    return payload

