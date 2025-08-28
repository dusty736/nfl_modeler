from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Request
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
    """
    Consistency/Volatility Violin data for Top-N players over a multi-season window.
    Mirrors the R function's selection, ordering, badges, and NA handling exactly.
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
  
  # ===== Quadrant Scatter helpers =====

ALLOWED_TOP_BY = {"combined", "x_gate", "y_gate", "x_value", "y_value"}

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

# ===== Quadrant Scatter endpoint =====

from fastapi import Query, Request

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
    """
    Player Quadrant Scatter:
      - Multi-season window (?seasons=YYYY[,YYYY] or repeatable)
      - Aggregates weekly *base* values, computes derived/raw metrics for X/Y
      - Top-N selection by `top_by` (combined gate, gates, or values)
      - Optional log filters drop non-positive values pre-selection
      - Returns points + medians and labels in meta
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
    """
    Rolling Form Percentiles for Top-N players over a pooled multi-season window.
    Matches the R implementation:
      - Top-N players by SUM(value) of the chosen metric across the window.
      - Percentiles computed per (season, season_type, week) *among those Top-N*.
      - Rolling mean over 'rolling_window' within (player, season, season_type).
      - Player panels ordered by last rolling percentile (desc).
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
  
  
# ===== Teams — Weekly Trajectories =====
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
    """
    Top-N team weekly trajectories (per season).
    - Always reads base rows from storage.
    - If stat_type='cumulative', returns season-to-date via window SUM (monotonic).
    - Top-N selection is per season by SUM/AVG of base values over the filtered weeks.
    - Colors come from team_metadata_tbl (team_color, team_color2).
    - Supports ?highlight=ALL or ?highlight=KC&highlight=DET (adds is_highlight flag).
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
