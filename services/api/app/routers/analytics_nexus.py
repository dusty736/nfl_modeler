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
