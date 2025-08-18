from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.db import AsyncSessionLocal

router = APIRouter(prefix="/team_stats", tags=["team_stats"])

@router.get("/{team_abbr}/record/{season}/{week}")
async def get_team_record(season: int, week: int, team_abbr: str):
    query = """
        SELECT
            t.wins,
            t.losses,
            t.ties,
            t.points_for as points_scored,
            t.points_against as points_allowed,
            t.point_diff as point_differential
        FROM public.season_results_tbl t
        WHERE t.season = :season
          AND t.team_id   = :team_abbr;
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"season": season, "week": week, "team_abbr": team_abbr.upper()}
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Record not found")
        return dict(row)


@router.get("/{team_abbr}/offense/{season}/{week}")
async def get_team_offense(team_abbr: str, season: int, week: int):
    query = """
    (SELECT
        CAST(o.week AS TEXT) AS week,
        COALESCE(SUM(o.passing_yards), 0)   AS passing_yards,
        COALESCE(SUM(o.rushing_yards), 0)   AS rushing_yards,
        COALESCE(SUM(o.receiving_yards), 0) AS receiving_yards
    FROM public.off_team_stats_week_tbl o
    WHERE o.season = :season
      AND o.team   = :team_abbr
      AND o.week  <= :week
    GROUP BY o.team, o.week

    UNION ALL

    SELECT
        'TOTAL' AS week,
        COALESCE(SUM(o.passing_yards), 0),
        COALESCE(SUM(o.rushing_yards), 0),
        COALESCE(SUM(o.receiving_yards), 0)
    FROM public.off_team_stats_week_tbl o
    WHERE o.season = :season
      AND o.team   = :team_abbr
      AND o.week  <= :week
    GROUP BY o.team)
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"season": season, "week": week, "team_abbr": "" + team_abbr.upper() + ""}
        )
        row = result.mappings().all()
        if not row:
            return {
                "team": team_abbr.upper(),
                "passing_yards": 0,
                "rushing_yards": 0,
                "receiving_yards": 0,
            }
        return [dict(r) for r in row]


@router.get("/{team_abbr}/defense/{season}/{week}")
async def get_team_defense(team_abbr: str, season: int, week: int):
    query = """
    (SELECT
        CAST(d.week AS TEXT) AS week,
        COALESCE(SUM(d.def_tackles), 0)       AS tackles,
        COALESCE(SUM(d.def_sacks), 0)         AS sacks,
        COALESCE(SUM(d.def_interceptions), 0) AS interceptions
    FROM public.def_team_stats_week_tbl d
    WHERE d.season = :season
      AND d.team   = :team_abbr
      AND d.week  <= :week
    GROUP BY d.team, d.week

    UNION ALL

    SELECT
        'TOTAL' AS week,
        COALESCE(SUM(d.def_tackles), 0)       AS tackles,
        COALESCE(SUM(d.def_sacks), 0)         AS sacks,
        COALESCE(SUM(d.def_interceptions), 0) AS interceptions
    FROM public.def_team_stats_week_tbl d
    WHERE d.season = :season
      AND d.team   = :team_abbr
      AND d.week  <= :week
    GROUP BY d.team)
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"season": season, "week": week, "team_abbr": "" + team_abbr.upper() + ""}
        )
        row = result.mappings().all()
        if not row:
            return {
                "team": team_abbr.upper(),
                "tackles": 0,
                "sacks": 0,
                "interceptions": 0,
            }
        return [dict(r) for r in row]


@router.get("/{team_abbr}/special/{season}/{week}")
async def get_team_special(team_abbr: str, season: int, week: int):
    query = """
    (SELECT
        CAST(s.week AS TEXT) AS week,
        COALESCE(SUM(s.fg_made), 0) AS total_fg_made,
        COALESCE(SUM(s.fg_att), 0)  AS total_fg_attempted
    FROM public.st_player_stats_weekly_tbl s
    WHERE s.season = :season
      AND s.team   = :team_abbr
      AND s.week  <= :week
    GROUP BY s.team, s.week

    UNION ALL

    SELECT
        'TOTAL' AS week,
        COALESCE(SUM(s.fg_made), 0),
        COALESCE(SUM(s.fg_att), 0)
    FROM public.st_player_stats_weekly_tbl s
    WHERE s.season = :season
      AND s.team   = :team_abbr
      AND s.week  <= :week
    GROUP BY s.team)
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(query), {"season": season, "week": week, "team_abbr": "" + team_abbr.upper() + ""}
        )
        row = result.mappings().all()
        if not row:
            return {
                "team": team_abbr.upper(),
                "total_fg_made": 0,
                "total_fg_attempted": 0,
            }
        return [dict(r) for r in row]
