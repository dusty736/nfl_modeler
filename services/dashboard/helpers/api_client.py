"""
API Client Helpers
------------------
Thin convenience wrappers around the FastAPI service with resilient base URL
resolution (Cloud Run or local).

Principles
- No retries/backoff here (the UI stays snappy and predictable).
- Fail closed with empty shapes or sensible defaults ([], {}, (None, None)).
- Keep local-dev fallbacks working: try env → docker hostnames → localhost.
- Handle both bare paths and '/api/*' prefixed paths automatically.

Notes
- Historically `API_BASE` was defined twice (env then hard-coded). We now
  centralize base resolution in `_get_json_resilient`, which preserves the
  same fallbacks while letting Cloud Run override via env.
"""

import os
import requests
from urllib.parse import quote
from typing import Iterable, List, Union, Optional, Dict, Any

# --- Base URL resolution (Cloud Run friendly) -----------------------------------
API_BASE_URL = (
    os.getenv("API_BASE_URL")
    or os.getenv("API_URL")
    or os.getenv("API_BASE")
    or ""                     # will force fallback to localhost below
).rstrip("/")

_BASES = [b for b in [API_BASE_URL, "http://localhost:8000"] if b]

def _api_get(path: str, *, timeout: int = 3):
    """Try env-provided base(s) with and without /api prefix. Return parsed JSON or {} / [] on error."""
    paths = [path if path.startswith("/") else f"/{path}", f"/api{path if path.startswith('/') else '/'+path}"]
    last_err = None
    for base in _BASES:
        for p in paths:
            url = f"{base}{p}"
            try:
                r = requests.get(url, timeout=timeout)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                continue
    print(f"[api_client] GET {path} failed: {last_err}")
    # pick a stable empty shape
    return {} if path.endswith("/stats") else []


# ============================
# Base URL resolution + HTTP
# ============================

def _base_candidates() -> List[str]:
    """Prefer Cloud Run env, then docker-compose hosts, then localhost."""
    env = os.getenv("API_BASE_URL") or os.getenv("API_BASE") or os.getenv("API_URL")
    bases: List[str] = []
    if env:
        bases.append(env.rstrip("/"))
    # Local/dev fallbacks
    bases += [
        "http://api:8000",
        "http://nfl_api_py:8000",
        "http://localhost:8000",
    ]
    # De-dupe while preserving order
    seen = set()
    out: List[str] = []
    for b in bases:
        if b not in seen:
            out.append(b)
            seen.add(b)
    return out

def _normalize_path(path: str) -> str:
    return path if path.startswith("/") else f"/{path}"

def _get_json_resilient(path: str, *, params: Optional[Dict[str, Any]] = None, timeout: int = 8):
    """
    Try multiple base URLs and with/without '/api' prefix.
    Returns parsed JSON on success; empty structure on failure.
    """
    path = _normalize_path(path)
    params = params or {}
    prefixes = ("", "/api")

    last_err = None
    for base in _base_candidates():
        for pref in prefixes:
            url = f"{base}{pref}{path}"
            try:
                r = requests.get(url, params=params, timeout=timeout)
                if r.status_code == 404:
                    # Try next prefix/base if this path style isn't mounted
                    last_err = f"404 at {url}"
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                continue

    # Heuristic: dict endpoints vs list endpoints for safe fallbacks
    fallback = {} if path.endswith("/stats") or path.count("/games/") >= 1 else []
    print(f"[api_client] GET {path} failed across fallbacks: {last_err}")
    return fallback

# ============================
# Core schedule/state lookups
# ============================

def fetch_current_season_week():
    try:
        data = _api_get("/season-week", timeout=2)
        return (data or {}).get("season"), (data or {}).get("week")
    except Exception:
        return None, None

def fetch_primetime_games():
    try:
        data = _api_get("/primetime-games", timeout=2)
        return (data or {}).get("games", []) if isinstance(data, dict) else (data or [])
    except Exception as e:
        print(f"[api_client] Failed to fetch primetime games: {e}")
        return []

# ============================
# Teams directory
# ============================

def get_all_teams():
    try:
        data = _api_get("/teams/", timeout=2)
        return data or []
    except Exception as e:
        print(f"[api_client] Failed to fetch teams: {e}")
        return []

def get_team_by_abbr(team_abbr: str):
    try:
        data = _api_get(f"/teams/{team_abbr}", timeout=2)
        return data or []
    except Exception as e:
        print(f"[api_client] Failed to fetch team abbr: {e}")
        return []

# ============================
# Team stats (record/off/def/special)
# ============================

def get_team_record(team_abbr: str, season: int, week: int):
    try:
        return _get_json_resilient(f"/team_stats/{team_abbr}/record/{int(season)}/{int(week)}", timeout=3) or {}
    except Exception as e:
        print(f"[api_client] Failed to fetch team record: {e}")
        return {}

def get_team_offense(team_abbr: str, season: int, week: int):
    try:
        data = _get_json_resilient(f"/team_stats/{team_abbr}/offense/{int(season)}/{int(week)}", timeout=4)
        return data if isinstance(data, (list, dict)) else {}
    except Exception as e:
        print(f"[api_client] Failed to fetch team offense: {e}")
        return {}

def get_team_defense(team_abbr: str, season: int, week: int):
    try:
        data = _get_json_resilient(f"/team_stats/{team_abbr}/defense/{int(season)}/{int(week)}", timeout=4)
        return data if isinstance(data, (list, dict)) else {}
    except Exception as e:
        print(f"[api_client] Failed to fetch team defense: {e}")
        return {}

def get_team_special(team_abbr: str, season: int, week: int):
    try:
        data = _get_json_resilient(f"/team_stats/{team_abbr}/special/{int(season)}/{int(week)}", timeout=4)
        return data if isinstance(data, (list, dict)) else {}
    except Exception as e:
        print(f"[api_client] Failed to fetch team special: {e}")
        return {}

# ============================
# Rosters
# ============================

def get_team_roster(team_abbr: str, season: int):
    try:
        data = _get_json_resilient(f"/team_rosters/{team_abbr}/{int(season)}", timeout=4)
        return data if isinstance(data, (list, dict)) else {}
    except Exception as e:
        print(f"[api_client] Failed to fetch team roster: {e}")
        return {}

def get_team_position_summary(team_abbr: str, season: int, position: str):
    try:
        data = _get_json_resilient(f"/team_rosters/{team_abbr}/{int(season)}/positions/{position}", timeout=4)
        return data if isinstance(data, (list, dict)) else {}
    except Exception as e:
        print(f"[api_client] Failed to fetch position summary: {e}")
        return {}

def get_team_depth_chart_starters(team_abbr: str, season: int, week: int):
    try:
        data = _get_json_resilient(f"/team_rosters/{team_abbr}/{int(season)}/weeks/{int(week)}", timeout=4)
        return data if isinstance(data, (list, dict)) else {}
    except Exception as e:
        print(f"[api_client] Failed to fetch depth chart starters: {e}")
        return {}

# ============================
# Week bounds
# ============================

def fetch_max_week(season: int) -> int:
    data = _get_json_resilient(f"/max-week/{int(season)}", timeout=3) or {}
    if isinstance(data, dict):
        return int(data.get("max_week", 18))
    return 18

def get_max_week_team(season: int, team: str) -> int:
    try:
        data = _get_json_resilient(f"/max-week-team/{int(season)}/{team}", timeout=3) or {}
        return int(data.get("max_week", 18)) if isinstance(data, dict) else 18
    except Exception as e:
        print(f"[api_client] Failed to fetch max week for {team} {season}: {e}")
        return 18

# ============================
# Injuries
# ============================

def get_team_injury_summary(team_abbr: str, season: int, week: int, position: str):
    try:
        data = _get_json_resilient(
            f"/team_injuries/{team_abbr}/injuries/team/{int(season)}/{int(week)}/{position}",
            timeout=5
        )
        return data if isinstance(data, (list, dict)) else {}
    except Exception as e:
        print(f"[api_client] Failed to fetch team injury summary: {e}")
        return {}

def get_player_injuries(team_abbr: str, season: int, week: int, position: str):
    try:
        data = _get_json_resilient(
            f"/team_injuries/{team_abbr}/injuries/player/{int(season)}/{int(week)}/{position}",
            timeout=5
        )
        return data if isinstance(data, (list, dict)) else {}
    except Exception as e:
        print(f"[api_client] Failed to fetch player injuries: {e}")
        return {}

# ============================
# Analytics Nexus — shared bits
# ============================

ALLOWED_POSITIONS = {"QB", "RB", "WR", "TE"}
ALLOWED_SEASON_TYPES = {"REG", "POST", "ALL"}
ALLOWED_ORDER_BY = {"rCV", "IQR", "median"}
ALLOWED_TOP_BY = {"combined", "x_gate", "y_gate", "x_value", "y_value"}

# Keep legacy API_PREFIXES for code that builds its own paths
API_PREFIXES = ["", "/api"]

# ============================
# Analytics Nexus (Players — Trajectories)
# ============================

def fetch_player_trajectories(
    season: int,
    season_type: str,
    stat_name: str,
    position: str,
    top_n: int,
    week_start: int = 1,
    week_end: int = 18,
    rank_by: str = "sum",          # 'sum' or 'mean'
    stat_type: str = "base",       # 'base' or 'cumulative'
    min_games: int = 0,
    timeout: int = 4,
    debug: bool = True,
):
    try:
        pos = (position or "").upper().strip()
        if pos not in ALLOWED_POSITIONS:
            raise ValueError(f"position must be one of {sorted(ALLOWED_POSITIONS)}")

        st = (season_type or "").upper().strip()
        if st not in ALLOWED_SEASON_TYPES:
            raise ValueError(f"season_type must be one of {sorted(ALLOWED_SEASON_TYPES)}")

        rb = (rank_by or "sum").lower().strip()
        rb = "mean" if rb in {"mean", "avg", "average"} else "sum"

        stype = (stat_type or "base").lower().strip()
        if stype not in {"base", "cumulative"}:
            raise ValueError("stat_type must be 'base' or 'cumulative'")

        mg = max(0, int(min_games))
        stat_seg = quote(str(stat_name), safe="")
        params = {
            "week_start": int(week_start),
            "week_end": int(week_end),
            "stat_type": stype,
            "rank_by": rb,
            "min_games": mg,
        }
        path = f"/analytics_nexus/player/trajectories/{int(season)}/{st}/{stat_seg}/{pos}/{int(top_n)}"
        data = _get_json_resilient(path, params=params, timeout=timeout)

        if isinstance(data, list):
            if debug:
                print(f"[api_client] OK {path} -> {len(data)} rows")
            return data
        if isinstance(data, dict) and data.get("error"):
            if debug:
                print(f"[api_client] Empty (error): {data.get('error')}")
        return []
    except Exception as e:
        print(f"[api_client] Failed to fetch player trajectories: {e}")
        return []

# ============================
# Analytics Nexus (Players — Violins)
# ============================

def fetch_player_violins(
    seasons,
    season_type: str,
    stat_name: str,
    position: str,
    top_n: int,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",
    order_by: str = "rCV",
    min_games_for_badges: int = 6,
    timeout: int = 5,
    debug: bool = True,
):
    def _empty_payload(_seasons_list):
        return {
            "weekly": [],
            "summary": [],
            "badges": {"most_consistent": "—", "most_volatile": "—"},
            "meta": {
                "position": (position or "").upper().strip(),
                "stat_name": stat_name,
                "stat_type": (stat_type or "base").lower().strip(),
                "season_type": (season_type or "REG").upper().strip(),
                "seasons": _seasons_list,
                "week_start": int(week_start),
                "week_end": int(week_end),
                "order_by": order_by,
                "top_n": int(top_n),
                "min_games_for_badges": int(min_games_for_badges),
            },
        }

    try:
        # seasons -> sorted unique list[int]
        if seasons is None:
            seasons_list = []
        elif isinstance(seasons, (list, tuple, set)):
            seasons_list = [int(s) for s in seasons]
        elif isinstance(seasons, int):
            seasons_list = [int(seasons)]
        else:
            seasons_list = [int(s.strip()) for s in str(seasons).split(",") if s.strip()]
        seasons_list = sorted(set(seasons_list))
        if not seasons_list:
            return _empty_payload([])

        pos = (position or "").upper().strip()
        if pos not in ALLOWED_POSITIONS:
            raise ValueError(f"position must be one of {sorted(ALLOWED_POSITIONS)}")

        st = (season_type or "").upper().strip()
        if st not in ALLOWED_SEASON_TYPES:
            raise ValueError(f"season_type must be one of {sorted(ALLOWED_SEASON_TYPES)}")

        stype = (stat_type or "base").lower().strip()
        if stype not in {"base", "cumulative"}:
            raise ValueError("stat_type must be 'base' or 'cumulative'")

        ob = (order_by or "rCV").strip()
        ob_final = "rCV" if ob.lower() == "rcv" else "IQR" if ob.lower() == "iqr" else "median" if ob.lower() == "median" else None
        if ob_final is None:
            raise ValueError(f"order_by must be one of {sorted(ALLOWED_ORDER_BY)}")

        ws = max(1, int(week_start))
        we = min(22, int(week_end))
        if we < ws:
            return _empty_payload(seasons_list)

        mg = max(0, int(min_games_for_badges))
        tn = max(1, int(top_n))
        stat_seg = quote(str(stat_name), safe="")
        params = {
            "seasons": seasons_list,
            "season_type": st,
            "stat_type": stype,
            "week_start": ws,
            "week_end": we,
            "order_by": ob_final,
            "min_games_for_badges": mg,
        }
        path = f"/analytics_nexus/player/violins/{stat_seg}/{pos}/{tn}"
        data = _get_json_resilient(path, params=params, timeout=timeout)

        if isinstance(data, dict) and "weekly" in data and "summary" in data:
            if debug:
                print(f"[api_client] OK {path} -> weekly={len(data.get('weekly', []))}, summary={len(data.get('summary', []))}")
            return data
        if debug:
            print(f"[api_client] Unexpected payload at {path}")
        return _empty_payload(seasons_list)
    except Exception as e:
        print(f"[api_client] Failed to fetch player violins: {e}")
        if isinstance(seasons, (int, str)):
            try:
                seasons = [int(s) for s in str(seasons).split(",") if s.strip()]
            except Exception:
                seasons = []
        return _empty_payload(sorted(set(seasons)) if seasons else [])

# ============================
# Analytics Nexus (Players — Scatter)
# ============================

def fetch_player_scatter(
    seasons,
    season_type: str,
    position: str,
    metric_x: str,
    metric_y: str,
    top_n: int = 20,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",
    top_by: str = "combined",
    log_x: bool = False,
    log_y: bool = False,
    label_all_points: bool = True,
    timeout: int = 5,
    debug: bool = True,
):
    try:
        pos = (position or "").upper().strip()
        if pos not in ALLOWED_POSITIONS:
            raise ValueError(f"position must be one of {sorted(ALLOWED_POSITIONS)}")
        st = (season_type or "").upper().strip()
        if st not in ALLOWED_SEASON_TYPES:
            raise ValueError(f"season_type must be one of {sorted(ALLOWED_SEASON_TYPES)}")

        tb = (top_by or "combined").strip().lower()
        if tb not in ALLOWED_TOP_BY:
            raise ValueError(f"top_by must be one of {sorted(ALLOWED_TOP_BY)}")

        stype = (stat_type or "base").strip().lower()
        if stype != "base":  # enforce 'base'
            stype = "base"

        # seasons normalize -> sorted unique list[int]
        if seasons is None:
            raise ValueError("seasons is required")
        if isinstance(seasons, (int, str)):
            seasons = [seasons]
        clean_seasons: List[int] = []
        for s in seasons:
            try:
                clean_seasons.append(int(s))
            except Exception:
                for tok in str(s).split(","):
                    tok = tok.strip()
                    if tok:
                        clean_seasons.append(int(tok))
        seasons = sorted(set(clean_seasons))
        if not seasons:
            raise ValueError("At least one season must be provided")

        mx = quote(str(metric_x), safe="")
        my = quote(str(metric_y), safe="")
        params = {
            "season_type": st,
            "week_start": int(week_start),
            "week_end": int(week_end),
            "stat_type": stype,
            "top_by": tb,
            "log_x": bool(log_x),
            "log_y": bool(log_y),
            "label_all_points": bool(label_all_points),
            "seasons": seasons,  # requests encodes as repeatable
        }
        path = f"/analytics_nexus/player/scatter/{mx}/{my}/{pos}/{int(top_n)}"
        data = _get_json_resilient(path, params=params, timeout=timeout)
        if isinstance(data, dict):
            if debug:
                pts = len(data.get("points", []) or [])
                print(f"[api_client] OK scatter -> {pts} points")
            return data
        return {}
    except Exception as e:
        print(f"[api_client] Failed to fetch player scatter: {e}")
        return {}

# ============================
# Analytics Nexus (Players — Rolling Percentiles)
# ============================

def fetch_player_rolling_percentiles(
    seasons,
    season_type: str,
    position: str,
    metric: str,
    top_n: int,
    week_start: int,
    week_end: int,
    stat_type: str = "base",
    rolling_window: int = 4,
    timeout: int = 8,
    debug: bool = False,
):
    """
    /analytics_nexus/player/rolling_percentiles/{metric}/{position}/{top_n}
    """
    try:
        pos   = (position or "").upper().strip()
        stype = (stat_type or "base").lower().strip()
        st    = (season_type or "REG").upper().strip()

        if seasons is None:
            raise ValueError("seasons is required")
        if isinstance(seasons, (int, str)):
            seasons = [seasons]
        clean: List[int] = []
        for s in seasons:
            try:
                clean.append(int(s))
            except Exception:
                for tok in str(s).split(","):
                    tok = tok.strip()
                    if tok:
                        clean.append(int(tok))
        seasons = sorted(set(clean))
        if not seasons:
            raise ValueError("At least one season must be provided")

        path = f"/analytics_nexus/player/rolling_percentiles/{metric}/{pos}/{int(top_n)}"
        params = {
            "seasons": seasons,
            "season_type": st,
            "stat_type": stype,
            "week_start": int(week_start),
            "week_end": int(week_end),
            "rolling_window": int(rolling_window),
            "debug": str(bool(debug)).lower(),
        }
        data = _get_json_resilient(path, params=params, timeout=timeout)
        if isinstance(data, dict):
            data.setdefault("series", [])
            data.setdefault("players", [])
            data.setdefault("meta", {})
            if debug:
                print(f"[ROLLING DEBUG] OK -> {len(data.get('series', []))} series rows")
            return data
        return {"series": [], "players": [], "meta": {}}
    except Exception as e:
        print(f"[fetch_player_rolling_percentiles] error: {e}")
        return {"series": [], "players": [], "meta": {}}

# ============================
# Analytics Nexus (Teams — Trajectories)
# ============================

def fetch_team_trajectories(
    stat_name: str,
    top_n: int,
    seasons: List[int],
    season_type: str = "REG",
    week_start: int = 1,
    week_end: int = 18,
    rank_by: str = "sum",
    stat_type: str = "base",
    highlight: Optional[Union[str, List[str]]] = None,
    timeout: int = 4,
    debug: bool = True,
):
    try:
        st = (season_type or "REG").upper().strip()
        rb = (rank_by or "sum").lower().strip()
        rb = "mean" if rb in {"mean","avg","average"} else "sum"
        stype = (stat_type or "base").lower().strip()
        if st not in {"REG","POST","ALL"}:
            raise ValueError("season_type must be REG, POST, or ALL")
        if stype not in {"base","cumulative"}:
            raise ValueError("stat_type must be base or cumulative")

        stat_seg = quote(str(stat_name), safe="")
        params = {
            "seasons": [int(s) for s in seasons],
            "season_type": st,
            "week_start": int(week_start),
            "week_end": int(week_end),
            "rank_by": rb,
            "stat_type": stype,
        }
        path = f"/analytics_nexus/team/trajectories/{stat_seg}/{int(top_n)}"
        data = _get_json_resilient(path, params=params, timeout=timeout)
        if isinstance(data, list):
            if debug:
                print(f"[api_client] OK {path} -> {len(data)} rows")
            return data
        if isinstance(data, dict) and data.get("error"):
            if debug:
                print(f"[api_client] Empty (error): {data.get('error')}")
        return []
    except Exception as e:
        print(f"[api_client] Failed to fetch team trajectories: {e}")
        return []

# ============================
# Analytics Nexus (Teams — Violins)
# ============================

def fetch_team_violins(
    seasons,
    season_type: str,
    stat_name: str,
    top_n: int,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",
    order_by: str = "rCV",
    min_games_for_badges: int = 6,
    timeout: int = 5,
    debug: bool = True,
):
    def _empty_payload(_seasons):
        return {
            "weekly": [],
            "summary": [],
            "badges": {"most_consistent": "—", "most_volatile": "—"},
            "meta": {
                "stat_name": stat_name,
                "stat_type": (stat_type or "base"),
                "season_type": (season_type or "REG"),
                "seasons": _seasons or [],
                "week_start": int(week_start) if week_start is not None else 1,
                "week_end": int(week_end) if week_end is not None else 18,
                "order_by": order_by or "rCV",
                "top_n": int(top_n) if top_n is not None else 10,
                "min_games_for_badges": int(min_games_for_badges) if min_games_for_badges is not None else 6,
            },
        }

    try:
        if seasons is None:
            seasons_list = []
        elif isinstance(seasons, (list, tuple, set)):
            seasons_list = [int(s) for s in seasons if s is not None]
        elif isinstance(seasons, int):
            seasons_list = [seasons]
        else:
            seasons_list = [int(s.strip()) for s in str(seasons).split(",") if s.strip()]
        seasons_list = sorted(set(seasons_list))
        if not seasons_list:
            return _empty_payload([])

        st = (season_type or "REG").upper().strip()
        if st not in ALLOWED_SEASON_TYPES:
            raise ValueError(f"season_type must be one of {sorted(ALLOWED_SEASON_TYPES)}")

        stype = (stat_type or "base").lower().strip()
        if stype not in {"base", "cumulative"}:
            raise ValueError("stat_type must be 'base' or 'cumulative'")

        ob_norm = (order_by or "rCV").strip().lower()
        if ob_norm == "rcv":
            ob_final = "rCV"
        elif ob_norm == "iqr":
            ob_final = "IQR"
        elif ob_norm == "median":
            ob_final = "median"
        else:
            raise ValueError(f"order_by must be one of {sorted(ALLOWED_ORDER_BY)}")

        ws = max(1, int(week_start))
        we = min(22, int(week_end))
        if we < ws:
            return _empty_payload(seasons_list)

        tn = max(1, int(top_n))
        mg = max(0, int(min_games_for_badges))

        stat_seg = quote(str(stat_name), safe="")
        params = {
            "seasons": seasons_list,
            "season_type": st,
            "week_start": ws,
            "week_end": we,
            "stat_type": stype,
            "order_by": ob_final,
            "min_games_for_badges": mg,
        }
        path = f"/analytics_nexus/team/violins/{stat_seg}/{tn}"
        data = _get_json_resilient(path, params=params, timeout=timeout)
        if isinstance(data, dict) and "weekly" in data and "summary" in data:
            if debug:
                print(f"[api_client] OK team violins -> weekly={len(data.get('weekly', []))}, summary={len(data.get('summary', []))}")
            return data
        if debug:
            print(f"[api_client] Unexpected payload (team violins)")
        return _empty_payload(seasons_list)
    except Exception as e:
        print(f"[api_client] Failed to fetch team violins: {e}")
        return _empty_payload(seasons_list if 'seasons_list' in locals() else [])

# ============================
# Analytics Nexus (Teams — Scatter)
# ============================

def fetch_team_scatter(
    seasons,
    season_type: str,
    metric_x: str,
    metric_y: str,
    top_n: int = 20,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",
    top_by: str = "combined",
    log_x: bool = False,
    log_y: bool = False,
    label_all_points: bool = True,
    timeout: int = 5,
    debug: bool = True,
):
    try:
        st = (season_type or "").upper().strip()
        if st not in ALLOWED_SEASON_TYPES:
            raise ValueError(f"season_type must be one of {sorted(ALLOWED_SEASON_TYPES)}")

        stype = (stat_type or "base").lower().strip()
        if stype != "base":
            stype = "base"

        tb = (top_by or "combined").strip().lower()
        if tb not in ALLOWED_TOP_BY:
            raise ValueError(f"top_by must be one of {sorted(ALLOWED_TOP_BY)}")

        if seasons is None:
            raise ValueError("seasons is required")
        if isinstance(seasons, (int, str)):
            seasons = [seasons]
        clean: List[int] = []
        for s in seasons:
            try:
                clean.append(int(s))
            except Exception:
                for tok in str(s).split(","):
                    tok = tok.strip()
                    if tok:
                        clean.append(int(tok))
        seasons = sorted(set(clean))
        if not seasons:
            raise ValueError("At least one season must be provided")

        mx = quote(str(metric_x), safe="")
        my = quote(str(metric_y), safe="")
        params = {
            "season_type": st,
            "week_start": int(week_start),
            "week_end": int(week_end),
            "stat_type": stype,
            "top_by": tb,
            "log_x": bool(log_x),
            "log_y": bool(log_y),
            "label_all_points": bool(label_all_points),
            "seasons": seasons,
        }
        path = f"/analytics_nexus/team/scatter/{mx}/{my}/{int(top_n)}"
        data = _get_json_resilient(path, params=params, timeout=timeout)
        if isinstance(data, dict) and "points" in data and "meta" in data:
            if debug:
                print(f"[api_client] OK team scatter -> {len(data.get('points', []) or [])} points")
            return data
        if debug:
            print(f"[api_client] Unexpected payload (team scatter)")
        return {}
    except Exception as e:
        print(f"[api_client] Failed to fetch team scatter: {e}")
        return {}

# ============================
# Team Rolling Percentiles
# ============================

def fetch_team_rolling_percentiles(
    seasons,
    season_type="REG",
    metric="rushing_epa",
    top_n=16,
    week_start=1,
    week_end=18,
    stat_type="base",
    rolling_window=4,
    timeout=8,
    debug=False,
):
    """
    /analytics_nexus/team/rolling_percentiles/{metric}/{top_n}
    """
    params = {
        "seasons": seasons or [],
        "season_type": season_type,
        "stat_type": stat_type,
        "week_start": int(week_start),
        "week_end": int(week_end),
        "rolling_window": int(rolling_window),
    }
    if debug:
        params["debug"] = True

    path = f"/analytics_nexus/team/rolling_percentiles/{metric}/{int(top_n)}"
    data = _get_json_resilient(path, params=params, timeout=timeout)
    if isinstance(data, dict):
        data.setdefault("series", [])
        data.setdefault("teams", [])
        data.setdefault("meta", {})
        return data
    return {"series": [], "teams": [], "meta": {}}

# ============================
# Games — week list + details
# ============================

def get_games_week(season: int, week: int, *, timeout: int = 20):
    """
    GET /games/{season}/{week}
    Returns list[dict] or [] on error.
    """
    try:
        s = int(season)
        w = int(week)
    except Exception as e:
        raise ValueError(f"Invalid season/week: {season}/{week}") from e

    path = f"/games/{s}/{w}"
    try:
        data = _get_json_resilient(path, timeout=timeout)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[api_client.get_games_week] GET {path} failed: {e}")
        return []

def get_game_detail(season: int, week: int, game_id: str):
    """GET /games/{season}/{week}/{game_id}."""
    path = f"/games/{int(season)}/{int(week)}/{game_id}"
    data = _get_json_resilient(path, timeout=8)
    return data if isinstance(data, dict) else {}

def get_game_stats(season: int, week: int, game_id: str):
    """GET /games/{season}/{week}/{game_id}/stats."""
    path = f"/games/{int(season)}/{int(week)}/{game_id}/stats"
    data = _get_json_resilient(path, timeout=10)
    return data if isinstance(data, dict) else {}

