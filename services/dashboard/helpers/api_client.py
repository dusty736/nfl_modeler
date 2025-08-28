"""
API Client Helpers
------------------
Thin convenience wrappers around the internal FastAPI service.

Principles
- No retries/backoff here (the UI stays snappy and predictable).
- Fail closed with empty shapes or sensible defaults ([], {}, (None, None)).
- Do NOT change behaviour: this file only adds documentation/comments.

Caution
- `API_BASE` is defined twice on purpose in the current code: first via env
  (default "http://api:8000"), later hard-set to "http://nfl_api_py:8000".
  The latter *wins* at runtime. We are not changing that precedence here.
"""

import os
import requests
from urllib.parse import quote
from typing import Iterable, List, Union, Optional, Dict, Any

# --- Core schedule/state lookups ------------------------------------------------
def fetch_current_season_week():
    """Return the current (season, week) from /api/season-week.
    
    Returns:
        tuple[int|None, int|None]: (season, week) or (None, None) on error.
    """
    try:
        r = requests.get("http://api:8000/api/season-week", timeout=2)
        r.raise_for_status()
        data = r.json()
        return data.get("season"), data.get("week")
    except Exception:
        return None, None

def fetch_primetime_games():
    """Fetch primetime games for the current season/week.

    Primetime: any non-Sunday, or Sunday 18:00+ (Europe/London), per backend logic.

    Returns:
        list[dict]: Zero or more game objects. Empty list on error.
    """
    try:
        response = requests.get("http://api:8000/api/primetime-games", timeout=2)
        response.raise_for_status()
        data = response.json()
        return data.get("games", [])
    except Exception as e:
        print(f"[api_client] Failed to fetch primetime games: {e}")
        return []

# --- Teams directory ------------------------------------------------------------
def get_all_teams():
    """List teams (excluding legacy OAK/STL/SD) from /teams/.
    
    Returns:
        list[dict]: [{team_name, team_abbr, team_division}, ...] or [] on error.
    """

    try:
        r = requests.get("http://api:8000/teams/", timeout=2)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch teams: {e}")
        return []

def get_team_by_abbr(team_abbr: str):
    """Fetch a single team by abbreviation from /teams/{abbr}.
    
    Args:
        team_abbr (str): e.g., "KC".
    
    Returns:
        dict|list: Team mapping on success; [] on error for UI stability.
    """

    try:
        r = requests.get(f"http://api:8000/teams/{team_abbr}", timeout=2)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch team abbr: {e}")
        return []
      
# --- Team stats (record/offense/defense/special) --------------------------------
def get_team_record(team_abbr: str, season: int, week: int):
    """Get season record snapshot for a team.
    
    Args:
        team_abbr (str), season (int), week (int): week is forwarded unchanged.
    
    Returns:
        dict: {wins, losses, ties, points_scored, points_allowed, point_differential}
              or {} on error.
    """

    try:
        r = requests.get(
            f"http://api:8000/team_stats/{team_abbr}/record/{season}/{week}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch team record: {e}")
        return {}

def get_team_offense(team_abbr: str, season: int, week: int):
    """Cumulative offensive yards by week (plus TOTAL row).
    
    Returns:
        list[dict]|dict: [{'week','passing_yards','rushing_yards','receiving_yards'}, ...]
                         or {} on error.
    """

    try:
        r = requests.get(
            f"http://api:8000/team_stats/{team_abbr}/offense/{season}/{week}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch team offense: {e}")
        return {}

def get_team_defense(team_abbr: str, season: int, week: int):
    """Cumulative defensive counting stats by week (plus TOTAL row).
    
    Returns:
        list[dict]|dict: [{'week','tackles','sacks','interceptions'}, ...] or {} on error.
    """

    try:
        r = requests.get(
            f"http://api:8000/team_stats/{team_abbr}/defense/{season}/{week}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch team defense: {e}")
        return {}

def get_team_special(team_abbr: str, season: int, week: int):
    """Cumulative special teams FG counts by week (plus TOTAL row).
    
    Returns:
        list[dict]|dict: [{'week','total_fg_made','total_fg_attempted'}, ...] or {} on error.
    """

    try:
        r = requests.get(
            f"http://api:8000/team_stats/{team_abbr}/special/{season}/{week}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch team special: {e}")
        return {}
      
# --- Rosters --------------------------------------------------------------------
def get_team_roster(team_abbr: str, season: int):
    """Full roster listing for a season/team.
  
    Returns:
        list[dict]|dict: rows with name, position, status, bio fields; {} on error.
    """

    try:
        r = requests.get(
            f"http://api:8000/team_rosters/{team_abbr}/{season}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch team roster: {e}")
        return {}

# --- Roster: position summary ---
def get_team_position_summary(team_abbr: str, season: int, position: str):
    """Position-group summary (averages + stability score) or TEAM aggregate.
  
    Returns:
        list[dict]|dict: one row for the requested position; {} on error.
    """

    try:
        r = requests.get(
            f"http://api:8000/team_rosters/{team_abbr}/{season}/positions/{position}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch position summary: {e}")
        return {}

# --- Roster: weekly depth chart starters ---
def get_team_depth_chart_starters(team_abbr: str, season: int, week: int):
    """Depth chart starters for a given week.
    
    Returns:
        list[dict]|dict: [{position_group, player, starts, new_starter, ...}] or {} on error.
    """

    try:
        r = requests.get(
            f"http://api:8000/team_rosters/{team_abbr}/{season}/weeks/{week}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch depth chart starters: {e}")
        return {}

# --- Week bounds ----------------------------------------------------------------
def fetch_max_week(season: int) -> int:
    """Return max completed week for a season via /api/max-week/{season}.
    
    Returns:
        int: Max week (default 18 on error).
    """

    r = requests.get(f"http://api:8000/api/max-week/{season}", timeout=2)
    if r.ok:
        return r.json().get("max_week", 18)
    return 18
  
def get_max_week_team(season: int, team: str) -> int:
    """Return max completed week for a season/team via /api/max-week-team.
    
    Returns:
        int: Max week (default 18 on error).
    """
    try:
        r = requests.get(
            f"http://api:8000/api/max-week-team/{season}/{team}",
            timeout=2
        )
        r.raise_for_status()
        return r.json().get("max_week", 18)
    except Exception as e:
        print(f"[api_client] Failed to fetch max week for {team} {season}: {e}")
        return 18

# --- Injuries -------------------------------------------------------------------
def get_team_injury_summary(team_abbr: str, season: int, week: int, position: str):
    """Team injury counts (weekly and season-to-date) for a position group.
    
    Returns:
        list[dict]|dict: [{'week','position','weekly_injuries','season_injuries'}] or {} on error.
    """
    try:
        r = requests.get(
            f"http://api:8000/team_injuries/{team_abbr}/injuries/team/{season}/{week}/{position}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch team injury summary: {e}")
        return {}

# --- Injuries: player-level raw data ---
def get_player_injuries(team_abbr: str, season: int, week: int, position: str):
    """Per-player injury details for a position group.
    
    Returns:
        list[dict]|dict: [{'name','position','report_status','practice_status',...}] or {} on error.
    """
    try:
        r = requests.get(
            f"http://api:8000/team_injuries/{team_abbr}/injuries/player/{season}/{week}/{position}",
            timeout=2
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch player injuries: {e}")
        return {}

# --- Analytics Nexus (Players) --------------------------------------------------
API_BASE = os.getenv("API_BASE", "http://api:8000")
API_PREFIXES = ["", "/api"]  # try bare path first, then /api

ALLOWED_POSITIONS = {"QB", "RB", "WR", "TE"}
ALLOWED_SEASON_TYPES = {"REG", "POST", "ALL"}

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
    min_games: int = 0,            # floor on non-NULL weeks
    timeout: int = 3,
    debug: bool = True,
):
    """Analytics Nexus: Top-N player weekly trajectories.
    
    Args:
        season (int), season_type (str), stat_name (str), position (str), top_n (int)
        week_start/week_end (int): inclusive bounds
        rank_by (str): "sum"|"mean" (avg)
        stat_type (str): "base"|"cumulative"
        min_games (int): minimum non-NULL weeks
        timeout (int), debug (bool)
    
    Returns:
        list[dict]: rows ordered by player_rank then week; [] on error/empty.
    """
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

        # Safe for path segment
        stat_seg = quote(str(stat_name), safe="")

        params = {
            "week_start": int(week_start),
            "week_end": int(week_end),
            "stat_type": stype,
            "rank_by": rb,
            "min_games": mg,
        }

        last_err = None
        for prefix in API_PREFIXES:
            url = f"{API_BASE}{prefix}/analytics_nexus/player/trajectories/{int(season)}/{st}/{stat_seg}/{pos}/{int(top_n)}"
            try:
                if debug:
                    print(f"[api_client] GET {url} params={params}")
                r = requests.get(url, params=params, timeout=timeout)
                if r.status_code == 404:
                    last_err = f"404 at {url}"
                    continue
                r.raise_for_status()
                data = r.json()
                if isinstance(data, dict) and data.get("error"):
                    if debug:
                        print(f"[api_client] Empty (error): {data.get('error')}")
                    return []
                if isinstance(data, list):
                    if debug:
                        print(f"[api_client] OK {url} -> {len(data)} rows")
                    return data
                if debug:
                    print(f"[api_client] Unexpected payload at {url}: {type(data)}")
                return []
            except Exception as e:
                last_err = str(e)
                continue

        if debug:
            print(f"[api_client] Failed after trying {API_PREFIXES}: {last_err}")
        return []
    except Exception as e:
        print(f"[api_client] Failed to fetch player trajectories: {e}")
        return []
      
# --- Analytics Nexus (Players — Scatter) ----------------------------------------
ALLOWED_ORDER_BY = {"rCV", "IQR", "median"}

def fetch_player_violins(
    seasons,
    season_type: str,
    stat_name: str,
    position: str,
    top_n: int,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",              # 'base' or 'cumulative'
    order_by: str = "rCV",                # 'rCV' | 'IQR' | 'median'
    min_games_for_badges: int = 6,        # small-n badge threshold; does NOT filter players
    timeout: int = 4,
    debug: bool = True,
):
    """Analytics Nexus: Player consistency/volatility violins.
    
    Returns:
        dict: {"weekly": [...], "summary": [...], "badges": {...}, "meta": {...}}
              Empty-shaped payload on error.
    """

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
        # --- Normalize inputs ---
        # Seasons -> unique sorted list[int]
        if seasons is None:
            seasons_list = []
        elif isinstance(seasons, (list, tuple, set)):
            seasons_list = [int(s) for s in seasons]
        elif isinstance(seasons, (int,)):
            seasons_list = [int(seasons)]
        else:
            # assume comma-separated string
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
        ob_norm = ob.lower()
        if ob_norm == "rcv":
            ob_final = "rCV"
        elif ob_norm == "iqr":
            ob_final = "IQR"
        elif ob_norm == "median":
            ob_final = "median"
        else:
            raise ValueError(f"order_by must be one of {sorted(ALLOWED_ORDER_BY)}")

        ws = int(week_start)
        we = int(week_end)
        if ws < 1: ws = 1
        if we > 22: we = 22
        if we < ws:
            return _empty_payload(seasons_list)

        mg = max(0, int(min_games_for_badges))
        tn = max(1, int(top_n))

        # Safe path segment for stat_name
        stat_seg = quote(str(stat_name), safe="")

        # Build query params (repeatable 'seasons' is supported by `requests` when value is a list)
        params = {
            "seasons": seasons_list,
            "season_type": st,
            "stat_type": stype,
            "week_start": ws,
            "week_end": we,
            "order_by": ob_final,
            "min_games_for_badges": mg,
        }

        last_err = None
        for prefix in API_PREFIXES:
            url = f"{API_BASE}{prefix}/analytics_nexus/player/violins/{stat_seg}/{pos}/{tn}"
            try:
                if debug:
                    print(f"[api_client] GET {url} params={params}")
                r = requests.get(url, params=params, timeout=timeout)
                if r.status_code == 404:
                    last_err = f"404 at {url}"
                    continue
                r.raise_for_status()
                data = r.json()
                # Expect a dict with 'weekly'/'summary' keys
                if isinstance(data, dict) and "weekly" in data and "summary" in data:
                    if debug:
                        w = len(data.get("weekly", []))
                        s = len(data.get("summary", []))
                        print(f"[api_client] OK {url} -> weekly={w}, summary={s}")
                    return data
                if debug:
                    print(f"[api_client] Unexpected payload at {url}: type={type(data)} keys={list(data) if isinstance(data, dict) else 'n/a'}")
                return _empty_payload(seasons_list)
            except Exception as e:
                last_err = str(e)
                continue

        if debug:
            print(f"[api_client] Failed after trying {API_PREFIXES}: {last_err}")
        return _empty_payload(seasons_list)

    except Exception as e:
        print(f"[api_client] Failed to fetch player violins: {e}")
        return _empty_payload(sorted(set([int(seasons)])) if isinstance(seasons, (int, str)) else (sorted(set(seasons)) if seasons else []))

# --- Analytics Nexus (Players — Scatter) ----------------------------------------
ALLOWED_TOP_BY = {"combined", "x_gate", "y_gate", "x_value", "y_value"}

def fetch_player_scatter(
    seasons,
    season_type: str,
    position: str,
    metric_x: str,
    metric_y: str,
    top_n: int = 20,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",                # scatter uses 'base' only
    top_by: str = "combined",               # combined | x_gate | y_gate | x_value | y_value
    log_x: bool = False,
    log_y: bool = False,
    label_all_points: bool = True,          # UI may choose to show/hide labels
    timeout: int = 5,
    debug: bool = True,
):
    """Analytics Nexus: Player quadrant scatter.
    
    Returns:
        dict: {"points":[...], "meta":{...}} or {} on error.
    Notes:
        stat_type is forced to "base"; seasons can be list or comma string.
    """
    try:
        # --- Validate & normalize ---
        pos = (position or "").upper().strip()
        if pos not in ALLOWED_POSITIONS:
            raise ValueError(f"position must be one of {sorted(ALLOWED_POSITIONS)}")

        st = (season_type or "").upper().strip()
        if st not in ALLOWED_SEASON_TYPES:
            raise ValueError(f"season_type must be one of {sorted(ALLOWED_SEASON_TYPES)}")

        tb = (top_by or "combined").strip().lower()
        if tb not in ALLOWED_TOP_BY:
            raise ValueError(f"top_by must be one of {sorted(ALLOWED_TOP_BY)}")

        # enforce base for scatter
        stype = (stat_type or "base").strip().lower()
        if stype != "base":
            stype = "base"

        # seasons → list[int]
        if seasons is None:
            raise ValueError("seasons is required")
        if isinstance(seasons, (int, str)):
            seasons = [seasons]
        clean_seasons = []
        for s in seasons:
            if s is None:
                continue
            try:
                clean_seasons.append(int(s))
            except Exception:
                # tolerate comma strings like "2023,2024"
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
        }

        last_err = None
        for prefix in API_PREFIXES:
            url = f"{API_BASE}{prefix}/analytics_nexus/player/scatter/{mx}/{my}/{pos}/{int(top_n)}"
            try:
                if debug:
                    print(f"[api_client] GET {url} seasons={seasons} params={params}")
                # requests will encode list -> repeated ?seasons=YYYY&seasons=YYYY
                r = requests.get(url, params={**params, "seasons": seasons}, timeout=timeout)
                if r.status_code == 404:
                    last_err = f"404 at {url}"
                    continue
                r.raise_for_status()
                data = r.json()
                if isinstance(data, dict):
                    if debug:
                        pts = len(data.get("points", []) or [])
                        print(f"[api_client] OK scatter -> {pts} points")
                    return data
                if debug:
                    print(f"[api_client] Unexpected payload at {url}: {type(data)}")
                return {}
            except Exception as e:
                last_err = str(e)
                continue

        if debug:
            print(f"[api_client] Scatter failed after trying {API_PREFIXES}: {last_err}")
        return {}
    except Exception as e:
        print(f"[api_client] Failed to fetch player scatter: {e}")
        return {}

# ============================
# API client — Player Rolling Percentiles
# ============================

# ============================ Analytics Nexus (Players — Rolling) ===============
# NOTE: This block redefines API_BASE to "http://nfl_api_py:8000" and declares
#       a local _api_url() helper. This intentionally overrides the earlier
#       env-based API_BASE. We leave this intact to preserve current behaviour.
API_BASE = "http://nfl_api_py:8000"

def _api_url(path: str) -> str:
    """Build a full API URL using the local (overriding) API_BASE."""
    return f"{API_BASE}{path}"

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
    Call Analytics Nexus: Player Rolling Percentiles.

    Path params:
      /analytics_nexus/player/rolling_percentiles/{metric}/{position}/{top_n}

    Query params:
      seasons (repeatable), season_type, stat_type, week_start, week_end,
      rolling_window, debug
    """
    try:
        # --- Normalize ---
        pos   = (position or "").upper().strip()
        stype = (stat_type or "base").lower().strip()
        st    = (season_type or "REG").upper().strip()

        # --- Clean seasons ---
        if seasons is None:
            raise ValueError("seasons is required")
        if isinstance(seasons, (int, str)):
            seasons = [seasons]

        clean_seasons = []
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

        # --- URL ---
        path = f"/analytics_nexus/player/rolling_percentiles/{metric}/{pos}/{int(top_n)}"
        url = _api_url(path)
        params = {
            "seasons": seasons,   # list[int] → encoded as repeated ?seasons=2024&seasons=2025
            "season_type": st,
            "stat_type": stype,
            "week_start": int(week_start),
            "week_end": int(week_end),
            "rolling_window": int(rolling_window),
            "debug": str(bool(debug)).lower(),
        }

        if debug:
            print("[ROLLING DEBUG] URL:", url)
            print("[ROLLING DEBUG] Params:", params)

        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        if debug:
            print(f"[ROLLING DEBUG] OK -> {len(data.get('series', []))} series rows")

        return data

    except Exception as e:
        print(f"[fetch_player_rolling_percentiles] error: {e}")
        return {"series": [], "players": [], "meta": {}}
      
# --- Analytics Nexus (Teams — Trajectories) -------------------------------------
def fetch_team_trajectories(
    stat_name: str,
    top_n: int,
    seasons: List[int],
    season_type: str = "REG",
    week_start: int = 1,
    week_end: int = 18,
    rank_by: str = "sum",       # 'sum' or 'mean'
    stat_type: str = "base",    # 'base' or 'cumulative' (view mode)
    highlight: Optional[Union[str, List[str]]] = None,  # not used server-side yet
    timeout: int = 4,
    debug: bool = True,
):
    """Analytics Nexus: Top-N team weekly trajectories.
    
    Args mirror server: seasons (list[int]), season_type, week window, rank_by, stat_type.
    
    Returns:
        list[dict]: ordered by season, team_rank, week; [] on error/empty.
    """

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

        last_err = None
        for prefix in API_PREFIXES:
            url = f"{API_BASE}{prefix}/analytics_nexus/team/trajectories/{stat_seg}/{int(top_n)}"
            try:
                if debug:
                    print(f"[api_client] GET {url} params={params}")
                r = requests.get(url, params=params, timeout=timeout)
                if r.status_code == 404:
                    last_err = f"404 at {url}"
                    continue
                r.raise_for_status()
                data = r.json()
                if isinstance(data, dict) and data.get("error"):
                    if debug: print(f"[api_client] Empty (error): {data.get('error')}")
                    return []
                if isinstance(data, list):
                    if debug: print(f"[api_client] OK {url} -> {len(data)} rows")
                    return data
                if debug: print(f"[api_client] Unexpected payload at {url}: {type(data)}")
                return []
            except Exception as e:
                last_err = str(e)
                continue

        if debug:
            print(f"[api_client] Failed after trying {API_PREFIXES}: {last_err}")
        return []
    except Exception as e:
        print(f"[api_client] Failed to fetch team trajectories: {e}")
        return []
      
# --- Analytics Nexus (Teams — Violins) ------------------------------------------
ALLOWED_TEAM_ORDER_BY = {"rCV", "IQR", "median"}

def fetch_team_violins(
    seasons,
    season_type: str,
    stat_name: str,
    top_n: int,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",              # 'base' | 'cumulative'
    order_by: str = "rCV",                # 'rCV' | 'IQR' | 'median'
    min_games_for_badges: int = 6,        # only affects badges; does NOT filter teams
    timeout: int = 5,
    debug: bool = True,
):
    """Analytics Nexus: Team consistency/volatility violins.
    
    Returns:
        dict: {"weekly":[...], "summary":[...], "badges":{...}, "meta":{...}}
              Empty-shaped payload on error.
    """

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
        # --- Normalize seasons → sorted unique list[int] ---
        if seasons is None:
            seasons_list = []
        elif isinstance(seasons, (list, tuple, set)):
            seasons_list = [int(s) for s in seasons if s is not None]
        elif isinstance(seasons, int):
            seasons_list = [seasons]
        else:
            # tolerate "2023,2024"
            seasons_list = [int(s.strip()) for s in str(seasons).split(",") if s.strip()]

        seasons_list = sorted(set(seasons_list))
        if not seasons_list:
            return _empty_payload([])

        # --- Validate enums ---
        st = (season_type or "REG").upper().strip()
        if st not in ALLOWED_SEASON_TYPES:
            raise ValueError(f"season_type must be one of {sorted(ALLOWED_SEASON_TYPES)}")

        stype = (stat_type or "base").lower().strip()
        if stype not in {"base", "cumulative"}:
            raise ValueError("stat_type must be 'base' or 'cumulative'")

        ob_norm = (order_by or "rCV").strip()
        if ob_norm.lower() == "rcv":
            ob_final = "rCV"
        elif ob_norm.lower() == "iqr":
            ob_final = "IQR"
        elif ob_norm.lower() == "median":
            ob_final = "median"
        else:
            raise ValueError(f"order_by must be one of {sorted(ALLOWED_TEAM_ORDER_BY)}")

        ws = max(1, int(week_start))
        we = min(22, int(week_end))
        if we < ws:
            return _empty_payload(seasons_list)

        tn = max(1, int(top_n))
        mg = max(0, int(min_games_for_badges))

        # --- URL & params ---
        stat_seg = quote(str(stat_name), safe="")
        params = {
            "seasons": seasons_list,    # requests encodes as repeated ?seasons=YYYY
            "season_type": st,
            "week_start": ws,
            "week_end": we,
            "stat_type": stype,
            "order_by": ob_final,
            "min_games_for_badges": mg,
        }

        last_err = None
        for prefix in API_PREFIXES:
            url = f"{API_BASE}{prefix}/analytics_nexus/team/violins/{stat_seg}/{tn}"
            try:
                if debug:
                    print(f"[api_client] GET {url} params={params}")
                r = requests.get(url, params=params, timeout=timeout)
                if r.status_code == 404:
                    last_err = f"404 at {url}"
                    continue
                r.raise_for_status()
                data = r.json()

                if isinstance(data, dict) and "weekly" in data and "summary" in data:
                    if debug:
                        print(f"[api_client] OK team violins -> weekly={len(data.get('weekly', []))}, summary={len(data.get('summary', []))}")
                    return data

                if debug:
                    print(f"[api_client] Unexpected payload (team violins): type={type(data)}; keys={list(data) if isinstance(data, dict) else 'n/a'}")
                return _empty_payload(seasons_list)

            except Exception as e:
                last_err = str(e)
                continue

        if debug:
            print(f"[api_client] Team violins failed after trying {API_PREFIXES}: {last_err}")
        return _empty_payload(seasons_list)

    except Exception as e:
        print(f"[api_client] Failed to fetch team violins: {e}")
        return _empty_payload(seasons_list if 'seasons_list' in locals() else [])

# --- Analytics Nexus (Teams — Scatter) ------------------------------------------
def fetch_team_scatter(
    seasons,
    season_type: str,
    metric_x: str,
    metric_y: str,
    top_n: int = 20,
    week_start: int = 1,
    week_end: int = 18,
    stat_type: str = "base",                # scatter uses 'base' only
    top_by: str = "combined",               # combined | x_gate | y_gate | x_value | y_value
    log_x: bool = False,
    log_y: bool = False,
    label_all_points: bool = True,          # UI decides label density
    timeout: int = 5,
    debug: bool = True,
):
    """Analytics Nexus: Team quadrant scatter.
    
    Returns:
        dict: {"points":[...], "meta":{...}} or {} on error.
    Notes:
        Enforces 'base' stat_type; seasons can be list or comma string.
    """

    try:
        # --- Validate season_type ---
        st = (season_type or "").upper().strip()
        if st not in ALLOWED_SEASON_TYPES:
            raise ValueError(f"season_type must be one of {sorted(ALLOWED_SEASON_TYPES)}")

        # --- Enforce 'base' for scatter ---
        stype = (stat_type or "base").lower().strip()
        if stype != "base":
            stype = "base"

        # --- Validate top_by ---
        tb = (top_by or "combined").strip().lower()
        if tb not in ALLOWED_TOP_BY:
            raise ValueError(f"top_by must be one of {sorted(ALLOWED_TOP_BY)}")

        # --- Normalize seasons → sorted unique list[int] ---
        if seasons is None:
            raise ValueError("seasons is required")
        if isinstance(seasons, (int, str)):
            seasons = [seasons]
        clean_seasons: List[int] = []
        for s in seasons:
            if s is None:
                continue
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

        # --- Encode path segments safely ---
        mx = quote(str(metric_x), safe="")
        my = quote(str(metric_y), safe="")

        # --- Query params ---
        params = {
            "season_type": st,
            "week_start": int(week_start),
            "week_end": int(week_end),
            "stat_type": stype,
            "top_by": tb,
            "log_x": bool(log_x),
            "log_y": bool(log_y),
            "label_all_points": bool(label_all_points),
        }

        last_err = None
        for prefix in API_PREFIXES:
            url = f"{API_BASE}{prefix}/analytics_nexus/team/scatter/{mx}/{my}/{int(top_n)}"
            try:
                if debug:
                    print(f"[api_client] GET {url} seasons={seasons} params={params}")
                # `requests` encodes list -> repeated ?seasons=YYYY&seasons=YYYY
                r = requests.get(url, params={**params, "seasons": seasons}, timeout=timeout)
                if r.status_code == 404:
                    last_err = f"404 at {url}"
                    continue
                r.raise_for_status()
                data = r.json()
                if isinstance(data, dict) and "points" in data and "meta" in data:
                    if debug:
                        print(f"[api_client] OK team scatter -> {len(data.get('points', []) or [])} points")
                    return data
                if debug:
                    print(f"[api_client] Unexpected payload (team scatter): type={type(data)}")
                return {}
            except Exception as e:
                last_err = str(e)
                continue

        if debug:
            print(f"[api_client] Team scatter failed after trying {API_PREFIXES}: {last_err}")
        return {}
    except Exception as e:
        print(f"[api_client] Failed to fetch team scatter: {e}")
        return {}
      
# --- Analytics Nexus (Teams — Rolling) ------------------------------------------
def fetch_team_rolling_percentiles(
    seasons,
    season_type="REG",            # "REG" | "POST" | "ALL"
    metric="rushing_epa",         # stat_name in storage
    top_n=16,
    week_start=1,
    week_end=18,
    stat_type="base",             # "base" | "cumulative"
    rolling_window=4,
    timeout=8,
    debug=False,
):
    """
    Call the Analytics Nexus router to get Team Rolling Form Percentiles.

    Returns a payload shaped like:
      {
        "series": [
          {
            "team": "KC", "season": 2024, "season_type": "REG", "week": 7,
            "t_idx": 13, "pct": 72.4, "pct_roll": 68.1,
            "team_color": "#E31837", "team_color2": "#FFB81C",
            "team_order": 3
          }, ...
        ],
        "teams": [
          {
            "team": "KC",
            "team_color": "#E31837",
            "team_color2": "#FFB81C",
            "last_pct": 74.2,
            "team_order": 3
          }, ...
        ],
        "meta": { ... }
      }
    """
    if not API_BASE:
        # keep the UI stable even if env is missing
        return {
            "series": [],
            "teams": [],
            "meta": {
                "metric": metric,
                "metric_label": metric.replace("_", " ").title(),
                "stat_type": stat_type,
                "season_type": season_type,
                "seasons": seasons or [],
                "week_start": week_start,
                "week_end": week_end,
                "top_n": int(top_n),
                "rolling_window": int(rolling_window),
                "error": "BACKEND_BASE_URL is not set",
            },
        }

    url = f"{API_BASE}/analytics_nexus/team/rolling_percentiles/{metric}/{int(top_n)}"

    # requests encodes list values as repeatable query params: ?seasons=2023&seasons=2024
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

    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json() or {}
        # normalize minimal shape for safety
        if not isinstance(payload, dict):
            return {"series": [], "teams": [], "meta": {"error": "Unexpected response"}}
        payload.setdefault("series", [])
        payload.setdefault("teams", [])
        payload.setdefault("meta", {})
        return payload
    except requests.RequestException as e:
        # stable fallback on transport errors
        return {
            "series": [],
            "teams": [],
            "meta": {
                "metric": metric,
                "metric_label": metric.replace("_", " ").title(),
                "stat_type": stat_type,
                "season_type": season_type,
                "seasons": seasons or [],
                "week_start": week_start,
                "week_end": week_end,
                "top_n": int(top_n),
                "rolling_window": int(rolling_window),
                "error": str(e),
            },
        }
