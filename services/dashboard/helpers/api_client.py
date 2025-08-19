import requests

def fetch_current_season_week():
    try:
        r = requests.get("http://api:8000/api/season-week", timeout=2)
        r.raise_for_status()
        data = r.json()
        return data.get("season"), data.get("week")
    except Exception:
        return None, None

def fetch_primetime_games():
    """
    Fetch primetime games for the current season/week.
    Returns a list of dicts, each representing a game.
    """
    try:
        response = requests.get("http://api:8000/api/primetime-games", timeout=2)
        response.raise_for_status()
        data = response.json()
        return data.get("games", [])
    except Exception as e:
        print(f"[api_client] Failed to fetch primetime games: {e}")
        return []

def get_all_teams():
    try:
        r = requests.get("http://api:8000/teams/", timeout=2)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch teams: {e}")
        return []

def get_team_by_abbr(team_abbr: str):
    try:
        r = requests.get(f"http://api:8000/teams/{team_abbr}", timeout=2)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[api_client] Failed to fetch team abbr: {e}")
        return []
      
def get_team_record(team_abbr: str, season: int, week: int):
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
      
      # --- Roster: full team roster ---
def get_team_roster(team_abbr: str, season: int):
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

def fetch_max_week(season: int) -> int:
    r = requests.get(f"http://api:8000/api/max-week/{season}", timeout=2)
    if r.ok:
        return r.json().get("max_week", 18)
    return 18
  
def get_max_week_team(season: int, team: str) -> int:
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

# --- Injuries: team-level summary ---
def get_team_injury_summary(team_abbr: str, season: int, week: int, position: str):
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

