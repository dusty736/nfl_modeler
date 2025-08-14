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
