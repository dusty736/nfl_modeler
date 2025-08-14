import os
import httpx
import pandas as pd
from dash import html, dash_table
import requests

API_BASE = os.getenv("API_BASE", "http://api:8000") + "/api"

def fetch_standings():
    """
    Fetch division standings from the API.
    Returns:
        (pd.DataFrame, str or None):
            DataFrame of standings (empty if error),
            Error message (None if no error)
    """
    try:
        with httpx.Client(timeout=10.0) as c:
            r = c.get(f"{API_BASE}/standings")
            r.raise_for_status()
            return pd.DataFrame(r.json()["items"]), None
    except Exception as e:
        return pd.DataFrame(), str(e)


def division_table(df: pd.DataFrame, title: str):
    """
    Create a styled Dash DataTable for a single division.
    Args:
        df (pd.DataFrame): DataFrame containing columns:
            team_id, wins, losses, ties, point_diff, team_color, team_color2
        title (str): Division title for the table.
    Returns:
        html.Div: Styled DataTable wrapped in a Div.
    """
    def norm_color(c, default):
        if not c or pd.isna(c):
            return default
        c = str(c).strip()
        return c if (c.startswith("#") or c.startswith("rgb")) else f"#{c.lstrip('#')}"

    # Conditional formatting for each team row
    rules = []
    for _, r in df.iterrows():
        bg = norm_color(r.get("team_color"), "#ffffff")
        fg = norm_color(r.get("team_color2"), "#000000")
        rules.append({
            "if": {"filter_query": f'{{team_id}} = "{r["team_id"]}"'},
            "backgroundColor": bg,
            "color": fg,
        })

    cols = [
        {"name": "Team", "id": "team_id"},
        {"name": "W", "id": "wins"},
        {"name": "L", "id": "losses"},
        {"name": "T", "id": "ties"},
        {"name": "PD", "id": "point_diff"},
    ]

    return html.Div([
        html.H5(title, style={"margin": "0.25rem 0"}),
        dash_table.DataTable(
            data=df[["team_id", "wins", "losses", "ties", "point_diff"]].to_dict("records"),
            columns=cols,
            page_size=8,
            sort_action="native",
            style_header={"fontWeight": "700", "backgroundColor": "#f7f7f7"},
            style_cell={"padding": "6px", "textAlign": "left"},
            style_data_conditional=rules
        )
    ], style={"padding": "0.5rem", "border": "1px solid #eee", "borderRadius": "12px"})

def get_standings_conference():
    """
    Fetch AFC/NFC standings from the API and return two DataFrames + err.
    """
    try:
        resp = requests.get(f"{API_BASE}/standings/conference", timeout=5)
        resp.raise_for_status()
        payload = resp.json()
        afc_df = pd.DataFrame(payload.get("afc", []))
        nfc_df = pd.DataFrame(payload.get("nfc", []))
        return afc_df, nfc_df, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), e
