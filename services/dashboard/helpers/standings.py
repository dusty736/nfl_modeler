# standings.py

# helpers/standings.py (top of file)
import pandas as pd
from dash import html, dash_table

from helpers.api_client import _get_json_resilient

# -----------------------------
# Data fetchers (resilient)
# -----------------------------

def fetch_standings():
    """
    Fetch division standings from the API.

    Returns:
        (pd.DataFrame, str | None):
            DataFrame of standings (empty if error),
            Error message (None if no error)
    """
    try:
        data = _get_json_resilient("/standings", timeout=10)
        items = (data or {}).get("items", []) if isinstance(data, dict) else (data or [])
        return pd.DataFrame(items), None
    except Exception as e:
        return pd.DataFrame(), str(e)


def get_standings_conference():
    """
    Fetch AFC/NFC standings from the API.

    Returns:
        (pd.DataFrame, pd.DataFrame, Exception | None):
            AFC DataFrame, NFC DataFrame, error (None if OK)
    """
    try:
        payload = _get_json_resilient("/standings/conference", timeout=5) or {}
        afc_df = pd.DataFrame(payload.get("afc", []))
        nfc_df = pd.DataFrame(payload.get("nfc", []))
        return afc_df, nfc_df, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), e


# -----------------------------
# UI helpers
# -----------------------------

def division_table(df: pd.DataFrame, title: str):
    """
    Create a styled Dash DataTable for a single division.

    Args:
        df (pd.DataFrame): columns expected:
            team_id, wins, losses, ties, point_diff, team_color, team_color2
        title (str): Division title for the table.

    Returns:
        html.Div
    """
    def _norm_color(c, default):
        if not c or pd.isna(c):
            return default
        c = str(c).strip()
        return c if (c.startswith("#") or c.startswith("rgb")) else f"#{c.lstrip('#')}"

    # Conditional formatting for each team row
    rules = []
    if not df.empty:
        for _, r in df.iterrows():
            bg = _norm_color(r.get("team_color"), "#ffffff")
            fg = _norm_color(r.get("team_color2"), "#000000")
            rules.append({
                "if": {"filter_query": f'{{team_id}} = "{r.get("team_id", "")}"'},
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

    safe_cols = [c["id"] for c in cols if c["id"] in df.columns]
    data_records = df[safe_cols].to_dict("records") if safe_cols else []

    return html.Div(
        [
            html.H5(title, style={"margin": "0.25rem 0"}),
            dash_table.DataTable(
                data=data_records,
                columns=[c for c in cols if c["id"] in safe_cols],
                page_size=8,
                sort_action="native",
                style_header={"fontWeight": "700", "backgroundColor": "#f7f7f7"},
                style_cell={"padding": "6px", "textAlign": "left"},
                style_data_conditional=rules,
            ),
        ],
        style={"padding": "0.5rem", "border": "1px solid #eee", "borderRadius": "12px"},
    )

