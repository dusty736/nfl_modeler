# dashboard/pages/games.py
# -- minimal updates to align with new query fields (no layout changes) --

import math
from datetime import datetime

import dash
from dash import html, dcc, callback, Input, Output, State
from dash.dash_table import DataTable
import pandas as pd
import pytz

from helpers import api_client  # module import to avoid symbol import issues

dash.register_page(__name__, path="/games", name="Game Center")

eastern = pytz.timezone("US/Eastern")

def format_kickoff_et(iso_like: str) -> str:
    if not iso_like:
        return "TBD"
    try:
        dt_str = iso_like[:19]
        naive_dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
        kickoff_et = eastern.localize(naive_dt)
        return kickoff_et.strftime("%a, %b %-d — %-I:%M %p ET")
    except Exception:
        return "TBD"

def blank(v):
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return v

def build_table_rows(raw_rows, season, week):
    rows = []
    for r in raw_rows or []:
        home = r.get("home_team")
        away = r.get("away_team")
        home_logo_url = dash.get_asset_url(f"logos/{home}.png")
        away_logo_url = dash.get_asset_url(f"logos/{away}.png")
        home_rec = r.get("home_record") or ""
        away_rec = r.get("away_record") or ""

        home_cell = f"![{home}]({home_logo_url})  **{home}**" + (f" ({home_rec})" if home_rec else "")
        away_cell = f"![{away}]({away_logo_url})  **{away}**" + (f" ({away_rec})" if away_rec else "")

        rows.append({
            "home": home_cell,
            "away": away_cell,
            "kickoff": format_kickoff_et(r.get("kickoff")),
            "stadium": blank(r.get("stadium")),
            "line": blank(r.get("line")),
            "vegas_total": blank(r.get("vegas_total")),
            "pred_total": blank(r.get("pred_total")),
            "pred_margin": blank(r.get("pred_margin")),
            "pred_winner": blank(r.get("pred_winner_team")),  # unchanged display
            "home_score": blank(r.get("home_score")),
            "away_score": blank(r.get("away_score")),
            # hidden metadata
            "game_id": r.get("game_id"),
            "season": season,
            "week": week,
            "winning_team": r.get("winning_team"),  # NEW: available for future styling/logic
        })
    return rows

def season_week_defaults():
    try:
        cw = (api_client.get_current_week() if hasattr(api_client, "get_current_week") else None) or {}
        season = int(cw.get("season", 2025))
        week = int(cw.get("week", 1))
    except Exception:
        season, week = 2025, 1
    return season, week

def layout():
    season_init, week_init = season_week_defaults()
    try:
        raw = api_client.get_games_week(season_init, week_init) if hasattr(api_client, "get_games_week") else []
    except Exception:
        raw = []

    data_init = build_table_rows(raw, season_init, week_init)

    season_options = [{"label": str(y), "value": y} for y in range(2009, season_init + 1)][::-1]
    week_options = [{"label": str(w), "value": w} for w in range(1, 23)]

    YOUR_NAME = "Dustin Burnham"
    YOUR_EMAIL = "you@example.com"
    YOUR_GITHUB = "dusty736"

    header = html.Header(
        className="topbar",
        children=[
            html.Div(
                className="topbar-inner",
                children=[
                    dcc.Link(
                        html.Div(
                            className="brand-badge",
                            children=[
                                html.Img(
                                    src=dash.get_asset_url("logos/dashboard_emblem.png"),
                                    alt="Dashboard emblem",
                                    className="brand-img",
                                )
                            ],
                        ),
                        href="/",
                        className="logo-link",
                    ),
                    html.Div(
                        className="topbar-center",
                        children=[
                            html.H1("NFL Analytics Dashboard", className="topbar-title"),
                            html.Nav(
                                className="topbar-actions",
                                children=[
                                    dcc.Link(html.Button("Home", className="btn"), href="/"),
                                    dcc.Link(html.Button("Standings", className="btn"), href="/overview"),
                                    dcc.Link(html.Button("Teams", className="btn"), href="/teams"),
                                    dcc.Link(html.Button("Game Center", className="btn primary"), href="/games"),
                                    dcc.Link(html.Button("Analytics Nexus", className="btn"), href="/analytics_nexus"),
                                ],
                            ),
                        ],
                    ),
                    html.Div(className="topbar-right"),
                ],
            )
        ],
    )

    footer = html.Footer(
        className="bottombar",
        children=[
            html.Div(
                className="bottombar-inner",
                children=[
                    html.Div(
                        className="footer-col footer-me",
                        children=[
                            html.H4("About"),
                            html.Ul(
                                [
                                    html.Li([html.Strong(""), YOUR_NAME]),
                                    html.Li([html.Strong("Email: "), html.A(YOUR_EMAIL, href=f"mailto:{YOUR_EMAIL}", className="footer-link")]),
                                    html.Li([html.Strong("GitHub: "), html.A(f"@{YOUR_GITHUB}", href=f"https://github.com/{YOUR_GITHUB}", target="_blank", rel="noopener noreferrer", className="footer-link")]),
                                ],
                                className="footer-list",
                            ),
                        ],
                    ),
                    html.Div(
                        className="footer-col footer-logos",
                        children=[
                            html.Div("Built with", className="footer-kicker"),
                            html.Div(
                                className="logo-row",
                                children=[
                                    html.Img(src=dash.get_asset_url("logos/R_logo.png"), alt="R logo", className="footer-tech-logo"),
                                    html.Img(src=dash.get_asset_url("logos/python_logo.png"), alt="Python logo", className="footer-tech-logo"),
                                ],
                            ),
                        ],
                    ),
                    html.Div(
                        className="footer-col footer-credits",
                        children=[
                            html.H4("Credits"),
                            html.Div(
                                className="footer-small",
                                children=[
                                    html.Div("This project is non-commercial and purely educational."),
                                    html.Div(["Special thanks to ", html.Span("ChatGPT", className="footer-mention"), " for assistance."]),
                                    html.Div(["Data & tools include the ", html.Span("nflfastR", className="footer-mention"), " R package."]),
                                ],
                            ),
                        ],
                    ),
                ],
            )
        ],
    )

    controls = html.Div(
        className="games-controls",
        children=[
            html.Div(
                className="control",
                children=[
                    html.Label("Season", className="control-label"),
                    dcc.Dropdown(
                        id="games-season",
                        options=season_options,
                        value=season_init,
                        clearable=False,
                        className="dd dd-compact",
                    ),
                ],
            ),
            html.Div(
                className="control",
                children=[
                    html.Label("Week", className="control-label"),
                    dcc.Dropdown(
                        id="games-week",
                        options=week_options,
                        value=week_init,
                        clearable=False,
                        className="dd dd-compact",
                    ),
                ],
            ),
        ],
    )

    columns = [
        {"name": "Home",         "id": "home",         "presentation": "markdown"},
        {"name": "Away",         "id": "away",         "presentation": "markdown"},
        {"name": "Kickoff",      "id": "kickoff"},
        {"name": "Stadium",      "id": "stadium"},
        {"name": "Line",         "id": "line"},
        {"name": "Vegas Total",  "id": "vegas_total"},
        {"name": "winning_team", "id": "winning_team"},
        {"name": "Home Score",   "id": "home_score"},
        {"name": "Away Score",   "id": "away_score"},
        {"name": "Pred. Winner", "id": "pred_winner"},
        {"name": "Pred. Total",  "id": "pred_total"},
        {"name": "Pred. Margin", "id": "pred_margin"},
        {"name": "game_id",      "id": "game_id"},
        {"name": "season",       "id": "season"},
        {"name": "week",         "id": "week"},
    ]

    table = DataTable(
        id="games-table",
        columns=columns,
        data=data_init,
        hidden_columns=["game_id", "season", "week"],
        page_action="none",
        style_table={"overflowX": "auto", "backgroundColor": "transparent"},
        style_cell={  # ensure readable text on platinum
            "whiteSpace": "nowrap",
            "textAlign": "left",
            "padding": "10px",
            "backgroundColor": "transparent",
            "color": "#111",
            "border": "none",
        },
        style_header={
            "backgroundColor": "#F4F3F1",  # platinum-light
            "color": "#0B1620",            # ink
            "fontWeight": "700",
            "border": "none",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "rgba(0,0,0,0.03)"},
            {"if": {"state": "active"}, "backgroundColor": "rgba(0,0,0,0.06)"},
            {"if": {"column_id": ["home", "away"]}, "fontWeight": "600"},
        ],
        markdown_options={"link_target": "_self"},
    )

    main_children = [
        html.H2("Game Center", className="page-title"),
        controls,
        html.Div(dcc.Loading(table, type="dot"), className="games-table-wrap"),
        dcc.Location(id="games-nav", refresh=True),
    ]

    return html.Div(
        [
            header,
            html.Main(className="home-content fullwidth", children=main_children),
            footer,
        ],
        className="home-page",
    )

@callback(
    Output("games-table", "data"),
    Input("games-season", "value"),
    Input("games-week", "value"),
)
def _update_games_table(season, week):
    try:
        raw = api_client.get_games_week(int(season), int(week)) if hasattr(api_client, "get_games_week") else []
    except Exception:
        raw = []
    return build_table_rows(raw, season, week)

@callback(
    Output("games-nav", "href"),
    Input("games-table", "active_cell"),
    State("games-table", "data"),
    State("games-season", "value"),
    State("games-week", "value"),
    prevent_initial_call=True,
)
def _row_click_to_detail(active_cell, rows, season, week):
    if not active_cell or not rows:
        return dash.no_update
    idx = active_cell.get("row")
    if idx is None or idx < 0 or idx >= len(rows):
        return dash.no_update
    game_id = rows[idx].get("game_id")
    if not game_id:
        return dash.no_update
    return f"/games/{season}/{week}/{game_id}"
