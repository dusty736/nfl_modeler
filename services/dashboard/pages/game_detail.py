# dashboard/pages/game_detail.py

import dash
from dash import html, dcc, callback, Input, Output, no_update
from dash.dash_table import DataTable
from urllib.parse import unquote

from helpers import api_client

from datetime import datetime
import pytz

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


YOUR_NAME = "Dustin Burnham"
YOUR_EMAIL = "you@example.com"
YOUR_GITHUB = "dusty736"

dash.register_page(
    __name__,
    path_template="/games/<season>/<week>/<game_id>",
    name="Game Detail",
)

# ---- Labels ----
LABELS = {
    "Total Yards": "Total Yards",
    "Total First Downs": "Total First Downs",
    "Total TDs (Offense)": "Total TDs (Offense)",
    "Total Turnovers (Off.)": "Total Turnovers (Off.)",
    "Total Penalties": "Total Penalties",
    "Total Penalty Yards": "Total Penalty Yards",
    "Pass Attempts": "Pass Attempts",
    "Pass Completions": "Pass Completions",
    "Passing Yards": "Passing Yards",
    "Passing TDs": "Passing TDs",
    "Interceptions": "Interceptions",
    "Passing 1st Downs": "Passing 1st Downs",
    "Sacks Taken": "Sacks Taken",
    "Sack Yards Lost": "Sack Yards Lost",
    "Carries": "Carries",
    "Rushing Yards": "Rushing Yards",
    "Rushing TDs": "Rushing TDs",
    "Rushing 1st Downs": "Rushing 1st Downs",
    "Rushing Fumbles": "Rushing Fumbles",
    "Rushing Fumbles Lost": "Rushing Fumbles Lost",

    "Def. Sacks": "Def. Sacks",
    "QB Hits": "QB Hits",
    "Interceptions": "Interceptions",
    "Passes Defended": "Passes Defended",
    "Tackles (Total)": "Tackles (Total)",
    "TFL": "Tackles For Loss",
    "Forced Fumbles": "Forced Fumbles",
    "Def. Fumbles": "Def. Fumbles",
    "Def. TDs": "Def. TDs",
    "Safeties": "Safeties",
    "Penalty Yards": "Penalty Yards",
    "Penalties": "Penalties",

    "FG Att": "FG Att",
    "FG Made": "FG Made",
    "FG Blocked": "FG Blocked",
    "PAT Att": "PAT Att",
    "PAT Made": "PAT Made",
    "PAT Blocked": "PAT Blocked",
    "FG Made 0-19": "FG Made 0-19",
    "FG Made 20-29": "FG Made 20-29",
    "FG Made 30-39": "FG Made 30-39",
    "FG Made 40-49": "FG Made 40-49",
    "FG Made 50-59": "FG Made 50-59",
    "FG Made 60+": "FG Made 60+",
}

# asset aliasing (logos folder uses standard abbrs)
ALIAS_TO_ASSET = {
    "LAR": "LA",
    "JAC": "JAX",
    "WSH": "WAS",
    "SD": "SD", "STL": "STL", "OAK": "OAK", "LAC": "LAC", "LV": "LV",
}

def _asset_for_team(abbr: str) -> str:
    code = (abbr or "").upper().strip()
    return ALIAS_TO_ASSET.get(code, code)

def _logo_img(team_abbr: str, cls: str = "gd-logo"):
    asset = _asset_for_team(team_abbr)
    src = dash.get_asset_url(f"logos/{asset}.png") if asset else dash.get_asset_url("logos/dashboard_emblem.png")
    alt = f"{team_abbr} logo" if team_abbr else "logo"
    return html.Img(src=src, className=cls, alt=alt)

def _parse_ids_from_path(pathname: str):
    try:
        parts = (pathname or "").strip("/").split("/")
        if len(parts) >= 4 and parts[0] == "games":
            return int(parts[1]), int(parts[2]), unquote(parts[3])
    except Exception:
        pass
    return None, None, None

def _parse_teams_from_gid(game_id: str):
    try:
        toks = (game_id or "").split("_")
        if len(toks) >= 4:
            return toks[-2].upper(), toks[-1].upper()
    except Exception:
        pass
    return "", ""

def _best_teams(detail: dict, stats: dict, game_id: str):
    # Prefer detail → stats → URL-derived
    h1 = (detail or {}).get("home_team")
    a1 = (detail or {}).get("away_team")
    h2 = (stats or {}).get("home_team")
    a2 = (stats or {}).get("away_team")
    if not (h1 and a1):
        h3, a3 = _parse_teams_from_gid(game_id)
    else:
        h3, a3 = "", ""
    home = (h1 or h2 or h3 or "").upper()
    away = (a1 or a2 or a3 or "").upper()
    return home, away

def _header_from(detail: dict, stats: dict, game_id: str):
    home, away = _best_teams(detail, stats, game_id)

    # Records / lines / etc from detail (fallbacks safe)
    home_rec = (detail or {}).get("home_record") or ""
    away_rec = (detail or {}).get("away_record") or ""
    kickoff  = format_kickoff_et((detail or {}).get("kickoff"))
    stadium  = (detail or {}).get("stadium") or "TBD"
    line     = (detail or {}).get("line") or "—"
    vegas    = (detail or {}).get("vegas_total")
    vegas_s  = f"Total {vegas}" if vegas is not None else "Total —"
    pred_win = (detail or {}).get("pred_winner_team") or ""
    h_score  = (detail or {}).get("home_score")
    a_score  = (detail or {}).get("away_score")

    # Subtle highlight ring for model pick
    home_classes = "gd-side gd-home" + (" pred-winner" if pred_win and pred_win.upper() == home else "")
    away_classes = "gd-side gd-away" + (" pred-winner" if pred_win and pred_win.upper() == away else "")

    # Score pills show only if scores exist
    h_score_el = html.Span(str(h_score), className="gd-score") if h_score is not None else None
    a_score_el = html.Span(str(a_score), className="gd-score") if a_score is not None else None

    return html.Div(
        id="gd-hero",
        className="gd-hero",
        children=[
            # Home side
            html.Div(
                className=home_classes,
                children=[
                    html.Div(className="gd-logo-wrap", children=_logo_img(home)),
                    html.Div(className="gd-teamline", children=[
                        html.Div(home, className="gd-abbr"),
                        h_score_el,
                    ]),
                    html.Div(home_rec, className="gd-record"),
                ],
            ),

            # Middle meta / VS bar
            html.Div(
                className="gd-mid",
                children=[
                    html.Div("Game Center", className="gd-title"),
                    html.Div(className="gd-vs", children="vs"),
                    html.Div(className="gd-meta-chips", children=[
                        html.Span(kickoff, className="gd-chip"),
                        html.Span(stadium, className="gd-chip"),
                        html.Span(f"Line {line}", className="gd-chip"),
                        html.Span(vegas_s, className="gd-chip"),
                    ]),
                ],
            ),

            # Away side
            html.Div(
                className=away_classes,
                children=[
                    html.Div(className="gd-logo-wrap", children=_logo_img(away)),
                    html.Div(className="gd-teamline", children=[
                        html.Div(away, className="gd-abbr"),
                        a_score_el,
                    ]),
                    html.Div(away_rec, className="gd-record"),
                ],
            ),
        ],
    )


def _format_rows(rows):
    out = []
    for r in rows or []:
        metric = LABELS.get(r.get("metric"), r.get("metric"))
        home = r.get("home")
        away = r.get("away")
        out.append({"metric": metric or "", "home": "" if home is None else home, "away": "" if away is None else away})
    return out

def _placeholder_rows(msg="No stats available yet."):
    return [{"metric": msg, "home": "", "away": ""}]

# ---------- Layout ----------
def layout(season=None, week=None, game_id=None, **kwargs):
    header = html.Header(
        className="topbar",
        children=[
            html.Div(
                className="topbar-inner",
                children=[
                    html.Div(
                        className="topbar-left",
                        children=[
                            dcc.Link(
                                html.Img(
                                    src=dash.get_asset_url("logos/dashboard_emblem.png"),
                                    alt="Dashboard emblem",
                                    className="topbar-logo",
                                ),
                                href="/",
                                className="logo-link",
                            )
                        ],
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

    tabs = dcc.Tabs(
        id="gd-tabs",
        value="offense",
        children=[
            dcc.Tab(label="Offense", value="offense"),
            dcc.Tab(label="Defense", value="defense"),
            dcc.Tab(label="Special Teams", value="special"),
        ],
    )

    table = DataTable(
        id="game-detail-table",
        columns=[{"name": "Metric", "id": "metric"}, {"name": "Home", "id": "home"}, {"name": "Away", "id": "away"}],
        data=[],
        page_action="none",
        style_table={
            "overflowX": "auto",
            "backgroundColor": "transparent",
            # make it almost full width and centered
            "width": "98%",
            "margin": "0 auto",
        },
        style_cell={
            "whiteSpace": "nowrap",
            "textAlign": "left",
            "padding": "10px",
            "backgroundColor": "transparent",
            "color": "#111",
            "border": "none",
        },
        style_header={
            "backgroundColor": "#F4F3F1",
            "color": "#0B1620",
            "fontWeight": "700",
            "border": "none",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "rgba(0,0,0,0.03)"},
            {"if": {"state": "active"}, "backgroundColor": "rgba(0,0,0,0.06)"},
        ],
    )

    url = dcc.Location(id="gd-url")

    main_children = [
        header,
        # NOTE: fullwidth makes content span the page like the games page
        html.Main(
            className="home-content fullwidth",
            children=[
                html.Div(id="gd-header-wrap"),
                tabs,
                html.Div(dcc.Loading(table, type="dot"), className="games-table-wrap"),
                url,
            ],
        ),
        footer,
    ]
    return html.Div(main_children, className="home-page")

# ---------- Single hydration callback ----------
@callback(
    Output("gd-header-wrap", "children"),
    Output("game-detail-table", "data"),
    Output("game-detail-table", "columns"),
    Input("gd-url", "pathname"),
    Input("gd-tabs", "value"),
    prevent_initial_call=False,
)
def hydrate_game_detail(pathname, tab):
    season, week, game_id = _parse_ids_from_path(pathname)
    if not (season and week and game_id):
        return no_update, _placeholder_rows("Invalid game URL."), [
            {"name": "Metric", "id": "metric"},
            {"name": "Home", "id": "home"},
            {"name": "Away", "id": "away"},
        ]

    detail = {}
    stats = {}
    try:
        detail = api_client.get_game_detail(season, week, game_id) or {}
    except Exception as e:
        print("[game_detail] detail fetch error:", repr(e))
    try:
        stats = api_client.get_game_stats(season, week, game_id) or {}
    except Exception as e:
        print("[game_detail] stats fetch error:", repr(e))

    # debug counts (handy during dev)
    try:
        print(f"[game_detail] {game_id} → offense={len(stats.get('offense', []) or [])}, defense={len(stats.get('defense', []) or [])}, special={len(stats.get('special', []) or [])}")
    except Exception:
        pass

    header = _header_from(detail, stats, game_id)

    tab = (tab or "offense").lower()
    if tab == "defense":
        block = stats.get("defense")
    elif tab == "special":
        block = stats.get("special")
    else:
        block = stats.get("offense")

    rows = _format_rows(block) if block else _placeholder_rows()

    home_abbr, away_abbr = _best_teams(detail, stats, game_id)
    cols = [
        {"name": "Metric", "id": "metric"},
        {"name": home_abbr or "Home", "id": "home"},
        {"name": away_abbr or "Away", "id": "away"},
    ]

    return header, rows, cols
