# dashboard/pages/team_detail.py

import dash
from dash import html, dcc, callback, Input, Output
from helpers.api_client import (
    get_team_by_abbr,
    get_team_record,
    get_team_offense,
    get_team_defense,
    get_team_special,
    fetch_current_season_week,
    fetch_max_week,
    get_team_roster,
    get_team_position_summary,
    get_team_depth_chart_starters,
    get_max_week_team,
    get_team_injury_summary,
    get_player_injuries
)

season, week = fetch_current_season_week()

# Replace these with your real values
YOUR_NAME = "Dustin Burnham"
YOUR_EMAIL = "burnhamdustin@gmail.com"
YOUR_GITHUB = "dusty736"

dash.register_page(
    __name__,
    path_template="/teams/<team_abbr>",
    name="Team Detail"
)

def dict_to_table(d, table_type="stats"):
    """Render dict or list-of-dicts as an HTML table.
       table_type = "stats" (default) or "roster"
    """
    if not d:
        return html.Div("No data available")

    class_name = "team-detail-stats-table" if table_type == "stats" else "team-detail-roster-table"

    if isinstance(d, list):
        headers = list(d[0].keys())
        return html.Table(
            [
                html.Thead(
                    html.Tr([html.Th(h) for h in headers])
                ),
                html.Tbody([
                    html.Tr([html.Td(str(row[h])) for h in headers]) for row in d
                ])
            ],
            className=class_name
        )

    return html.Table(
        [
            html.Tbody([
                html.Tr([html.Td(str(k)), html.Td(str(v))])
                for k, v in d.items()
            ])
        ],
        className=class_name
    )

    
def normalize_api_result(result):
  if isinstance(result, dict):
      if "error" in result:   # skip error rows
          return []
      return [result]
  if isinstance(result, list):
      # filter out dicts with "error"
      return [r for r in result if isinstance(r, dict) and "error" not in r]
  return []
    
# ---------------------------------------------------
# Roster Section
# ---------------------------------------------------
# --- Roster Section ---
def roster_section(team_abbr: str):
    current_season, current_week = fetch_current_season_week()
    season_options = [{"label": str(y), "value": y} for y in range(current_season, 1998, -1)]
    position_options = [
        {"label": "All", "value": "ALL"},
        {"label": "TEAM", "value": "TEAM"},
        {"label": "QB", "value": "QB"},
        {"label": "RB", "value": "RB"},
        {"label": "WR", "value": "WR"},
        {"label": "TE", "value": "TE"},
        {"label": "Offensive Line", "value": "OL"},
        {"label": "Defensive Line", "value": "DL"},
        {"label": "Linebackers", "value": "LB"},
        {"label": "Defensive Backs", "value": "DB"},
        {"label": "Special Teams", "value": "ST"},
    ]

    return html.Div(
        [
            html.Div(
                [
                    dcc.Dropdown(
                        id="team-detail-roster-year-dropdown",
                        options=season_options,
                        value=current_season,
                        clearable=False,
                        style={"width": "200px"},
                    ),
                    dcc.Dropdown(
                        id="team-detail-roster-position-dropdown",
                        options=position_options,
                        value="ALL",
                        clearable=False,
                        style={"width": "200px"},
                    ),
                    dcc.Dropdown(
                        id="team-detail-roster-week-dropdown",
                        options=[],  # will be filled dynamically
                        value=current_week,
                        clearable=False,
                        style={"width": "200px"},
                    ),
                ],
                style={"display": "flex", "gap": "10px", "marginBottom": "20px"},
            ),
            html.Div(id="team-detail-roster-tables")
        ]
    )

@callback(
    Output("team-detail-roster-week-dropdown", "options"),
    Output("team-detail-roster-week-dropdown", "value"),
    Input("team-detail-roster-year-dropdown", "value"),
    Input("_pages_location", "pathname"),
)
def update_week_dropdown(selected_year, pathname):
    team_abbr = pathname.split("/")[-1].upper()
    max_week = get_max_week_team(selected_year, team_abbr)
    week_options = [{"label": str(w), "value": w} for w in range(1, max_week + 1)]
    return week_options, max_week
  
# ---------------------------------------------------
# Injuries Section
# ---------------------------------------------------
def injuries_section(team_abbr: str):
    current_season, current_week = fetch_current_season_week()
    season_options = [{"label": str(y), "value": y} for y in range(current_season, 1998, -1)]
    position_options = [
        {"label": "All", "value": "ALL"},
        {"label": "Total", "value": "TOTAL"},
        {"label": "Quarterbacks", "value": "QB"},
        {"label": "Running Backs", "value": "RB"},
        {"label": "Wide Receivers", "value": "WR"},
        {"label": "Tight Ends", "value": "TE"},
        {"label": "Offensive Line", "value": "OL"},
        {"label": "Defensive Line", "value": "DL"},
        {"label": "Linebackers", "value": "LB"},
        {"label": "Defensive Backs", "value": "DB"},
        {"label": "Special Teams", "value": "ST"},
        {"label": "Other", "value": "OTHER"},
    ]

    return html.Div(
        [
            html.Div(
                [
                    dcc.Dropdown(
                        id="team-detail-injuries-year-dropdown",
                        options=season_options,
                        value=current_season,
                        clearable=False,
                        style={"width": "200px"},
                    ),
                    dcc.Dropdown(
                        id="team-detail-injuries-position-dropdown",
                        options=position_options,
                        value="ALL",
                        clearable=False,
                        style={"width": "200px"},
                    ),
                    dcc.Dropdown(
                        id="team-detail-injuries-week-dropdown",
                        options=[],  # filled dynamically
                        value=current_week,
                        clearable=False,
                        style={"width": "200px"},
                    ),
                ],
                style={"display": "flex", "gap": "10px", "marginBottom": "20px"},
            ),
            html.Div(id="team-detail-injuries-tables"),
        ]
    )

@callback(
    Output("team-detail-injuries-week-dropdown", "options"),
    Output("team-detail-injuries-week-dropdown", "value"),
    Input("team-detail-injuries-year-dropdown", "value"),
    Input("_pages_location", "pathname"),
)
def update_injuries_week_dropdown(selected_year, pathname):
    team_abbr = pathname.split("/")[-1].upper()
    max_week = get_max_week_team(selected_year, team_abbr)
    week_options = [{"label": str(w), "value": w} for w in range(1, max_week + 1)]
    return week_options, max_week


# ---------------------------------------------------
# Layout
# ---------------------------------------------------
def layout(team_abbr=None):
    data = get_team_by_abbr(team_abbr.upper()) if team_abbr else None

    if not data:
        main_children = [
            html.H3("Error loading team"),
            html.Pre(f"No data found for {team_abbr}")
        ]
    else:
        # --- Team header card
        team_header = html.Div([
            html.Img(
                src=f"/assets/logos/{data['team_abbr']}.png",
                className="team-detail-logo"
            ),
            html.H2(data["team_name"], className="team-detail-title"),
            html.Div(f"Division: {data['team_division']}", className="team-detail-meta"),
        ], className="team-detail-card")

        # --- Nav buttons
        nav_buttons = html.Div(
            [
                html.Button("Season Statistics", id="team-detail-btn-stats", n_clicks=0, className="btn primary"),
                html.Button("Roster", id="team-detail-btn-roster", n_clicks=0, className="btn"),
                html.Button("Injuries", id="team-detail-btn-injuries", n_clicks=0, className="btn"),
                html.Button("NextGen", id="team-detail-btn-nextgen", n_clicks=0, className="btn"),
            ],
            className="team-detail-nav-buttons",
            style={"display": "flex", "gap": "10px", "marginBottom": "20px"},
        )

        content = html.Div(id="team-detail-content")

        main_children = [
            html.Div([team_header, nav_buttons, content], className="team-detail-wrapper")
        ]

    # --------------------------
    # Page chrome: header/footer
    # --------------------------
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
                                    dcc.Link(html.Button("Teams", className="btn primary"), href="/teams"),
                                    dcc.Link(html.Button("Matchup Central", className="btn"), href="/matchup_central"),
                                    dcc.Link(html.Button("Player Hub", className="btn"), href="/player_hub"),
                                    dcc.Link(html.Button("Time Capsule", className="btn"), href="/time-capsule"),
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

    return html.Div(
        [
            header,
            html.Main(className="home-content fullwidth", children=main_children),
            footer,
        ],
        className="home-page",
    )

# ---------------------------------------------------
# Callbacks
# ---------------------------------------------------
@callback(
    Output("team-detail-content", "children"),
    Input("team-detail-btn-stats", "n_clicks"),
    Input("team-detail-btn-roster", "n_clicks"),
    Input("team-detail-btn-injuries", "n_clicks"),
    Input("team-detail-btn-nextgen", "n_clicks"),
    Input("_pages_location", "pathname")
)
def switch_tab(stats_click, roster_click, injuries_click, nextgen_click, pathname):
    if not pathname or not pathname.startswith("/teams/"):
        return "No team selected"
    team_abbr = pathname.split("/")[-1].upper()

    ctx = dash.callback_context
    if not ctx.triggered:
        # Default tab = Season Statistics
        return stats_section(team_abbr)

    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "team-detail-btn-stats":
        return stats_section(team_abbr)
    elif button_id == "team-detail-btn-roster":
        return roster_section(team_abbr)
    elif button_id == "team-detail-btn-injuries":
        return injuries_section(team_abbr)
    elif button_id == "team-detail-btn-nextgen":
        return html.Div("NextGen goes here")
    return "Invalid selection"


# ---------------------------------------------------
# Season Stats Section
# ---------------------------------------------------
def stats_section(team_abbr: str):
    current_season, _ = fetch_current_season_week()
    options = [{"label": str(y), "value": y} for y in range(current_season, 1998, -1)]

    return html.Div(
        [
            dcc.Dropdown(
                id="team-detail-season-year-dropdown",
                options=options,
                value=current_season,
                clearable=False,
                style={"width": "200px"},
            ),
            html.Div(id="team-detail-season-stats-tables", style={"marginTop": "20px"})
        ]
    )

@callback(
    Output("team-detail-season-stats-tables", "children"),
    Input("team-detail-season-year-dropdown", "value"),
    Input("_pages_location", "pathname")
)
def update_season_stats(selected_year, pathname):
    team_abbr = pathname.split("/")[-1].upper()
    current_season, current_week = fetch_current_season_week()

    if selected_year == current_season:
        week = current_week
    else:
        week = fetch_max_week(selected_year)

    record = get_team_record(team_abbr, selected_year, week)
    offense = get_team_offense(team_abbr, selected_year, week)
    defense = get_team_defense(team_abbr, selected_year, week)
    special = get_team_special(team_abbr, selected_year, week)

    return html.Div(
        [
            html.H4(f"{selected_year} Season (through Week {week})"),
            html.Div([html.H5("Record"), dict_to_table(record, table_type="stats")], className="team-detail-team-stats-card"),
            html.Div([html.H5("Offense"), dict_to_table(offense, table_type="stats")], className="team-detail-team-stats-card"),
            html.Div([html.H5("Defense"), dict_to_table(defense, table_type="stats")], className="team-detail-team-stats-card"),
            html.Div([html.H5("Special Teams"), dict_to_table(special, table_type="stats")], className="team-detail-team-stats-card"),
        ]
    )

    
@callback(
    Output("team-detail-roster-tables", "children"),
    Input("team-detail-roster-year-dropdown", "value"),
    Input("team-detail-roster-position-dropdown", "value"),
    Input("team-detail-roster-week-dropdown", "value"),
    Input("_pages_location", "pathname")
)
def update_roster(selected_year, position, week, pathname):
    team_abbr = pathname.split("/")[-1].upper()

    # Full roster
    roster = get_team_roster(team_abbr, selected_year)

    # Position summary
    if position == "ALL":
        pos_list = ["TEAM", "QB", "RB", "WR", "TE", "OL", "DL", "LB", "DB", "ST"]
        position_summary = []
        for p in pos_list:
            data = get_team_position_summary(team_abbr, selected_year, p)
            position_summary.extend(normalize_api_result(data))
    else:
        position_summary = normalize_api_result(
            get_team_position_summary(team_abbr, selected_year, position)
        )

    # Depth chart (specific week, fallback to max if empty)
    week = week or fetch_max_week(selected_year)
    starters = get_team_depth_chart_starters(team_abbr, selected_year, week)

    return html.Div(
        [
            html.H4(f"{selected_year} Roster"),
            html.Div(
                [html.H5(f"Depth Chart Starters (Week {week})"), dict_to_table(starters, table_type="roster")],
                className="team-detail-team-stats-card",
            ),
            html.Div(
                [html.H5(f"Position Summary ({position})"), dict_to_table(position_summary, table_type="roster")],
                className="team-detail-team-stats-card",
            ),
            html.Div(
                [html.H5("Full Roster"), dict_to_table(roster, table_type="roster")],
                className="team-detail-team-stats-card",
            ),
        ]
    )

@callback(
    Output("team-detail-injuries-tables", "children"),
    Input("team-detail-injuries-year-dropdown", "value"),
    Input("team-detail-injuries-position-dropdown", "value"),
    Input("team-detail-injuries-week-dropdown", "value"),
    Input("_pages_location", "pathname")
)
def update_injuries(selected_year, position, week, pathname):
    team_abbr = pathname.split("/")[-1].upper()
    week = week or fetch_max_week(selected_year)

    if position == "ALL":
        pos_list = ["TOTAL", "QB", "RB", "WR", "TE", "OL", "DL", "LB", "DB", "ST", "OTHER"]
        team_summary = []
        players = []
        for p in pos_list:
            team_summary.extend(
                normalize_api_result(get_team_injury_summary(team_abbr, selected_year, week, p))
            )
            players.extend(
                normalize_api_result(get_player_injuries(team_abbr, selected_year, week, p))
            )
    else:
        team_summary = normalize_api_result(
            get_team_injury_summary(team_abbr, selected_year, week, position)
        )
        players = normalize_api_result(
            get_player_injuries(team_abbr, selected_year, week, position)
        )

    return html.Div(
        [
            html.H4(f"{selected_year} Injuries (Week {week}, {position})"),
            html.Div(
                [html.H5("Team Injury Summary"), dict_to_table(team_summary, table_type="roster")],
                className="team-detail-team-stats-card",
            ),
            html.Div(
                [html.H5("Player Injuries"), dict_to_table(players, table_type="roster")],
                className="team-detail-team-stats-card",
            ),
        ]
    )
