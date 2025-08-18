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
    fetch_max_week
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

def dict_to_table(d):
    """Render dict or list-of-dicts as an HTML table."""
    if not d:
        return html.Div("No data available")

    # Case 1: list of dicts (rows)
    if isinstance(d, list):
        # use keys from the first row for headers
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
            className="team-detail-stats-table"
        )

    # Case 2: single dict
    return html.Table(
        [
            html.Tbody([
                html.Tr([html.Td(str(k)), html.Td(str(v))])
                for k, v in d.items()
            ])
        ],
        className="team-detail-stats-table"
    )

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
        return "Select a section"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "team-detail-btn-stats":
        return stats_section(team_abbr)
    elif button_id == "team-detail-btn-roster":
        return html.Div("Roster goes here")
    elif button_id == "team-detail-btn-injuries":
        return html.Div("Injuries go here")
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
            html.Div([html.H5("Record"), dict_to_table(record)], className="team-detail-team-stats-card"),
            html.Div([html.H5("Offense"), dict_to_table(offense)], className="team-detail-team-stats-card"),
            html.Div([html.H5("Defense"), dict_to_table(defense)], className="team-detail-team-stats-card"),
            html.Div([html.H5("Special Teams"), dict_to_table(special)], className="team-detail-team-stats-card"),
        ]
    )
