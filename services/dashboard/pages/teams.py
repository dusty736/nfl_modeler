# dashboard/pages/teams.py

import dash
from dash import html, dcc, callback, Input, Output
import pandas as pd
from helpers.api_client import get_all_teams

# Replace these with your real values
YOUR_NAME = "Dustin Burnham"
YOUR_EMAIL = "you@example.com"
YOUR_GITHUB = "dusty736"

dash.register_page(__name__, path="/teams", name="Teams")


def layout():
    teams = get_all_teams()
    if not teams:
        main_children = [
            html.H3("Error loading teams"),
            html.Pre("No data returned from /teams/ API")
        ]
    else:
        df = pd.DataFrame(teams).sort_values(["team_division", "team_name"])
        
        # Group divisions into rows of 2
        division_names = df["team_division"].unique().tolist()
        division_rows = [division_names[i:i+2] for i in range(0, len(division_names), 2)]
        
        grid_rows = []
        
        for row in division_rows:
            row_divs = []
            for division in row:
                group = df[df["team_division"] == division]
                row_divs.append(html.Div([
                    html.H3(division, className="division-title"),
                    html.Div([
                        html.A(
                            href=f"/teams/{r.team_abbr}",
                            className="team-card",
                            children=[
                                html.Div(
                                    className="team-card-inner",
                                    children=[
                                        html.Img(
                                            src=f"/assets/logos/{r.team_abbr}.png",
                                            className="team-logo"
                                        ),
                                        html.Div(r.team_name, className="team-name")
                                    ]
                                )
                            ]
                        ) for _, r in group.iterrows()
                    ], className="division-grid")
                ], className="division-block"))
        
            # Wrap two divisions side by side
            grid_rows.append(html.Div(row_divs, className="division-row"))
        
        main_children = [
            html.H2("Teams by Division", className="page-title"),
            html.Div(grid_rows, className="division-container")
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
                                    dcc.Link(html.Button("Time Capsule", className="btn"), href="/time-capsule")
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
