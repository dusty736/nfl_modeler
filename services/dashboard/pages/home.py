import dash
from dash import html, dcc

from helpers.api_client import fetch_current_season_week
from helpers.api_client import fetch_primetime_games

from datetime import datetime
import pytz

dash.register_page(__name__, path="/", name="Home")

# Fetch current season and week for the heading
season, week = fetch_current_season_week()
primetime_games = fetch_primetime_games()

if season is not None and week is not None:
    primetime_heading = f"PRIME TIME FOOTBALL — Week {week}, {season}"
else:
    primetime_heading = "PRIME TIME FOOTBALL"
    
london = pytz.timezone("Europe/London")
eastern = pytz.timezone("US/Eastern")

# Personal details (edit these if needed)
YOUR_NAME = "Dustin Burnham"
YOUR_EMAIL = "dustinburnham@gmail.com"  # change to your real email
YOUR_GITHUB = "dusty736"           # pulled from your earlier setup

# Helper to build a matchup row
def render_matchup_row(game):
    # Parse kickoff datetime
    try:
        # STRIP the timezone part and treat it as naive (wrong offset)
        dt_str = game["kickoff"][:19]  # Only the "YYYY-MM-DD HH:MM:SS"
        naive_dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
        
        # Treat it as if it were Eastern time
        kickoff_et = eastern.localize(naive_dt)

        kickoff_str = kickoff_et.strftime("%a, %b %-d — %-I:%M %p ET")
    except Exception as e:
        kickoff_str = "TBD"


    return html.Div(
        className="primetime-row",
        children=[
            html.Div(
                className="team-col home-team",
                children=[
                    html.Img(
                        src=dash.get_asset_url(f"logos/{game['home_team']}.png"),
                        alt=f"{game['home_team']} logo",
                        className="team-logo"
                    ),
                    html.Span(game["home_team"], className="team-abbr")
                ]
            ),
            html.Div(
                className="ou-col",
                children=[
                    html.Div(kickoff_str, className="kickoff-text"),
                    html.Span(f"O/U {game['spread_line']:.1f}", className="over-under")
                ]
            ),
            html.Div(
                className="team-col away-team",
                children=[
                    html.Span(game["away_team"], className="team-abbr"),
                    html.Img(
                        src=dash.get_asset_url(f"logos/{game['away_team']}.png"),
                        alt=f"{game['away_team']} logo",
                        className="team-logo"
                    )
                ]
            )
        ]
    )


layout = html.Div(
    [
        # --- Top Bar (you already have this) ---
        html.Header(
            className="topbar",
            children=[
                html.Div(
                    className="topbar-inner",
                    children=[
                        # Left: logo
                        html.Div(
                            className="topbar-left",
                            children=[
                                dcc.Link(
                                    html.Img(
                                        src=dash.get_asset_url("logos/dashboard_emblem.png"),
                                        alt="Dashboard emblem",
                                        className="topbar-logo"
                                    ),
                                    href="/",
                                    className="logo-link"
                                ),
                            ],
                        ),
                        # Center: title + buttons
                        html.Div(
                            className="topbar-center",
                            children=[
                                html.H1("NFL Analytics Dashboard ", className="topbar-title"),
                                html.Nav(
                                    className="topbar-actions",
                                    children=[
                                        dcc.Link(html.Button("Home", className="btn primary"), href="/"),
                                        dcc.Link(html.Button("Standings", className="btn"), href="/overview"),
                                        dcc.Link(html.Button("Teams", className="btn"), href="/teams"),
                                        dcc.Link(html.Button("Matchup Central", className="btn"), href="/matchup_central"),
                                        dcc.Link(html.Button("Player Hub", className="btn"), href="/player_hub"),
                                        dcc.Link(html.Button("Time Capsule", className="btn"), href="/time-capsule")
                                    ],
                                ),
                            ],
                        ),
                        # Right spacer
                        html.Div(className="topbar-right"),
                    ],
                )
            ],
        ),

        # --- Main content (placeholder) ---
        html.Main(className="home-content", children=[]),
        
        # --- Project Overview Box ---
        html.Section(
        className="home-section",
        children=[
            html.Div(
                className="section-block",
                children=[
                    html.H2("Welcome to the NFL Analytics Dashboard "),
                    html.P("This dashboard is built for fans, researchers, and data scientists to explore the 2025 NFL season through advanced metrics, interactive visualizations, and predictive modeling."),
                    html.H3("Project Goals"),
                    html.Ul(
                        [
                            html.Li("Visualize season standings, player stats, and team matchups"),
                            html.Li("Predict outcomes using machine learning models"),
                            html.Li(" Provide historical and real-time insights for every NFL game"),
                            html.Li(" Build a full-stack project using Python, R, PostgreSQL, and Dash"),
                        ],
                        className="home-goals-list"
                    ),
    
                    html.H2(primetime_heading, style={"marginTop": "48px"}),
                    html.Div(
                        className="primetime-grid",
                        children=[render_matchup_row(g) for g in primetime_games]
                    )
                ]
            )
        ]
    ),


        # --- Bottom Bar (footer) ---
        html.Footer(
            className="bottombar",
            children=[
                html.Div(
                    className="bottombar-inner",
                    children=[
                        # Left: your details
                        html.Div(
                            className="footer-col footer-me",
                            children=[
                                html.H4("About"),
                                html.Ul(
                                    [
                                        html.Li([html.Strong("Name: "), YOUR_NAME]),
                                        html.Li(
                                            [
                                                html.Strong("Email: "),
                                                html.A(YOUR_EMAIL, href=f"mailto:{YOUR_EMAIL}", className="footer-link"),
                                            ]
                                        ),
                                        html.Li(
                                            [
                                                html.Strong("GitHub: "),
                                                html.A(f"@{YOUR_GITHUB}",
                                                       href=f"https://github.com/{YOUR_GITHUB}",
                                                       target="_blank",
                                                       rel="noopener noreferrer",
                                                       className="footer-link"),
                                            ]
                                        ),
                                    ],
                                    className="footer-list"
                                ),
                            ],
                        ),

                        # Middle: tech logos
                        html.Div(
                            className="footer-col footer-logos",
                            children=[
                                html.Div("Built with", className="footer-kicker"),
                                html.Div(
                                    className="logo-row",
                                    children=[
                                        html.Img(
                                            src=dash.get_asset_url("logos/R_logo.png"),
                                            alt="R logo",
                                            className="footer-tech-logo"
                                        ),
                                        html.Img(
                                            src=dash.get_asset_url("logos/python_logo.png"),
                                            alt="Python logo",
                                            className="footer-tech-logo"
                                        ),
                                    ],
                                ),
                            ],
                        ),

                        # Right: credits & disclaimer
                        html.Div(
                            className="footer-col footer-credits",
                            children=[
                                html.H4("Credits"),
                                html.Div(
                                    className="footer-small",
                                    children=[
                                        html.Div("This project is non‑commercial and purely educational."),
                                        html.Div([
                                            "Special thanks to ",
                                            html.Span("ChatGPT", className="footer-mention"),
                                            " for assistance."
                                        ]),
                                        html.Div([
                                            "Data & tools include the ",
                                            html.Span("nflfastR", className="footer-mention"),
                                            " R package."
                                        ]),
                                    ],
                                ),
                            ],
                        ),
                    ],
                )
            ],
        ),
    ],
    className="home-page"
)



