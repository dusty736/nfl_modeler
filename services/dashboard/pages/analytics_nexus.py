import dash
from dash import html, dcc

from helpers.api_client import fetch_current_season_week

# --- Register page ---
dash.register_page(__name__, path="/analytics_nexus", name="Analytics Nexus")

# --- Fetch season/week context (if needed later) ---
season, week = fetch_current_season_week()

# Personal details (footer)
YOUR_NAME = "Dustin Burnham"
YOUR_EMAIL = "dustinburnham@gmail.com"
YOUR_GITHUB = "dusty736"

# --- Layout ---
layout = html.Div(
    className="analytics-page",
    children=[
        # --- Top Bar ---
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
                        # Center: title + nav buttons
                        html.Div(
                            className="topbar-center",
                            children=[
                                html.H1("NFL Analytics Dashboard ", className="topbar-title"),
                                html.Nav(
                                    className="topbar-actions",
                                    children=[
                                        dcc.Link(html.Button("Home", className="btn"), href="/"),
                                        dcc.Link(html.Button("Standings", className="btn"), href="/overview"),
                                        dcc.Link(html.Button("Teams", className="btn"), href="/teams"),
                                        dcc.Link(html.Button("Analytics Nexus", className="btn primary"), href="/analytics_nexus"),
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

        # --- Main Content: Sidebar + Plot Area ---
        html.Div(
            className="analytics-layout",
            children=[
                # Sidebar
                html.Nav(
                    className="analytics-sidebar",
                    children=[
                        html.H3("Players"),
                        html.Ul(
                            [
                                html.Li(
                                    html.Button("Weekly Trajectories", id="nav-player-trajectories", n_clicks=0, className="nav-btn active")
                                ),
                                html.Li(
                                    html.Button("Consistency / Volatility Violin", id="nav-player-violin", n_clicks=0, className="nav-btn")
                                ),
                                html.Li(
                                    html.Button("Quadrant Scatter", id="nav-player-scatter", n_clicks=0, className="nav-btn")
                                ),
                                html.Li(
                                    html.Button("Rolling Percentiles", id="nav-player-percentiles", n_clicks=0, className="nav-btn")
                                ),
                            ]
                        ),
                        html.H3("Teams"),
                        html.Ul(
                            [
                                html.Li(
                                    html.Button("Time Series", id="nav-team-timeseries", n_clicks=0, className="nav-btn")
                                ),
                                html.Li(
                                    html.Button("Violin Distributions", id="nav-team-violin", n_clicks=0, className="nav-btn")
                                ),
                                html.Li(
                                    html.Button("Quadrant Scatter", id="nav-team-scatter", n_clicks=0, className="nav-btn")
                                ),
                                html.Li(
                                    html.Button("Rolling Percentiles", id="nav-team-percentiles", n_clicks=0, className="nav-btn")
                                ),
                            ]
                        ),
                    ],
                ),

                # Main panel
                html.Main(
                    className="analytics-content",
                    children=[
                        html.Div(
                            id="analytics-main-panel",
                            children=[
                                html.H2("Players — Weekly Trajectories (default)"),
                                html.P("Default plot will render here when callbacks are added."),
                            ],
                        )
                    ],
                ),
            ],
        ),

        # Hidden store for selected nav item
        dcc.Store(id="selected-plot", data="nav-player-trajectories"),

        # --- Footer ---
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
                                                html.A(
                                                    YOUR_EMAIL,
                                                    href=f"mailto:{YOUR_EMAIL}",
                                                    className="footer-link",
                                                ),
                                            ]
                                        ),
                                        html.Li(
                                            [
                                                html.Strong("GitHub: "),
                                                html.A(
                                                    f"@{YOUR_GITHUB}",
                                                    href=f"https://github.com/{YOUR_GITHUB}",
                                                    target="_blank",
                                                    rel="noopener noreferrer",
                                                    className="footer-link",
                                                ),
                                            ]
                                        ),
                                    ],
                                    className="footer-list",
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
                                            className="footer-tech-logo",
                                        ),
                                        html.Img(
                                            src=dash.get_asset_url("logos/python_logo.png"),
                                            alt="Python logo",
                                            className="footer-tech-logo",
                                        ),
                                    ],
                                ),
                            ],
                        ),

                        # Right: credits
                        html.Div(
                            className="footer-col footer-credits",
                            children=[
                                html.H4("Credits"),
                                html.Div(
                                    className="footer-small",
                                    children=[
                                        html.Div("This project is non-commercial and purely educational."),
                                        html.Div(
                                            [
                                                "Special thanks to ",
                                                html.Span("ChatGPT", className="footer-mention"),
                                                " for assistance.",
                                            ]
                                        ),
                                        html.Div(
                                            [
                                                "Data & tools include the ",
                                                html.Span("nflfastR", className="footer-mention"),
                                                " R package.",
                                            ]
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                )
            ],
        ),
    ],
)

