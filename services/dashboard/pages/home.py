import dash
from dash import html, dcc

dash.register_page(__name__, path="/", name="Home")

# Personal details (edit these if needed)
YOUR_NAME = "Dustin Burnham"
YOUR_EMAIL = "dustinburnham@gmail.com"  # change to your real email
YOUR_GITHUB = "dusty736"           # pulled from your earlier setup

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
                                html.H1("NFL Analytics Dashboard (2025)", className="topbar-title"),
                                html.Nav(
                                    className="topbar-actions",
                                    children=[
                                        dcc.Link(html.Button("Season Overview", className="btn primary"), href="/overview"),
                                        # Add more buttons here as you build them
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



