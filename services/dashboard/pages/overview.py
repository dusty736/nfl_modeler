# dashboard/pages/overview.py

import dash
from dash import html, dcc, callback, Input, Output
import pandas as pd

from helpers.standings import fetch_standings, division_table

dash.register_page(__name__, path="/overview", name="Season Overview")

# Personal details (duplicate of home.py so pages are self-contained)
YOUR_NAME = "Dustin Burnham"
YOUR_EMAIL = "dustinburnham@gmail.com"
YOUR_GITHUB = "dusty736"


# --------------------------
# Helpers to build sections
# --------------------------
def _division_view(df: pd.DataFrame) -> html.Div:
    """AFC (left) and NFC (right) columns, each a 2×2 grid of division tables."""
    afc = df[df["division"].astype(str).str.startswith("AFC")].reset_index(drop=True)
    nfc = df[df["division"].astype(str).str.startswith("NFC")].reset_index(drop=True)
    by = lambda base, name: base[base["division"] == name].reset_index(drop=True)

    def grid(children):
        # Wrap each division table in a card so CSS can size them predictably
        return html.Div(
            [html.Div(division_table(*args), className="division-card") for args in children],
            className="division-grid"
        )

    afc_grid = grid([
        (by(afc, "AFC East"),  "AFC East"),
        (by(afc, "AFC North"), "AFC North"),
        (by(afc, "AFC South"), "AFC South"),
        (by(afc, "AFC West"),  "AFC West"),
    ])
    nfc_grid = grid([
        (by(nfc, "NFC East"),  "NFC East"),
        (by(nfc, "NFC North"), "NFC North"),
        (by(nfc, "NFC South"), "NFC South"),
        (by(nfc, "NFC West"),  "NFC West"),
    ])

    return html.Div(
        [
            html.Div(
                [html.H4("AFC"), afc_grid],
                className="conference-column"
            ),
            html.Div(
                [html.H4("NFC"), nfc_grid],
                className="conference-column"
            ),
        ],
        className="conference-columns standings-scope",
        style={"width": "100%", "maxWidth": "none", "padding": "0 8px"}
    )

from dash import dash_table

from dash import dash_table

def _render_conf_table(title: str, frame: pd.DataFrame) -> html.Div:
    cols = [
        ("team_id", "Team"),
        ("wins", "W"),
        ("losses", "L"),
        ("ties", "T"),
        ("points_for", "PF"),
        ("points_against", "PA"),
        ("point_diff", "PD"),
    ]
    cols = [(c, lab) for c, lab in cols if c in frame.columns]

    # Data for DataTable
    data_records = frame.to_dict("records")

    # Build conditional styling for every row so all cells get team colors
    style_data_conditional = []
    for i, row in frame.reset_index(drop=True).iterrows():
        style_data_conditional.append({
            "if": {"row_index": i},
            "backgroundColor": str(row.get("team_color", "#333")),
            "color": str(row.get("team_color2", "#fff")),
        })

    return html.Div(
        children=[
            html.H4(title),
            dash_table.DataTable(
                columns=[{"name": lab, "id": col} for col, lab in cols],
                data=data_records,
                sort_action="native",
                style_table={"width": "100%"},
                style_cell={
                    "padding": "8px",
                    "textAlign": "center",
                    "fontWeight": "bold",
                    "border": "1px solid #FFFFFF",  # white outline for each cell
                },
                style_header={
                    "backgroundColor": "var(--platinum)",
                    "color": "var(--ink)",
                    "fontWeight": "bold"
                },
                style_data_conditional=style_data_conditional
            ),
        ],
        className="standings-card",
        style={"flex": "1", "minWidth": "360px"},
    )


def _conference_view(df: pd.DataFrame) -> html.Div:
    """Return two conference-wide tables (AFC/NFC), sorted best record first, no pagination."""
    def sort_for_standings(frame: pd.DataFrame) -> pd.DataFrame:
        # wins desc, losses asc, ties desc, point_diff desc
        frame = frame.copy()
        for c in ["wins", "losses", "ties", "point_diff"]:
            if c in frame.columns:
                frame[c] = frame[c].fillna(0)
        return frame.sort_values(
            by=[c for c in ["wins", "losses", "ties", "point_diff"] if c in frame.columns],
            ascending=[False, True, False, False][:sum(c in frame.columns for c in ["wins","losses","ties","point_diff"])],
            kind="mergesort",
        ).reset_index(drop=True)

    afc_conf = sort_for_standings(df[df["division"].astype(str).str.startswith("AFC")])
    nfc_conf = sort_for_standings(df[df["division"].astype(str).str.startswith("NFC")])

    return html.Div(
      [
          _render_conf_table("AFC (Conference)", afc_conf),
          _render_conf_table("NFC (Conference)", nfc_conf),
      ],
      className="standings-scope conference-standings",
      style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "width": "90%", "margin": "0 auto"},
  )



def layout():
    # --------------------------
    # Data: division standings
    # --------------------------
    df, err = fetch_standings()

    if err or df.empty:
        # Error path: keep simple, no controls
        standings_section = html.Div(
            [
                html.H3("2025 Season — Division Standings"),
                html.Div("Could not load standings from API.", style={"color": "#b00"}),
                html.Pre(
                    str(err or "No details"),
                    style={"whiteSpace": "pre-wrap", "fontSize": "0.9rem", "color": "#666"},
                ),
            ],
        )
        main_children = [standings_section]
    else:
        # Validate columns
        expected_cols = {
            "team_id", "division", "wins", "losses", "ties",
            "points_for", "points_against", "point_diff",
            "team_color", "team_color2",
        }
        missing = expected_cols.difference(df.columns)
        if missing:
            standings_section = html.Div(
                [
                    html.H3("2025 Season — Division Standings"),
                    html.Div(
                        f"Missing expected columns from API response: {sorted(missing)}",
                        style={"color": "#b00"},
                    ),
                ],
            )
            main_children = [standings_section]
        else:
            # Controls + content container; pre-render division view for initial paint
            controls = html.Div(
                [
                    html.H3("2025 Season — Division Standings"),
                    dcc.RadioItems(
                        id="standings-view",
                        options=[
                            {"label": "Division", "value": "division"},
                            {"label": "Conference", "value": "conference"},
                        ],
                        value="division",
                        inline=True,
                        style={"margin": "0.25rem 0 0.75rem 0"}
                    )
                ]
            )

            content_initial = _division_view(df)
            main_children = [
                controls,
                dcc.Store(id="standings-data", data=df.to_dict("records")),
                html.Div(id="standings-content", children=content_initial),
                html.Div("Data source: /api/standings", style={"marginTop": "10px", "color": "#888"}),
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
                                    dcc.Link(html.Button("Home", className="btn primary"), href="/"),
                                    dcc.Link(html.Button("Standings", className="btn"), href="/overview"),
                                    dcc.Link(html.Button("Teams", className="btn"), href="/teams"),
                                    dcc.Link(html.Button("Game Center", className="btn"), href="/games"),
                                    dcc.Link(html.Button("Analytics Nexus", className="btn"), href="/analytics_nexus")
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
        html.Main(className="home-content fullwidth", children=main_children),  # add .fullwidth here
        footer,
    ],
    className="home-page",
)


# --------------------------
# Callbacks
# --------------------------
@callback(
    Output("standings-content", "children"),
    Input("standings-view", "value"),
    Input("standings-data", "data"),
)
def _update_standings(view, data):
    df = pd.DataFrame(data or [])
    if df.empty or "division" not in df.columns:
        return html.Div("Standings unavailable.", style={"color": "#b00"})

    if view == "conference":
        return _conference_view(df)
    # default
    return _division_view(df)

