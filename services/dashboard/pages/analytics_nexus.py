import dash
from dash import html, dcc, callback, Input, Output, State, no_update
import plotly.graph_objects as go

from helpers.api_client import (
    fetch_current_season_week,
    fetch_player_trajectories,   # <- new client helper you added earlier
)

# --- Register page ---
dash.register_page(__name__, path="/analytics_nexus", name="Analytics Nexus")

# --- Fetch season/week context (if needed later) ---
season, week = fetch_current_season_week()

# If current season hasn't started (week < 1), show last season by default.
DEFAULT_SEASON = 2024

# Use current completed week if valid; otherwise a safe display default.
DEFAULT_WEEK_END = 18

# Personal details (footer)
YOUR_NAME = "Dustin Burnham"
YOUR_EMAIL = "dustinburnham@gmail.com"
YOUR_GITHUB = "dusty736"

# --- Simple stat menu for now (can be made dynamic later) ---
STAT_OPTIONS = [
    {"label": "Passing Yards", "value": "passing_yards"},
    {"label": "Passing TDs", "value": "passing_tds"},
    {"label": "Passing EPA", "value": "passing_epa"},
    {"label": "Rushing Yards", "value": "rushing_yards"},
    {"label": "Rushing TDs", "value": "rushing_tds"},
    {"label": "Receiving Yards", "value": "receiving_yards"},
    {"label": "Receiving TDs", "value": "receiving_tds"},
    {"label": "Targets", "value": "targets"},
    {"label": "Receptions", "value": "receptions"},
    {"label": "Fantasy Points PPR", "value": "fantasy_points_ppr"},
]

SEASON_OPTIONS = [{"label": str(y), "value": y} for y in range(2019, 2026)]
POSITION_OPTIONS = [
    {"label": "QB", "value": "QB"},
    {"label": "RB", "value": "RB"},
    {"label": "WR", "value": "WR"},
    {"label": "TE", "value": "TE"},
]
SEASON_TYPE_OPTIONS = [
    {"label": "Regular", "value": "REG"},
    {"label": "Postseason", "value": "POST"},
    {"label": "All (REG+POST)", "value": "ALL"},
]
RANK_BY_OPTIONS = [
    {"label": "Sum", "value": "sum"},
    {"label": "Mean", "value": "mean"},
]

SERIES_MODE_OPTIONS = [
    {"label": "Weekly (Per-Game)", "value": "base"},
    {"label": "Season-to-Date (Cumulative)", "value": "cumulative"},
]

MIN_GAMES_OPTIONS = [
    {"label": "No Floor (0)", "value": 0},
    {"label": "≥ 4 games", "value": 4},
    {"label": "≥ 6 games", "value": 6},
    {"label": "≥ 8 games", "value": 8},
]

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
                                # ============================
                                # Analytics Nexus — Player Weekly Trajectories (ax-pt-*)
                                # ============================
                                html.Section(
                                    id="ax-pt-section",
                                    className="ax-pt-section",
                                    children=[
                                        html.H2("Players — Weekly Trajectories", className="ax-pt-title"),

                                        # Controls
                                        html.Div(
                                            className="ax-pt-controls",
                                            children=[
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Season"),
                                                        dcc.Dropdown(
                                                            id="ctl-season",
                                                            options=SEASON_OPTIONS,
                                                            value=DEFAULT_SEASON,
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Season Type"),
                                                        dcc.RadioItems(
                                                            id="ctl-season-type",
                                                            options=SEASON_TYPE_OPTIONS,
                                                            value="REG",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Stat"),
                                                        dcc.Dropdown(
                                                            id="ctl-stat",
                                                            options=STAT_OPTIONS,
                                                            value="passing_yards",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Position"),
                                                        dcc.RadioItems(
                                                            id="ctl-position",
                                                            options=POSITION_OPTIONS,
                                                            value="QB",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Top N"),
                                                        dcc.Input(
                                                            id="ctl-topn",
                                                            type="number",
                                                            min=1,
                                                            max=20,
                                                            step=1,
                                                            value=8,
                                                            className="ax-pt-topn",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Rank By"),
                                                        dcc.RadioItems(
                                                            id="ctl-rankby",
                                                            options=RANK_BY_OPTIONS,
                                                            value="sum",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group ax-pt-span-2",
                                                    children=[
                                                        html.Label("Week Range"),
                                                        dcc.RangeSlider(
                                                            id="ctl-week-range",
                                                            min=1,
                                                            max=22,
                                                            value=[1, DEFAULT_WEEK_END],
                                                            allowCross=False,
                                                            pushable=0,
                                                            marks={i: str(i) for i in range(1, 23)},
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                  className="ax-pt-group",
                                                  children=[
                                                      html.Label("Series View"),
                                                      dcc.RadioItems(
                                                          id="ctl-series-mode",
                                                          options=SERIES_MODE_OPTIONS,
                                                          value="base",
                                                          inline=True,
                                                          inputClassName="ax-pt-radio-input",
                                                          labelClassName="ax-pt-radio-label",
                                                      ),
                                                  ],
                                              ),
                                              html.Div(
                                                  className="ax-pt-group",
                                                  children=[
                                                      html.Label("Eligibility Floor"),
                                                      dcc.RadioItems(
                                                          id="ctl-min-games",
                                                          options=MIN_GAMES_OPTIONS,
                                                          value=0,
                                                          inline=True,
                                                          inputClassName="ax-pt-radio-input",
                                                          labelClassName="ax-pt-radio-label",
                                                      ),
                                                  ],
                                              ),
                                            ],
                                        ),

                                        # Store + Graph
                                        dcc.Store(id="store-player-trajectories"),
                                        dcc.Loading(
                                            type="default",
                                            children=dcc.Graph(
                                              id="ax-pt-graph",
                                              className="ax-pt-graph",
                                              figure=go.Figure(),
                                              style={"height": "650px", "width": "100%"},   # ← match CSS height
                                              config={"displayModeBar": False, "responsive": True},
                                          ),
                                        ),
                                    ],
                                ),
                                # ============================
                                # /Analytics Nexus — Player Weekly Trajectories
                                # ============================
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

# ============================
# Callbacks — Analytics Nexus: Player Weekly Trajectories
# ============================

@callback(
    Output("store-player-trajectories", "data"),
    Input("selected-plot", "data"),
    Input("ctl-season", "value"),
    Input("ctl-season-type", "value"),
    Input("ctl-stat", "value"),
    Input("ctl-position", "value"),
    Input("ctl-topn", "value"),
    Input("ctl-week-range", "value"),
    Input("ctl-rankby", "value"),
    Input("ctl-series-mode", "value"),   # ← NEW
    Input("ctl-min-games", "value"),     # ← NEW
    prevent_initial_call=False,
)
def fetch_ax_pt_data(selected_plot, season_val, season_type, stat_name, position,
                     topn, week_range, rankby, series_mode, min_games):
    if selected_plot != "nav-player-trajectories":
        return no_update

    if not all([season_val, season_type, stat_name, position, topn, week_range, rankby, series_mode]) \
       or min_games is None:
        return []

    week_start, week_end = int(week_range[0]), int(week_range[1])
    if week_end < week_start or int(topn) < 1:
        return []

    rows = fetch_player_trajectories(
        season=int(season_val),
        season_type=str(season_type),
        stat_name=str(stat_name),
        position=str(position),
        top_n=int(topn),
        week_start=week_start,
        week_end=week_end,
        rank_by=str(rankby),
        stat_type=str(series_mode),       # ← pass series mode to API
        min_games=int(min_games),         # ← pass floor to API
        timeout=3,
    )
    return rows or []

@callback(
    Output("ax-pt-graph", "figure"),
    Input("store-player-trajectories", "data"),
    State("ctl-stat", "value"),
    State("ctl-position", "value"),
    State("ctl-season", "value"),
    State("ctl-season-type", "value"),
    State("ctl-rankby", "value"),
    State("ctl-series-mode", "value"),   # ← NEW
    State("ctl-min-games", "value"),     # ← NEW
)
def render_ax_pt_figure(rows, stat_name, position, season_val, season_type, rankby, series_mode, min_games):
    fig = go.Figure()

    stat_label = next((o["label"] for o in STAT_OPTIONS if o["value"] == stat_name), stat_name)
    series_label = "Weekly" if (series_mode or "base") == "base" else "Season-to-Date"
    floor_label = f" • floor≥{min_games} gp" if (min_games or 0) > 0 else ""

    # Empty-state (light theme)
    if not rows:
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="white",
            plot_bgcolor="white",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[
                dict(
                    text=f"No data for {position} • {stat_label} • {season_val} {season_type}<br>"
                         f"{series_label} • Ranke By={rankby}{floor_label}",
                    x=0.5, y=0.5, xref="paper", yref="paper",
                    showarrow=False, font=dict(size=16, color="#444"),
                )
            ],
            margin=dict(l=40, r=20, t=80, b=40),
            autosize=True,
        )
        return fig

        # Group rows by player_id and preserve rank order
    by_player, rank_map = {}, {}
    for r in rows:
        pid = r["player_id"]
        by_player.setdefault(pid, []).append(r)
        # keep the smallest (best) rank we’ve seen for this player
        rank_map[pid] = min(rank_map.get(pid, 10**9), r.get("player_rank", 10**9))

    ordered_pids = [pid for pid, _ in sorted(rank_map.items(), key=lambda kv: kv[1])]

    # Build a line for each player
    for pid in ordered_pids:
        pts = sorted(by_player[pid], key=lambda x: x["week"])
        if not pts:
            continue
        name = pts[0]["name"]
        team = pts[0]["team"]
        color = pts[0].get("team_color") or "#888"
        fill  = pts[0].get("team_color2") or "#AAA"

        weeks  = [p["week"] for p in pts]
        values = [p["value"] for p in pts]  # keep None as gaps

        fig.add_trace(
            go.Scatter(
                x=weeks,
                y=values,
                mode="lines+markers",
                name=f"{name} ({team})",
                line=dict(width=2, color=color),
                marker=dict(size=6, symbol="circle", line=dict(width=1, color="black"), color=fill),
                connectgaps=False,
                hovertemplate="<b>%{fullData.name}</b><br>Week %{x}<br>Value: %{y}<extra></extra>",
            )
        )

    title = f"Top Trajectories • {position} • {stat_label}"
    subtitle = f"{series_label} • Season {season_val} • {season_type} • rank_by={rankby}{floor_label}"

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title=dict(
            text=f"{title}<br><span style='font-size:0.8em;color:#444'>{subtitle}</span>",
            x=0.02, y=0.98, xanchor="left", yanchor="top",
        ),
        xaxis=dict(
            title="Week",
            dtick=1, tick0=1,
            range=[min(r["week"] for r in rows) - 0.5, max(r["week"] for r in rows) + 0.5],
            gridcolor="rgba(0,0,0,0.08)", zeroline=False,
        ),
        yaxis=dict(
            title=stat_label,
            gridcolor="rgba(0,0,0,0.08)", zeroline=False,
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0),
        margin=dict(l=60, r=20, t=120, b=56),
        autosize=True,
    )
    return fig

# Optional: set 'selected-plot' when the sidebar button is clicked (keeps styles consistent)
@callback(
    Output("selected-plot", "data"),
    Input("nav-player-trajectories", "n_clicks"),
    prevent_initial_call=True,
)
def set_selected_to_pt(n1):
    if n1:
        return "nav-player-trajectories"
    return no_update


