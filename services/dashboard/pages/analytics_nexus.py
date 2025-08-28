import dash
from dash import html, dcc, callback, Input, Output, State, no_update, ctx
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from helpers.api_client import (
    fetch_current_season_week,
    fetch_player_trajectories,   # <- new client helper you added earlier
    fetch_player_violins,
    fetch_player_scatter,
    fetch_player_rolling_percentiles
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

METRIC_OPTIONS = [
    # Derived rates
    {"label": "EPA per Dropback",          "value": "passing_epa_per_dropback"},
    {"label": "ANY/A",                      "value": "passing_anya"},
    {"label": "EPA per Rush",               "value": "rushing_epa_per_carry"},
    {"label": "EPA per Target",             "value": "receiving_epa_per_target"},
    {"label": "Total EPA per Opportunity",  "value": "total_epa_per_opportunity"},
    {"label": "Yards per Opportunity",      "value": "yards_per_opportunity"},
    # A few raw examples (works across positions)
    {"label": "Passing Yards", "value": "passing_yards"},
    {"label": "Passing TDs", "value": "passing_tds"},
    {"label": "Passing EPA", "value": "passing_epa"},
    {"label": "Rushing Yards", "value": "rushing_yards"},
    {"label": "Rushing TDs", "value": "rushing_tds"},
    {"label": "Receiving Yards", "value": "receiving_yards"},
    {"label": "Receiving TDs", "value": "receiving_tds"},
    {"label": "Attempts", "value": "attempts"},
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
                                # ============================
                                # Analytics Nexus — Player Consistency / Volatility Violin (ax-pv-*)
                                # ============================
                                html.Section(
                                    id="ax-pv-section",
                                    className="ax-pv-section",
                                    children=[
                                        html.H2("Players — Consistency / Volatility (Violin)", className="ax-pv-title"),
                                
                                        # Controls
                                        html.Div(
                                            className="ax-pv-controls",
                                            children=[
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Seasons (multi)"),
                                                        dcc.Dropdown(
                                                            id="ctl-pv-seasons",
                                                            options=SEASON_OPTIONS,          # reuse 2019..2025
                                                            value=[DEFAULT_SEASON],          # default: current season
                                                            multi=True,
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Season Type"),
                                                        dcc.RadioItems(
                                                            id="ctl-pv-season-type",
                                                            options=SEASON_TYPE_OPTIONS,
                                                            value="REG",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Stat"),
                                                        dcc.Dropdown(
                                                            id="ctl-pv-stat",
                                                            options=STAT_OPTIONS,
                                                            value="passing_yards",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Position"),
                                                        dcc.RadioItems(
                                                            id="ctl-pv-position",
                                                            options=POSITION_OPTIONS,
                                                            value="QB",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Top N"),
                                                        dcc.Input(
                                                            id="ctl-pv-topn",
                                                            type="number",
                                                            min=1, max=20, step=1,
                                                            value=8,
                                                            className="ax-pt-topn",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Order By"),
                                                        dcc.RadioItems(
                                                            id="ctl-pv-order-by",
                                                            options=[
                                                                {"label": "rCV (MAD/median)", "value": "rCV"},
                                                                {"label": "IQR", "value": "IQR"},
                                                                {"label": "Median (desc)", "value": "median"},
                                                            ],
                                                            value="rCV",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group ax-pv-span-2",
                                                    children=[
                                                        html.Label("Week Range"),
                                                        dcc.RangeSlider(
                                                            id="ctl-pv-week-range",
                                                            min=1, max=22,
                                                            value=[1, DEFAULT_WEEK_END],
                                                            allowCross=False, pushable=0,
                                                            marks={i: str(i) for i in range(1, 23)},
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Series"),
                                                        dcc.RadioItems(
                                                            id="ctl-pv-series",
                                                            options=SERIES_MODE_OPTIONS,   # base | cumulative
                                                            value="base",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Badges Min Games"),
                                                        dcc.Input(
                                                            id="ctl-pv-min-badges",
                                                            type="number",
                                                            min=0, step=1, value=6,
                                                            className="ax-pt-topn",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        dcc.Checklist(
                                                            id="ctl-pv-show-points",
                                                            options=[{"label": "Show weekly points", "value": "show"}],
                                                            value=["show"],
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        ),
                                
                                        # Store + Graph
                                        dcc.Store(id="store-player-violins"),
                                        dcc.Loading(
                                            type="default",
                                            children=dcc.Graph(
                                                id="ax-pv-graph",
                                                className="ax-pv-graph",
                                                figure=go.Figure(),
                                                style={"height": "650px", "width": "100%"},
                                                config={"displayModeBar": False, "responsive": True},
                                            ),
                                        ),
                                    ],
                                ),
                                # ============================
                                # /Analytics Nexus — Player Consistency / Volatility Violin
                                # ============================
                                # ============================
                                # Analytics Nexus — Player Quadrant Scatter (ax-ps-*)
                                # ============================
                                html.Section(
                                    id="ax-ps-section",
                                    className="ax-ps-section",
                                    children=[
                                        html.H2("Players — Quadrant Scatter", className="ax-ps-title"),
                                
                                        # Controls (reuse the light card look)
                                        html.Div(
                                            className="ax-pt-controls ax-ps-controls",
                                            children=[
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Seasons (multi)"),
                                                        dcc.Dropdown(
                                                            id="ctl-ps-seasons",
                                                            options=SEASON_OPTIONS,
                                                            value=[DEFAULT_SEASON],
                                                            multi=True,
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Season Type"),
                                                        dcc.RadioItems(
                                                            id="ctl-ps-season-type",
                                                            options=SEASON_TYPE_OPTIONS,
                                                            value="REG",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Position"),
                                                        dcc.RadioItems(
                                                            id="ctl-ps-position",
                                                            options=POSITION_OPTIONS,
                                                            value="QB",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Top N"),
                                                        dcc.Input(
                                                            id="ctl-ps-topn",
                                                            type="number",
                                                            min=1, max=50, step=1,
                                                            value=10,
                                                            className="ax-pt-topn",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Metric X"),
                                                        dcc.Dropdown(
                                                            id="ctl-ps-metric-x",
                                                            options=METRIC_OPTIONS,
                                                            value="attempts",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Metric Y"),
                                                        dcc.Dropdown(
                                                            id="ctl-ps-metric-y",
                                                            options=METRIC_OPTIONS,
                                                            value="passing_epa",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group ax-pv-span-2",
                                                    children=[
                                                        html.Label("Week Range"),
                                                        dcc.RangeSlider(
                                                            id="ctl-ps-week-range",
                                                            min=1, max=22,
                                                            value=[1, DEFAULT_WEEK_END],
                                                            allowCross=False, pushable=0,
                                                            marks={i: str(i) for i in range(1, 23)},
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        html.Label("Select Top By"),
                                                        dcc.RadioItems(
                                                            id="ctl-ps-top-by",
                                                            options=[
                                                                {"label": "Combined Gate (x+y)", "value": "combined"},
                                                                {"label": "X Gate",               "value": "x_gate"},
                                                                {"label": "Y Gate",               "value": "y_gate"},
                                                                {"label": "X Value",              "value": "x_value"},
                                                                {"label": "Y Value",              "value": "y_value"},
                                                            ],
                                                            value="combined",
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        dcc.Checklist(
                                                            id="ctl-ps-log-x",
                                                            options=[{"label": "log₁₀ X", "value": "log"}],
                                                            value=[],
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        dcc.Checklist(
                                                            id="ctl-ps-log-y",
                                                            options=[{"label": "log₁₀ Y", "value": "log"}],
                                                            value=[],
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pv-group",
                                                    children=[
                                                        dcc.Checklist(
                                                            id="ctl-ps-labels",
                                                            options=[{"label": "Label all points", "value": "label"}],
                                                            value=["label"],
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        ),
                                
                                        # Store + Graph
                                        dcc.Store(id="store-player-scatter"),
                                        dcc.Loading(
                                            type="default",
                                            children=dcc.Graph(
                                                id="ax-ps-graph",
                                                className="ax-ps-graph",
                                                figure=go.Figure(),
                                                style={"height": "650px", "width": "100%"},
                                                config={"displayModeBar": False, "responsive": True},
                                            ),
                                        ),
                                    ],
                                ),
                                # ============================
                                # /Analytics Nexus — Player Quadrant Scatter
                                # ============================
                                # ============================
                                # Analytics Nexus — Player Rolling Percentiles (ax-pr-*)
                                # ============================
                                html.Section(
                                    id="ax-pr-section",
                                    className="ax-pr-section",
                                    children=[
                                        html.H2("Players — Rolling Form Percentiles", className="ax-pr-title"),
                                
                                        # Controls (reuse same visual card style)
                                        html.Div(
                                            className="ax-pt-controls",
                                            children=[
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Seasons (multi)"),
                                                        dcc.Dropdown(
                                                            id="ctl-pr-seasons",
                                                            options=SEASON_OPTIONS,
                                                            value=[DEFAULT_SEASON],
                                                            multi=True,
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Season Type"),
                                                        dcc.RadioItems(
                                                            id="ctl-pr-season-type",
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
                                                        html.Label("Position"),
                                                        dcc.RadioItems(
                                                            id="ctl-pr-position",
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
                                                        html.Label("Metric"),
                                                        dcc.Dropdown(
                                                            id="ctl-pr-metric",
                                                            options=STAT_OPTIONS,
                                                            value="passing_yards",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Top N"),
                                                        dcc.Input(
                                                            id="ctl-pr-topn",
                                                            type="number",
                                                            min=1, max=32, step=1,
                                                            value=8,
                                                            className="ax-pt-topn",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group ax-pt-span-2",
                                                    children=[
                                                        html.Label("Week Range"),
                                                        dcc.RangeSlider(
                                                            id="ctl-pr-week-range",
                                                            min=1, max=22,
                                                            value=[1, DEFAULT_WEEK_END],
                                                            allowCross=False, pushable=0,
                                                            marks={i: str(i) for i in range(1, 23)},
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Rolling Window (k)"),
                                                        dcc.Input(
                                                            id="ctl-pr-roll-k",
                                                            type="number",
                                                            min=1, step=1, value=4,
                                                            className="ax-pt-topn",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        dcc.Checklist(
                                                            id="ctl-pr-show-points",
                                                            options=[{"label": "Show weekly points", "value": "show"}],
                                                            value=["show"],   # default ON
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        dcc.Checklist(
                                                            id="ctl-pr-label-last",
                                                            options=[{"label": "Label last value", "value": "label"}],
                                                            value=["label"],  # default ON
                                                            inline=True,
                                                            inputClassName="ax-pt-radio-input",
                                                            labelClassName="ax-pt-radio-label",
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="ax-pt-group",
                                                    children=[
                                                        html.Label("Panels per row"),
                                                        dcc.Input(
                                                            id="ctl-pr-ncol",
                                                            type="number",
                                                            min=1, max=6, step=1,
                                                            value=4,
                                                            className="ax-pt-topn",
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        ),
                                
                                        # Store + Graph
                                        dcc.Store(id="store-player-rolling"),
                                        dcc.Loading(
                                            type="default",
                                            children=dcc.Graph(
                                                id="ax-pr-graph",
                                                className="ax-pt-graph",
                                                figure=go.Figure(),
                                                style={"height": "650px", "width": "100%"},
                                                config={"displayModeBar": False, "responsive": True},
                                            ),
                                        ),
                                    ],
                                ),
                                # ============================
                                # /Analytics Nexus — Player Rolling Percentiles
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
                         f"{series_label} • Rank By={rankby}{floor_label}",
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

# ============================
# Callbacks — Analytics Nexus: Player Violins
# ============================

@callback(
    Output("store-player-violins", "data"),
    Input("selected-plot", "data"),
    Input("ctl-pv-seasons", "value"),
    Input("ctl-pv-season-type", "value"),
    Input("ctl-pv-stat", "value"),
    Input("ctl-pv-position", "value"),
    Input("ctl-pv-topn", "value"),
    Input("ctl-pv-week-range", "value"),
    Input("ctl-pv-series", "value"),
    Input("ctl-pv-order-by", "value"),
    Input("ctl-pv-min-badges", "value"),
    prevent_initial_call=False,
)
def fetch_ax_pv_data(selected_plot, seasons, season_type, stat_name, position,
                     topn, week_range, series_mode, order_by, min_badges):
    if selected_plot != "nav-player-violin":
        return no_update

    if not all([seasons, season_type, stat_name, position, topn, week_range, series_mode, order_by]) \
       or min_badges is None:
        return {"weekly": [], "summary": [], "badges": {"most_consistent": "—", "most_volatile": "—"}, "meta": {}}

    week_start, week_end = int(week_range[0]), int(week_range[1])
    if week_end < week_start or int(topn) < 1:
        return {"weekly": [], "summary": [], "badges": {"most_consistent": "—", "most_volatile": "—"}, "meta": {}}

    payload = fetch_player_violins(
        seasons=seasons,
        season_type=str(season_type),
        stat_name=str(stat_name),
        position=str(position),
        top_n=int(topn),
        week_start=week_start,
        week_end=week_end,
        stat_type=str(series_mode),
        order_by=str(order_by),
        min_games_for_badges=int(min_badges),
        timeout=5,
        debug=True,
    )
    return payload or {"weekly": [], "summary": [], "badges": {"most_consistent": "—", "most_volatile": "—"}, "meta": {}}


@callback(
    Output("ax-pv-graph", "figure"),
    Input("store-player-violins", "data"),
    State("ctl-pv-show-points", "value"),
    State("ctl-pv-stat", "value"),
)
def render_ax_pv_figure(payload, show_points_vals, stat_name):
    fig = go.Figure()
    show_points = isinstance(show_points_vals, list) and ("show" in show_points_vals)

    # Empty-state
    if not payload or not isinstance(payload, dict) or not payload.get("summary"):
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="white",
            plot_bgcolor="white",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[
                dict(
                    text="No data to plot<br>Check filters: seasons, season_type, stat, position, week_range",
                    x=0.5, y=0.5, xref="paper", yref="paper",
                    showarrow=False, font=dict(size=16, color="#444"),
                )
            ],
            margin=dict(l=40, r=20, t=80, b=40),
            autosize=True,
        )
        return fig

    weekly = payload.get("weekly", [])
    summary = payload.get("summary", [])
    badges = payload.get("badges", {}) or {}
    meta   = payload.get("meta", {}) or {}

    # Labels (match R)
    stat_label = next((o["label"] for o in STAT_OPTIONS if o["value"] == stat_name), stat_name)
    seasons = meta.get("seasons", [])
    if seasons:
        seasons_sorted = sorted(seasons)
        if max(seasons_sorted) - min(seasons_sorted) + 1 == len(seasons_sorted):
            season_text = f"{seasons_sorted[0]}–{seasons_sorted[-1]}"
        else:
            season_text = ", ".join(str(s) for s in seasons_sorted)
    else:
        season_text = ""

    type_text = "REG+POST" if meta.get("season_type") == "ALL" else meta.get("season_type", "REG")
    week_text = f"Weeks {meta.get('week_start', 1)}–{meta.get('week_end', 18)}"
    order_by  = meta.get("order_by", "rCV")
    top_n     = meta.get("top_n", 0)

    most_consistent = badges.get("most_consistent", "—")
    most_consistent_list = most_consistent if isinstance(most_consistent, list) else ([most_consistent] if most_consistent != "—" else [])
    most_volatile = badges.get("most_volatile", "—")
    most_volatile_list = most_volatile if isinstance(most_volatile, list) else ([most_volatile] if most_volatile != "—" else [])

    # Order by player_order
    ordered = sorted(summary, key=lambda s: s.get("player_order", 10**9))
    x_labels = []
    x_key_by_order = {}  # order -> player_id
    for s in ordered:
        lbl = f"{s.get('name','')}\n(n={s.get('n_games',0)})"
        x_labels.append(lbl)
        x_key_by_order[s["player_order"]] = s["player_id"]

    # Build per-player lookup for weekly points
    by_player = {}
    for r in weekly:
        pid = r["player_id"]
        by_player.setdefault(pid, {"y": [], "week": [], "season": [], "pt_color": []})
        by_player[pid]["y"].append(r["value"])
        by_player[pid]["week"].append(r["week"])
        by_player[pid]["season"].append(r["season"])
        by_player[pid]["pt_color"].append(r.get("team_color2") or "#AAAAAA")

    # Add one violin trace per player (outline in dominant team color; dim if small-n)
    for s in ordered:
        pid = s["player_id"]
        name = s.get("name", "")
        team_color = s.get("team_color_major") or "#888888"
        small_n = bool(s.get("small_n", False))
        order_index = s["player_order"]  # 1..N
        label = x_labels[order_index - 1]

        pts = by_player.get(pid, {"y": [], "week": [], "season": [], "pt_color": []})
        yvals = pts["y"]
        # Customdata for hover on points
        custom = list(zip(pts["week"], pts["season"]))

        # compute a single point color (mode of team_color2s across weeks)
        pt_color_mode = (max(pts["pt_color"], key=pts["pt_color"].count) if pts["pt_color"] else "#AAAAAA")
        
        fig.add_trace(
            go.Violin(
                x=[label] * len(yvals),
                y=yvals,
                name=label,
                line=dict(color=team_color, width=1.1),
                fillcolor="rgba(0,0,0,0)",
                opacity=0.45 if small_n else 1.0,
                points="all" if show_points else False,   # ← jittered points ON/OFF
                pointpos=0.0,
                jitter=0.18,                               # ← jitter amount (like ggplot)
                scalemode="width",
                marker=dict(                               # ← one color for all points
                    size=6,
                    color=pt_color_mode,
                    line=dict(color="black", width=0.6),
                    opacity=0.65,
                ),
                customdata=custom,                         # (week, season)
                hoveron="points" if show_points else "violins",
                hovertemplate=(
                    "<b>"+name+"</b><br>"
                    "Week %{customdata[0]} • Season %{customdata[1]}<br>"
                    "Value: %{y}<extra></extra>"
                ),
                showlegend=False,
            )
        )

        # IQR (thick vertical segment) & Median tick
        q25 = s.get("q25")
        q50 = s.get("q50")
        q75 = s.get("q75")
        xcat = label  # category name consistent across traces

        if q25 is not None and q75 is not None:
            fig.add_trace(
                go.Scatter(
                    x=[xcat, xcat],
                    y=[q25, q75],
                    mode="lines",
                    line=dict(color=team_color, width=6),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
        if q50 is not None:
            fig.add_trace(
                go.Scatter(
                    x=[xcat],
                    y=[q50],
                    mode="markers",
                    marker=dict(color=team_color, size=8),
                    hovertemplate=f"<b>{name}</b><br>Median: %{{y}}<extra></extra>",
                    showlegend=False,
                )
            )

    title = f"Top {top_n} {stat_label} — {season_text} ({type_text})"
    subtitle = (
        f"{week_text}  •  Order by {order_by}  •  "
        f"Most consistent: {', '.join(most_consistent_list) if most_consistent_list else '—'}  •  "
        f"Most volatile: {', '.join(most_volatile_list) if most_volatile_list else '—'}"
    )

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title=dict(
            text=f"{title}<br><span style='font-size:0.8em;color:#444'>{subtitle}</span>",
            x=0.02, y=0.98, xanchor="left", yanchor="top",
        ),
        xaxis=dict(
            title=None,
            categoryorder="array",
            categoryarray=x_labels,          # enforce order by player_order
            tickangle=28,
            tickfont=dict(size=11),
            gridcolor="rgba(0,0,0,0.08)",
        ),
        yaxis=dict(
            title=stat_label,
            gridcolor="rgba(0,0,0,0.08)",
            zeroline=False,
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0),
        margin=dict(l=60, r=20, t=120, b=64),
        autosize=True,
    )
    return fig

@callback(
    Output("ax-pt-section", "style"),
    Output("ax-pv-section", "style"),
    Output("ax-ps-section", "style"),
    Output("ax-pr-section", "style"),
    Input("selected-plot", "data"),
)
def toggle_sections(selected):
    if selected == "nav-player-violin":
        return {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}
    if selected == "nav-player-scatter":
        return {"display": "none"}, {"display": "none"}, {"display": "block"}, {"display": "none"}
    if selected == "nav-player-percentiles":
        return {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "block"}
    # default: trajectories visible
    return {"display": "block"}, {"display": "none"}, {"display": "none"}, {"display": "none"}

@callback(
    Output("selected-plot", "data"),
    Input("nav-player-trajectories", "n_clicks"),
    Input("nav-player-violin", "n_clicks"),
    Input("nav-player-scatter", "n_clicks"),
    Input("nav-player-percentiles", "n_clicks"),
    prevent_initial_call=True,
)
def set_selected_plot(n_pt, n_pv, n_ps, n_pr):
    if not ctx.triggered_id:
        return no_update
    if ctx.triggered_id == "nav-player-trajectories":
        return "nav-player-trajectories"
    if ctx.triggered_id == "nav-player-violin":
        return "nav-player-violin"
    if ctx.triggered_id == "nav-player-scatter":
        return "nav-player-scatter"
    if ctx.triggered_id == "nav-player-percentiles":
        return "nav-player-percentiles"
    return no_update
  
# ============================
# Callbacks — Analytics Nexus: Player scatter plot
# ============================
  
@callback(
  Output("store-player-scatter", "data"),
  Input("selected-plot", "data"),
  Input("ctl-ps-seasons", "value"),
  Input("ctl-ps-season-type", "value"),
  Input("ctl-ps-position", "value"),
  Input("ctl-ps-topn", "value"),
  Input("ctl-ps-metric-x", "value"),
  Input("ctl-ps-metric-y", "value"),
  Input("ctl-ps-week-range", "value"),
  Input("ctl-ps-top-by", "value"),
  Input("ctl-ps-log-x", "value"),
  Input("ctl-ps-log-y", "value"),
  Input("ctl-ps-labels", "value"),
  prevent_initial_call=False,
)
def fetch_ax_ps_data(selected_plot, seasons, season_type, position, topn, metric_x, metric_y,
                     week_range, top_by, log_x_vals, log_y_vals, label_vals):
    if selected_plot != "nav-player-scatter":
        return no_update

    if not all([seasons, season_type, position, topn, metric_x, metric_y, week_range, top_by]):
        return {}

    week_start, week_end = int(week_range[0]), int(week_range[1])
    if week_end < week_start or int(topn) < 1:
        return {}

    log_x = isinstance(log_x_vals, list) and ("log" in log_x_vals)
    log_y = isinstance(log_y_vals, list) and ("log" in log_y_vals)
    label_all = isinstance(label_vals, list) and ("label" in label_vals)

    payload = fetch_player_scatter(
        seasons=seasons,
        season_type=str(season_type),
        position=str(position),
        metric_x=str(metric_x),
        metric_y=str(metric_y),
        top_n=int(topn),
        week_start=week_start,
        week_end=week_end,
        stat_type="base",
        top_by=str(top_by),
        log_x=log_x,
        log_y=log_y,
        label_all_points=label_all,
        timeout=6,
        debug=True,
    )
    return payload or {}

@callback(
    Output("ax-ps-graph", "figure"),
    Input("store-player-scatter", "data"),
)
def render_ax_ps_figure(payload):
    fig = go.Figure()

    if not payload or not isinstance(payload, dict):
        # Empty-state
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="white",
            plot_bgcolor="white",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[dict(
                text="No data to plot<br>Check filters.",
                x=0.5, y=0.5, xref="paper", yref="paper",
                showarrow=False, font=dict(size=16, color="#444"),
            )],
            margin=dict(l=40, r=20, t=80, b=40),
            autosize=True,
        )
        return fig

    pts  = payload.get("points", []) or []
    meta = payload.get("meta", {}) or {}

    if not pts:
        return fig

    # Build arrays directly from payload points
    xs      = [p.get("x", p.get("x_value")) for p in pts]
    ys      = [p.get("y", p.get("y_value")) for p in pts]
    names   = [p.get("name","") for p in pts]
    fills   = [p.get("team_color2") or "#AAAAAA" for p in pts]  # fill
    strokes = [p.get("team_color")  or "#333333"  for p in pts]  # outline

    def _pretty(s):
        return str(s).replace("_", " ").title() if s else None

    metric_x_id = meta.get("metric_x")
    metric_y_id = meta.get("metric_y")

    x_label = (
        meta.get("metric_x_label")
        or meta.get("x_label")
        or _pretty(metric_x_id)
        or "X"
    )
    y_label = (
        meta.get("metric_y_label")
        or meta.get("y_label")
        or _pretty(metric_y_id)
        or "Y"
    )

    # Median guides (handle either key name)
    mx = meta.get("median_x", meta.get("med_x"))
    my = meta.get("median_y", meta.get("med_y"))
    if mx is not None:
        fig.add_vline(x=mx, line_width=1, line_dash="dash", line_color="grey")
    if my is not None:
        fig.add_hline(y=my, line_width=1, line_dash="dash", line_color="grey")

    # Equal aspect (square units)
    fig.update_yaxes(scaleanchor="x", scaleratio=1)

    # Main scatter points (fill=team_color2, outline=team_color)
    fig.add_trace(
        go.Scatter(
            x=xs, y=ys,
            mode="markers+text",
            text=names,                   # always-on labels
            texttemplate="%{text}",
            textposition="top center",
            textfont=dict(size=12),
            cliponaxis=False,             # allow labels to breathe
            marker=dict(
                size=16,
                color=fills,              # per-point fill (team_color2)
                line=dict(color=strokes, width=0.8)  # per-point outline (team_color)
            ),
            hovertemplate=(
                "<b>%{text}</b><br>"
                f"{x_label}: %{{x}}<br>"
                f"{y_label}: %{{y}}<extra></extra>"
            ),
            showlegend=False,
        )
    )

    # Log toggles (router pre-filters nonpositive)
    if meta.get("log_x"):
        fig.update_xaxes(type="log")
    if meta.get("log_y"):
        fig.update_yaxes(type="log")

    # Title + subtitle
    title = f"{x_label} vs {y_label}"
    seasons = meta.get("seasons", [])
    if seasons:
        s_sorted = sorted(seasons)
        season_text = f"{s_sorted[0]}–{s_sorted[-1]}" if (max(s_sorted)-min(s_sorted)+1)==len(s_sorted) else ", ".join(map(str, s_sorted))
    else:
        season_text = ""
    type_text = "REG+POST" if meta.get("season_type") == "ALL" else meta.get("season_type", "REG")
    week_text = f"Weeks {meta.get('week_start',1)}–{meta.get('week_end',18)}"
    subtitle = (
        f"{meta.get('position','')} • {season_text} ({type_text}) • "
        f"{week_text} • Top {meta.get('top_n',0)} by {meta.get('top_by','combined')} • Medians shown"
    )

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title=dict(
            text=f"{title}<br><span style='font-size:0.8em;color:#444'>{subtitle}</span>",
            x=0.02, y=0.98, xanchor="left", yanchor="top",
        ),
        xaxis=dict(
            title_text=x_label,
            gridcolor="rgba(0,0,0,0.08)",
            zeroline=False,
        ),
        yaxis=dict(
            title_text=y_label,
            gridcolor="rgba(0,0,0,0.08)",
            zeroline=False,
        ),
        margin=dict(l=60, r=20, t=120, b=64),
        autosize=True,
        showlegend=False,
    )

    # Belt-and-suspenders (force titles in case template overrides)
    fig.update_xaxes(title=x_label)
    fig.update_yaxes(title=y_label)

    return fig

# ============================
# Callbacks — Analytics Nexus: Player Rolling Percentiles
# ============================

@callback(
    Output("store-player-rolling", "data"),
    Input("selected-plot", "data"),
    Input("ctl-pr-seasons", "value"),
    Input("ctl-pr-season-type", "value"),
    Input("ctl-pr-position", "value"),
    Input("ctl-pr-topn", "value"),
    Input("ctl-pr-metric", "value"),
    Input("ctl-pr-week-range", "value"),
    Input("ctl-pr-roll-k", "value"),
    prevent_initial_call=False,
)
def fetch_ax_pr_data(selected_plot, seasons, season_type, position, topn, metric, week_range, roll_k):
    # only run on the Rolling Percentiles tab
    if selected_plot != "nav-player-percentiles":
        return no_update

    if not all([seasons, season_type, position, topn, metric, week_range, roll_k]):
        return {"series": [], "players": [], "meta": {}}

    week_start, week_end = int(week_range[0]), int(week_range[1])
    if week_end < week_start or int(topn) < 1 or int(roll_k) < 1:
        return {"series": [], "players": [], "meta": {}}

    payload = fetch_player_rolling_percentiles(
        seasons=seasons,
        season_type=str(season_type),
        position=str(position),
        metric=str(metric),
        top_n=int(topn),
        week_start=week_start,
        week_end=week_end,
        stat_type="base",
        rolling_window=int(roll_k),
        timeout=8,
        debug=True,
    )
    return payload or {"series": [], "players": [], "meta": {}}


@callback(
    Output("ax-pr-graph", "figure"),
    Input("store-player-rolling", "data"),
    State("ctl-pr-show-points", "value"),
    State("ctl-pr-label-last", "value"),
    State("ctl-pr-ncol", "value"),
)
def render_ax_pr_figure(payload, show_points_vals, label_last_vals, ncol_val):
    # always initialize a figure
    fig = go.Figure()
    
    import json

    # After fetching payload
    payload_str = json.dumps(payload, indent=2)[:300] 

    # hard title so you can verify callback fires
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title=dict(
        text=f"<span style='font-size:0.7em;color:#444'>{payload_str}</span>",
            x=0.02, y=0.98, xanchor="left", yanchor="top",
        ),
        margin=dict(l=40, r=20, t=120, b=40),
        showlegend=False,
    )

    if not payload or not isinstance(payload, dict):
        return fig

    series = payload.get("series") or []
    players = payload.get("players") or []
    meta = payload.get("meta") or {}

    # we can render from SERIES alone
    if not series:
        return fig

    # build per-player series (use keys returned by the router)
    by_player = {}
    order_hint = {}

    for r in series:
        pid = r.get("player_id")
        if not pid:
            continue
        t = r.get("t_idx")
        y = r.get("pct_roll")
        if t is None or y is None:
            continue

        s = by_player.setdefault(pid, {
            "t": [], "y": [],
            "name": r.get("name", ""),
            "line": r.get("team_color", "#888"),
            "fill": r.get("team_color2", "#AAA"),
        })
        # coerce types
        try:
            s["t"].append(int(t))
        except Exception:
            continue
        try:
            s["y"].append(float(y))
        except Exception:
            s["y"].append(None)

        po = r.get("player_order")
        if po is not None:
            order_hint[pid] = min(order_hint.get(pid, po), po)

    if not by_player:
        return fig

    # prefer PLAYERS order; fallback to order_hint
    pids_from_players = [p.get("player_id") for p in players if p.get("player_id")]
    names_lookup = {p.get("player_id"): p.get("name", "") for p in players if p.get("player_id")}

    pids = [pid for pid in pids_from_players if pid in by_player and by_player[pid]["t"]]
    if not pids:
        pids = sorted(by_player.keys(), key=lambda k: (order_hint.get(k, 10**9), k))

    names = [names_lookup.get(pid, by_player[pid]["name"]) for pid in pids]

    # grid layout
    ncol = max(1, min(6, int(ncol_val or 4)))
    n = len(pids)
    rows = (n + ncol - 1) // ncol
    titles = names + [""] * (rows * ncol - len(names))

    fig = make_subplots(
        rows=rows, cols=ncol,
        subplot_titles=tuple(titles),
        horizontal_spacing=0.05, vertical_spacing=0.1,
    )

    show_points = isinstance(show_points_vals, list) and ("show" in show_points_vals)
    label_last  = isinstance(label_last_vals, list)  and ("label" in label_last_vals)

    for i, pid in enumerate(pids):
        r_i = (i // ncol) + 1
        c_i = (i % ncol) + 1
        s = by_player[pid]
        pts = sorted((tt, yy) for tt, yy in zip(s["t"], s["y"]) if tt is not None and yy is not None)
        if not pts:
            continue
        xs = [a for a, _ in pts]
        ys = [b for _, b in pts]

        fig.add_trace(
            go.Scatter(
                x=xs, y=ys,
                mode="lines" + ("+markers" if show_points else ""),
                line=dict(color=s["line"], width=2),
                marker=(dict(size=6, color=s["fill"], line=dict(color="black", width=0.5)) if show_points else None),
                hovertemplate="<b>%{y:.0f}</b><extra></extra>",
                showlegend=False,
            ),
            row=r_i, col=c_i
        )

        if label_last:
            fig.add_trace(
                go.Scatter(
                    x=[xs[-1]], y=[ys[-1]],
                    mode="text", text=[f"{ys[-1]:.0f}"],
                    textposition="middle right",
                    textfont=dict(size=11),
                    cliponaxis=False, showlegend=False, hoverinfo="skip",
                ),
                row=r_i, col=c_i
            )

        fig.update_xaxes(range=[min(xs)-0.5, max(xs)+0.5], tickmode="linear", dtick=1,
                         showticklabels=False, row=r_i, col=c_i)
        fig.update_yaxes(range=[0, 100], tickvals=[0,25,50,75,100], row=r_i, col=c_i)

    # final layout + height scaling
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title=dict(text="HELP ME", x=0.02, y=0.98, xanchor="left", yanchor="top"),
        margin=dict(l=40, r=20, t=120, b=40),
        showlegend=False,
        height=max(400, 260 * rows),
    )
    fig.update_yaxes(title_text="Percentile (within position, weekly)", row=1, col=1)
    return fig
