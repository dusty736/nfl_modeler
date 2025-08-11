import os, httpx, pandas as pd
from dash import Dash, html, dash_table

API_BASE = os.getenv("API_BASE", "http://api:8000") + "/api"

def fetch_standings():
    try:
        with httpx.Client(timeout=10.0) as c:
            r = c.get(f"{API_BASE}/standings")
            r.raise_for_status()
            return pd.DataFrame(r.json()["items"]), None
    except Exception as e:
        return pd.DataFrame(), str(e)

def division_table(df, title):
    # Normalize color strings (accept '#RRGGBB' or 'RRGGBB')
    def norm_color(c, default):
        if not c or pd.isna(c):
            return default
        c = str(c).strip()
        return c if (c.startswith("#") or c.startswith("rgb")) else f"#{c.lstrip('#')}"

    # Build per-row style rules
    rules = []
    for _, r in df.iterrows():
        bg = norm_color(r.get("team_color"), "#ffffff")
        fg = norm_color(r.get("team_color2"), "#000000")
        rules.append({
            "if": {"filter_query": f'{{team_id}} = "{r["team_id"]}"'},
            "backgroundColor": bg,
            "color": fg,
        })

    cols = [
        {"name": "Team", "id": "team_id"},
        {"name": "W", "id": "wins"},
        {"name": "L", "id": "losses"},
        {"name": "T", "id": "ties"},
        {"name": "PD", "id": "point_diff"},
    ]

    return html.Div([
        html.H5(title, style={"margin":"0.25rem 0"}),
        dash_table.DataTable(
            data=df[["team_id","wins","losses","ties","point_diff"]].to_dict("records"),
            columns=cols,
            page_size=8,
            sort_action="native",
            style_header={"fontWeight":"700", "backgroundColor":"#f7f7f7"},
            style_cell={"padding":"6px", "textAlign":"left"},
            style_data_conditional=rules
        )
    ], style={"padding":"0.5rem","border":"1px solid #eee","borderRadius":"12px"})

app = Dash(__name__, title="NFL Analytics • 2025 Standings")
server = app.server

def layout():
    df, err = fetch_standings()
    if err or df.empty:
        return html.Div([
            html.H3("2025 Season — Division Standings"),
            html.Div("Could not load standings from API.", style={"color":"#b00"}),
            html.Pre(str(err or "No details"), style={"whiteSpace":"pre-wrap","fontSize":"0.9rem","color":"#666"})
        ], style={"padding":"16px"})

    afc = df[df["division"].str.startswith("AFC")]
    nfc = df[df["division"].str.startswith("NFC")]
    by = lambda base, name: base[base["division"] == name].reset_index(drop=True)

    return html.Div([
        html.H3("2025 Season — Division Standings"),
        html.Div([
            html.Div([
                html.H4("AFC"),
                html.Div([
                    division_table(by(afc,"AFC East"),"AFC East"),
                    division_table(by(afc,"AFC North"),"AFC North"),
                    division_table(by(afc,"AFC South"),"AFC South"),
                    division_table(by(afc,"AFC West"),"AFC West"),
                ], style={"display":"grid","gridTemplateColumns":"repeat(2,1fr)","gap":"12px"})
            ], style={"flex":"1","paddingRight":"12px"}),
            html.Div([
                html.H4("NFC"),
                html.Div([
                    division_table(by(nfc,"NFC East"),"NFC East"),
                    division_table(by(nfc,"NFC North"),"NFC North"),
                    division_table(by(nfc,"NFC South"),"NFC South"),
                    division_table(by(nfc,"NFC West"),"NFC West"),
                ], style={"display":"grid","gridTemplateColumns":"repeat(2,1fr)","gap":"12px"})
            ], style={"flex":"1","paddingLeft":"12px"}),
        ], style={"display":"flex","gap":"12px"}),
        html.Div("Data source: /api/standings", style={"marginTop":"10px","color":"#888"})
    ], style={"padding":"16px"})

app.layout = layout

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)
