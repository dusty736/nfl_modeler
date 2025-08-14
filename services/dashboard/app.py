# services/dashboard/app.py

from pathlib import Path
import dash
from dash import html

def _find_pages_dir() -> str:
    """
    Find the actual pages directory inside the container.
    Supports both:
      - /app/pages
      - /app/services/dashboard/pages
    Returns "" if not found (Dash will start without pages).
    """
    here = Path(__file__).parent  # expected: /app
    candidates = [
        here / "pages",
        here / "services" / "dashboard" / "pages",
    ]
    for p in candidates:
        if p.exists():
            print(f"[dash] Using pages_folder: {p}")
            return str(p)
    print("[dash][WARN] No pages folder found among:", *map(str, candidates), sep="\n  ")
    return ""

app = dash.Dash(
    __name__,
    title="NFL Analytics • 2025",
    use_pages=True,
    pages_folder=_find_pages_dir(),
    suppress_callback_exceptions=True  # ← allow page-specific IDs
)
server = app.server

# Global container for whichever page is active
app.layout = html.Div([dash.page_container])

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)

