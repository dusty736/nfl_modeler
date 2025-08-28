"""
Dashboard Entrypoint (Dash)
---------------------------
Creates the Dash app, auto-discovers pages, exposes `server` for WSGI, and runs
the dev server when invoked directly.

Conventions
-----------
- Pages: uses Dash Pages (`use_pages=True`) and a dynamic `pages_folder` that works
  both locally and inside the container (see `_find_pages_dir`).
- Assets: standard Dash behaviour auto-loads `/assets` (CSS, images, etc.).
- Exports: `server = app.server` is what Gunicorn/uvicorn workers import.

Notes
-----
- No functional changes in this file; only documentation and comments.
"""

# services/dashboard/app.py

from pathlib import Path
import dash
from dash import html

# --- App bootstrap --------------------------------------------------------------

def _find_pages_dir() -> str:
    """
    Resolve the pages directory inside the container or local dev tree.
    
    Search order:
      1) /app/pages
      2) /app/services/dashboard/pages
    
    Returns:
        str: Absolute path to the pages directory if found; otherwise "" (Dash
             will still start with `use_pages=True` and no pages registered).
    
    Side effects:
        Prints a one-line selection, or a WARN block listing checked candidates.
    
    Why:
        Keeps docker-compose and local layouts both working without env flags.
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

