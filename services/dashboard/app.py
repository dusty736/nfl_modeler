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
- Exports: `server = app.server` is what Gunicorn (or uvicorn) imports.

Notes
-----
- Cloud Run sets the PORT env var. We default to 8080 for local dev.
"""

# services/dashboard/app.py

from pathlib import Path
import os
import dash
from dash import html

# --- App bootstrap --------------------------------------------------------------

def _find_pages_dir() -> str | None:
    """
    Resolve the pages directory inside the container or local dev tree.

    Search order:
      1) /app/pages
      2) /app/services/dashboard/pages

    Returns:
        str | None: Absolute path to the pages directory if found; otherwise None.
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
    print(
        "[dash][WARN] No pages folder found among:",
        *map(str, candidates),
        sep="\n  ",
    )
    return None

pages_dir = _find_pages_dir()

app = dash.Dash(
    __name__,
    title="NFL Analytics • 2025",
    use_pages=True,
    pages_folder=pages_dir,                 # Dash will ignore None and use default
    suppress_callback_exceptions=True       # allow page-specific IDs
)
server = app.server

# Global container for whichever page is active
app.layout = html.Div([dash.page_container])

if __name__ == "__main__":
    # Cloud Run injects PORT. Default to 8080 for local dev (`python app.py`)
    port = int(os.getenv("PORT", "8080"))
    debug = os.getenv("DEBUG", "0") in ("1", "true", "True", "YES", "yes")
    app.run_server(host="0.0.0.0", port=port, debug=debug)


