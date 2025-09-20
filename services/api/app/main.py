"""
API Entrypoint
--------------
Creates the FastAPI app, exposes a minimal health check, redirects "/" -> "/docs",
and mounts router modules explicitly (standings, current_week, primetime, etc).

Notes
-----
- No functional changes here: purely documentation and comments.
- Routers are imported explicitly so startup fails fast if a module is missing.
"""

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import importlib

# --- Application factory -------------------------------------------------------
def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        FastAPI: Configured app with:
            - GET /health  -> {"status": "ok"} (simple liveness)
            - GET /        -> redirects to /docs
            - Routers mounted from app.routers.[standings, current_week, primetime, teams,
              team_stats, team_rosters, team_injuries, analytics_nexus].

    Notes:
        - Router modules are imported explicitly via importlib for clarity and to ensure
          import errors surface at startup rather than on first request.
    """
    app = FastAPI(title="NFL Analytics API", version="0.1.0")

    @app.get("/health")
    def health():
        """Lightweight liveness endpoint used by containers/load balancers."""
        return {"status": "ok"}

    @app.get("/")
    def index():
        """Redirect root to the interactive API docs (/docs)."""
        # FastAPI's RedirectResponse defaults to 307 (temporary) which is fine for docs.
        return RedirectResponse(url="/docs")

    # Explicit, educational mounting: keep this list in a stable order to reduce merge churn.
    # If you add a new router module under app/routers/, list it here.
    for mod in [
        "standings",
        "current_week",
        "primetime",
        "teams",
        "team_stats",
        "team_rosters",
        "team_injuries",
        "analytics_nexus",
        "games"
    ]:
        m = importlib.import_module(f"app.routers.{mod}")
        app.include_router(m.router)

    return app


# Expose a module-level ASGI app for uvicorn/gunicorn targets like "app.main:app".
# (This does not change behavior; it simply instantiates via the factory at import time.)
app = create_app()

# If ever run directly (not how containers usually do it), this gives a dev server.
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
