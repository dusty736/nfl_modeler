"""
API Entrypoint
--------------
Creates the FastAPI app, exposes a minimal health check, redirects "/" -> "/docs",
and mounts router modules explicitly (standings, current_week, primetime, etc.).

Notes
-----
- Cloud Run sets PORT via env; local dev defaults to 8080.
- We avoid creating a global `app` at import time. Use the factory instead.
"""

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import importlib
import logging
import os

logger = logging.getLogger("api")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


# --- Application factory -------------------------------------------------------
def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        FastAPI: Configured app with:
            - GET /health -> {"status": "ok"} (simple liveness)
            - GET /       -> redirects to /docs
            - Routers mounted from app.routers.[standings, current_week, primetime,
              teams, team_stats, team_rosters, team_injuries, analytics_nexus, games].
    """
    app = FastAPI(title="NFL Analytics API", version="0.1.0")

    @app.get("/health")
    def health():
        """Lightweight liveness endpoint used by containers/load balancers."""
        return {"status": "ok"}

    @app.get("/")
    def index():
        """Redirect root to the interactive API docs (/docs)."""
        return RedirectResponse(url="/docs")

    # Fail-fast, explicit router imports (keeps startup honest).
    routers = [
        "standings",
        "current_week",
        "primetime",
        "teams",
        "team_stats",
        "team_rosters",
        "team_injuries",
        "analytics_nexus",
        "games",
    ]
    for mod in routers:
        full = f"app.routers.{mod}"
        logger.info("Mounting router: %s", full)
        m = importlib.import_module(full)
        app.include_router(m.router)
        
    for r in app.router.routes:
      try:
          logger.info("ROUTE %s %s", getattr(r, "path", "?"), [m for m in getattr(r, "methods", [])])
      except Exception:
          pass

    logger.info("API startup complete.")
    return app


# --- Local dev entrypoint ------------------------------------------------------
# For local runs: `python -m uvicorn app.main:create_app --factory --reload`
# OR simply `python services/api/app/main.py`
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8080"))
    # Use factory mode so imports happen after Uvicorn has initialized logging, etc.
    uvicorn.run("app.main:create_app", host="0.0.0.0", port=port, factory=True, reload=True)

