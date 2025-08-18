from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import importlib

def create_app() -> FastAPI:
    app = FastAPI(title="NFL Analytics API", version="0.1.0")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/")
    def index():
        return RedirectResponse(url="/docs")

    # Explicit, educational mounting
    for mod in ["standings", "current_week", "primetime", "teams", "team_stats"]:
      m = importlib.import_module(f"app.routers.{mod}")
      app.include_router(m.router)

    return app

