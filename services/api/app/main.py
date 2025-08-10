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

    season = importlib.import_module("app.routers.season")
    app.include_router(season.router)
    return app

