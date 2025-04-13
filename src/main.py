from fastapi import FastAPI

from src.fx.router import router as fx_router

app = FastAPI(title="InFinViz Rate Service")

app.include_router(fx_router)