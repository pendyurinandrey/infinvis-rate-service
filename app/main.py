from fastapi import FastAPI

from app.routers import fx

app = FastAPI(title="InFinViz Rate Service")

app.include_router(fx.router)