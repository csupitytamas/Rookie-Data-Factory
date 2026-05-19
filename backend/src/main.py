from fastapi import FastAPI
from sqlalchemy.orm import configure_mappers
from src.middleware.middleware import add_middlewares
from src.routes import (
    etl_config_router, 
    api_schemas_router, 
    dashboard_router, 
    history_router, 
    settings_router
)

""" A backend alkalmazás fő belépési pontja, amely inicializálja az alkalmazást."""

# A kapcsolatok inicializálása, a FastAPI alkalmazás létrehozása és a middleware csatolása.
configure_mappers()
app = FastAPI()
add_middlewares(app)

# Routerek regisztrálása
app.include_router(etl_config_router, prefix="/etl/pipeline", tags=["ETL Pipelines"])
app.include_router(api_schemas_router, prefix="/etl/pipeline", tags=["ETL Schemas"])
app.include_router(dashboard_router, prefix="/etl", tags=["ETL Dashboard"])
app.include_router(
    history_router, 
    prefix="/etl/pipeline/history", 
    tags=["History"]
)

# A rendszerbeállítások végpontjainak regisztrálása.
app.include_router(
    settings_router,
    prefix="/etl/settings",
    tags=["Settings"]
)

# Alapértelmezett teszt végpont az alkalmazás futásának ellenőrzésére.
@app.get("/")
def read_root():
    return {"Hello": "BACKEND"}
