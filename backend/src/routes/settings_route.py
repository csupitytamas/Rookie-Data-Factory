from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from src.database.connection import get_db
from src.models.settings_model import Settings

router = APIRouter()

class SettingsSchema(BaseModel):
    timezone: str
    download_path: str

@router.get("/")
def get_settings(db: Session = Depends(get_db)):
    # Mindig az első (és egyetlen) sort kérjük le
    settings = db.query(Settings).first()
    
    # Ha még üres az adatbázis (első indítás), létrehozunk egy default-ot
    if not settings:
        settings = Settings(timezone="UTC", download_path="")
        db.add(settings)
        db.commit()
        db.refresh(settings)
        
    return settings

@router.put("/")
def update_settings(config: SettingsSchema, db: Session = Depends(get_db)):
    settings = db.query(Settings).first()
    # (Itt is kezelheted, ha None, de a GET már megoldotta elvileg)
    if not settings:
        settings = Settings()
        db.add(settings)

    settings.timezone = config.timezone
    settings.download_path = config.download_path
    
    db.commit()
    return {"message": "Settings updated"}