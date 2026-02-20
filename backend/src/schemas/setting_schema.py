from pydantic import BaseModel
from typing import Optional

class SettingsBase(BaseModel):
    timezone: str
    download_path: Optional[str] = None

class SettingsUpdate(SettingsBase):
    pass

class SettingsResponse(SettingsBase):
    id: int

    class Config:
        orm_mode = True