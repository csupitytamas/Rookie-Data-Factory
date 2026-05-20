from pydantic import BaseModel
from typing import Optional

""" Ez a modul a rendszerbeállítások  adatstruktúráját definiáló sémát tartalmazza. """

# A rendszerbeállításokat reprezentáló modell.
class SettingsSchema(BaseModel):
    timezone: str = "Europe/Budapest"
    download_path: Optional[str] = None

    class Config:
        from_attributes = True
