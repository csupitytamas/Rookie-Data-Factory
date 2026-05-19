from pydantic import BaseModel
from typing import List, Dict, Any, Optional

""" A dashboardon megjelenítendő pipeline adatok struktúráját definiáló modelleket tartalmazza. """

# A dashboardon megjelenő egyes pipeline-ok adatait reprezentáló modell.
class DashboardPipelineResponse(BaseModel):
    id: int
    name: str
    lastRun: Optional[str]
    status: Optional[str]
    nextRun: Optional[str]
    source: str
    dag_id: Optional[str]
    alias: Optional[str]
    save_option: Optional[str] = "todatabase"
    sampleData: List[Dict[str, Any]]

    # Pydantic konfiguráció az ORM modellekkel való kompatibilitáshoz.
    class Config:
        from_attributes = True

# Típusdefiníció a pipeline-ok listájához.
DashboardListResponse = List[DashboardPipelineResponse]
