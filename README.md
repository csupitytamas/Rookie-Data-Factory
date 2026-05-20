# Rookie Data Factory

Apache Airflow alapú asztali alkalmazás fejlesztése ETL-folyamatok automatizálására és adatgyűjtésre

## Architektúra
*   **Frontend:** Vue.js keretrendszer Electron asztali környezetbe csomagolva.
*   **Backend:** FastAPI, amely a metaadatokat és beállításokat kezeli.
*   **Adatbázis:** PostgreSQL (konfigurációk és állapotok tárolására).
*   **Folyamatkezelés:** Apache Airflow (dinamikusan generált DAG-ok a pipeline-ok futtatására).

## Telepítés és Futtatás
Az alkalmazás konténerizált környezetben fut, a szolgáltatásokat a Docker Compose kezeli.
A telepítő letölthető innen: https://github.com/csupitytamas/Rookie-Data-Factory/releases/tag/Prod
