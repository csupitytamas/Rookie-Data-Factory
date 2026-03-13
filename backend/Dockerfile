# 1. Alapkép
FROM python:3.10-slim

# 2. Munkakönyvtár a konténeren belül
WORKDIR /app

# 3. Rendszerfüggőségek (psycopg2-höz és gcc-hez kellhetnek)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. Requirements másolása (A gyökérből indulva!)
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. A backend forráskód másolása
COPY backend/src ./src

# 6. A DAG-ok beemelése az Airflow mappából (hogy a backend is lássa őket)
COPY airflow/dags ./src/airflow/dags/

# 7. Környezeti változók (PYTHONPATH)
ENV PYTHONPATH="/app/src:/app"

# 8. Indítás (adatbázis init + uvicorn)
CMD ["sh", "-c", "python src/database/init_db_manual.py && uvicorn src.main:app --host 0.0.0.0 --port 8000"]