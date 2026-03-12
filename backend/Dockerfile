# 1. Alapkép
FROM python:3.10-slim

# 2. Munkakönyvtár
WORKDIR /app

# 3. Rendszerfüggőségek
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. Requirements másolása (figyelem: a build context a ROOT lesz!)
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. A teljes backend forráskód másolása
COPY backend/src ./src

# 6. A HIÁNYZÓ LÁNCSZEM: A gyári script beemelése a másik mappából
# Ezt csak akkor látja a Docker, ha a projekt főkönyvtárából indítod a buildet!
COPY airflow/dags/etl_generate_dags.py ./src/airflow/dags/

# 7. FastAPI port
EXPOSE 8000

# 8. Indítás (marad az eredeti logikád)
CMD ["sh", "-c", "python src/database/init_db_manual.py && uvicorn src.main:app --host 0.0.0.0 --port 8000"]