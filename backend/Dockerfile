# Használjunk egy könnyű Python 3.10-es alapképet
FROM python:3.10-slim

# Munkakönyvtár beállítása
WORKDIR /app

# Rendszerfüggőségek telepítése (pl. a psycopg2 miatt kellhet)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Másoljuk át a requirements.txt fájlt és telepítsük a csomagokat
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Másoljuk át a teljes backend kódot
COPY . .

# FastAPI portjának megnyitása
EXPOSE 8000

# Szerver indítása
CMD ["sh", "-c", "python src/database/init_db_manual.py && uvicorn src.main:app --host 0.0.0.0 --port 8000"]