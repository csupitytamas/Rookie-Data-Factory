# Rookie Data Factroy


Frontend start:
(Vue + Electron)
```sh
cd frontend
npm run dev
npm start

```

```
cd backend
uvicorn src.main:app --reload --log-level debug
```
Backend start:
(FastAPI)
```sh
cd backend
uvicorn src.main:app --reload --log-level debug
``` 
Database: (PostgreSQL)


AIRFLOW start:
```
cd /mnt/c/MINISZTERUHR/GITHUB/etl-app-n-/airflow
sudo docker compose up -d
http://localhost:8080/home
```

A SIMPLE DAG:

1. import
2. functions 
3. DAG init
4. tasks init
