import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import os

""" 
A modul az Airflow API-val való kommunikációért felelős, 
speciálisan a következő futtatások lekérdezéséhez. 
"""

AIRFLOW_API_URL = os.environ.get("AIRFLOW_URL")
USERNAME = os.environ.get("AIRFLOW_USERNAME")
PASSWORD = os.environ.get("AIRFLOW_PASSWORD")

def get_next_scheduled_run(dag_id):
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}"
    response = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD))
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return None
    data = response.json()
    next_run_str = data.get('next_dagrun')
    if next_run_str:
        return datetime.fromisoformat(next_run_str.replace('Z', '+00:00'))
    return None


