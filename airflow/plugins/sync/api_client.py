
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

AIRFLOW_API_URL = "http://airflow-webserver:8080/api/v1"
USERNAME = "airflow"
PASSWORD = "modmq888"

def get_next_scheduled_run(dag_id):
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}"

    response = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD))

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return None

    data = response.json()
    next_run_str = data.get('next_dagrun')
    if next_run_str:
        return datetime.fromisoformat(next_run_str.replace('Z', '+00:00'))  # timezone kompatibilis
    return None


