import requests
from requests.auth import HTTPBasicAuth

AIRFLOW_API_URL = "http://localhost:8080/api/v1"
USERNAME = "airflow"
PASSWORD = "airflow"

def pause_airflow_dag(dag_id):
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}"
    payload = {
        "is_paused": True
    }
    response = requests.patch(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), json=payload)

    if response.status_code == 200:
        print(f"DAG {dag_id} sikeresen paused állapotba került.")
    else:
        print(f"Nem sikerült pausálni a DAG-ot {dag_id}. Status: {response.status_code} - {response.text}")


def unpause_airflow_dag(dag_id: str):
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}"
    payload = {
        "is_paused": False
    }
    response = requests.patch(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), json=payload)

    if response.status_code == 200:
        print(f"DAG {dag_id} sikeresen unpaused állapotba került.")
    else:
        print(f"Nem sikerült unpause-olni a DAG-ot {dag_id}. Status: {response.status_code} - {response.text}")