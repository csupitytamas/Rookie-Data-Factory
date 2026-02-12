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
        
def get_task_logs(dag_id, dag_run_id, task_id, try_number=1):
    """
    Lekéri a logokat az Airflow API-ból.
    """
    # Ha "latest" a run_id, akkor meg kell keresni a legutolsó futást (opcionális logika)
    if dag_run_id == "latest":
        # Itt lehetne egy extra hívás a /dags/{dag_id}/dagRuns?limit=1 -re
        # De egyszerűbb, ha a frontend mindig pontos ID-t küld.
        pass

    endpoint = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
    url = f"{AIRFLOW_URL}/api/v1/{endpoint}"
    
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH)
        
        if response.status_code == 200:
            # Az Airflow JSON-t ad vissza: {"content": "...", "continuation_token": ...}
            # Vagy sima textet, ha úgy van beállítva.
            try:
                return response.json().get('content', 'No log content found in JSON.')
            except:
                return response.text
        else:
            return f"Error fetching logs from Airflow (Status: {response.status_code}): {response.text}"
            
    except Exception as e:
        return f"Backend Error: {str(e)}"