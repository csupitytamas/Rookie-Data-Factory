import requests
import time
from requests.auth import HTTPBasicAuth
import os

""" 
Ez a modul az Airflow API-val való kommunikációért felelős kliens, 
amely lehetővé teszi a DAG-ok vezérlését és a logok lekérését. 
"""

AIRFLOW_URL = os.environ.get("AIRFLOW_URL")
USERNAME = os.environ.get("AIRFLOW_USERNAME")
PASSWORD = os.environ.get("AIRFLOW_PASSWORD")
AUTH = HTTPBasicAuth(USERNAME, PASSWORD)


# Megállítja a megadott Airflow DAG futását.
def pause_airflow_dag(dag_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}"
    payload = {
        "is_paused": True
    }
    response = requests.patch(url, auth=AUTH, json=payload)
    if response.status_code == 200:
        print(f"DAG {dag_id} successfully paused.")
    else:
        print(f"Failed to pause DAG {dag_id}. Status: {response.status_code} - {response.text}")

# Elindítja a megadott Airflow DAG-ot, hogy futtatható legyen.
def unpause_airflow_dag(dag_id: str):
    url = f"{AIRFLOW_URL}/dags/{dag_id}"
    payload = {
        "is_paused": False
    }
    response = requests.patch(url, auth=AUTH, json=payload)
    if response.status_code == 200:
        print(f"DAG {dag_id} successfully unpaused.")
    else:
        print(f"Failed to unpause DAG {dag_id}. Status: {response.status_code} - {response.text}")
        
# Lekéri egy konkrét task példány logjait az Airflow API-ból.
def get_task_logs(dag_id, dag_run_id, task_id, try_number=1):
    if dag_run_id == "latest":
        pass
    endpoint = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
    url = f"{AIRFLOW_URL}/{endpoint}"
    try:
        response = requests.get(url, auth=AUTH)
        if response.status_code == 200:
            try:
                return response.json().get('content', response.text)
            except:
                return response.text
        else:
            return f"Error fetching logs from Airflow (Status: {response.status_code}): {response.text}"
    except Exception as e:
        return f"Backend Error: {str(e)}"
    
# Lekéri a DAG futáshoz tartozó összes task logját, sorrendbe rendezve.
def get_dag_run_logs(dag_id: str, execution_date: str) -> str:
    try:
        response = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns",
            auth=AUTH,
            params={"limit": 1, "order_by": "-execution_date"}
        )
        response.raise_for_status()
        runs = response.json().get("dag_runs", [])
        if not runs:
            return "No runs found for this pipeline."
        latest_run_id = runs[0]["dag_run_id"]
        ti_response = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{latest_run_id}/taskInstances",
            auth=AUTH
        )
        # Ellenőrizzük a task példányok lekérésének sikerességét.
        ti_response.raise_for_status()
        task_instances = ti_response.json().get("task_instances", [])
        if not task_instances:
            return "No task instances found."
        task_instances.sort(key=lambda x: x.get('start_date') or '9999-12-31')
        full_log_output = []
        full_log_output.append(f"ALL LOGS FOR RUN ID: {latest_run_id}\n")
        for i, ti in enumerate(task_instances):
            task_id = ti["task_id"]
            state = ti["state"]
            try_number = ti["try_number"]
            header = f"\n{'='*60}\n"
            header += f"STEP {i+1}: {task_id.upper()} (State: {state})\n"
            header += f"{'='*60}\n"
            full_log_output.append(header)

            # Lekérjük az adott task konkrét log tartalmát az Airflow API-tól.
            try:
                log_response = requests.get(
                    f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{latest_run_id}/taskInstances/{task_id}/logs/{try_number}",
                    auth=AUTH
                )
                if log_response.status_code == 200:
                    try:
                        content = log_response.json().get('content', log_response.text)
                    except:
                        content = log_response.text
                    full_log_output.append(content)
                else:
                    full_log_output.append(f"[Error fetching logs: {log_response.status_code}]")
            except Exception as e:
                full_log_output.append(f"[Exception fetching log: {str(e)}]")
            full_log_output.append("\n")
        return "".join(full_log_output)
    except Exception as e:
        return f"Error connecting to Airflow: {str(e)}"

# Lekéri a legutóbbi DAG-ok futást és azok állapotát a dashboardhoz.
def get_dag_runs_history(dag_id: str, limit: int = 10):
    try:
        response = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns",
            auth=AUTH,
            params={"limit": limit, "order_by": "-execution_date"}
        )
        response.raise_for_status()
        dag_runs = response.json().get("dag_runs", [])
        history = []

        # Végigmegyünk a futásokon és begyűjtjük a hozzájuk tartozó taskok részletes állapotát.
        for run in dag_runs:
            run_id = run["dag_run_id"]
            ti_response = requests.get(
                f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
                auth=AUTH
            )
            ti_response.raise_for_status()
            task_instances = ti_response.json().get("task_instances", [])
            tasks = []
            for ti in task_instances:
                tasks.append({
                    "task_id": ti["task_id"],
                    "state": ti["state"],
                    "start_date": ti.get("start_date"),
                    "end_date": ti.get("end_date")
                })
            tasks.sort(key=lambda x: x["task_id"])
            history.append({
                "run_id": run_id,
                "state": run["state"],
                "execution_date": run["execution_date"],
                "tasks": tasks
            })
        return history
    except Exception as e:
        print(f"Error fetching DAG history for {dag_id}: {e}")
        return []

# Megpróbálja elindítani a DAG-ot, szükség esetén retry megoldással.
def trigger_airflow_dag_with_retry(dag_id: str, retries=5, delay=10):
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"

    # Újrapróbálkozási ciklus arra az esetre, ha az Airflow még nem frissítette a DAG listáját.
    for i in range(retries):
        try:
            unpause_airflow_dag(dag_id)
            response = requests.post(url, auth=AUTH, json={})
            if response.status_code in [200, 201]:
                print(f" DAG {dag_id} successfully triggered on try {i+1}.")
                return response.json()
            if response.status_code == 404:
                print(f" Airflow doesn't see {dag_id} yet. Waiting... ({i+1}/{retries})")
                time.sleep(delay)
            else:
                break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(delay)
    return None

# Ellenőrzi az Airflow rendszer állapotát.
def get_airflow_health():
    try:
        url = f"{AIRFLOW_URL}/health"
        response = requests.get(url, auth=AUTH, timeout=5)
        if response.status_code == 200:
            data = response.json()
            metadatabase_status = data.get("metadatabase", {}).get("status")
            scheduler_status = data.get("scheduler", {}).get("status")
            if metadatabase_status == "healthy" and scheduler_status == "healthy":
                return {"status": "healthy", "scheduler": scheduler_status, "metadatabase": metadatabase_status}
            else:
                return {"status": "degraded", "scheduler": scheduler_status, "metadatabase": metadatabase_status}
        else:
            return {"status": "unavailable", "detail": f"Status code: {response.status_code}"}
    except Exception as e:
        return {"status": "unavailable", "error": str(e)}
