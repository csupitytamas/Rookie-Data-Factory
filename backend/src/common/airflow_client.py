import requests
from requests.auth import HTTPBasicAuth

# --- KONFIGURÁCIÓ ---
# Egységesítjük: mostantól AIRFLOW_URL-t használunk mindenhol
AIRFLOW_URL = "http://localhost:8080/api/v1"
USERNAME = "airflow"
PASSWORD = "airflow"
AUTH = HTTPBasicAuth(USERNAME, PASSWORD) # Ezt is definiáljuk, hogy használható legyen

# --------------------

def pause_airflow_dag(dag_id):
    # Itt is javítva az AIRFLOW_URL használata
    url = f"{AIRFLOW_URL}/dags/{dag_id}"
    payload = {
        "is_paused": True
    }
    # Az AUTH változót használjuk, amit fent definiáltunk
    response = requests.patch(url, auth=AUTH, json=payload)

    if response.status_code == 200:
        print(f"DAG {dag_id} sikeresen paused állapotba került.")
    else:
        print(f"Nem sikerült pausálni a DAG-ot {dag_id}. Status: {response.status_code} - {response.text}")


def unpause_airflow_dag(dag_id: str):
    url = f"{AIRFLOW_URL}/dags/{dag_id}"
    payload = {
        "is_paused": False
    }
    response = requests.patch(url, auth=AUTH, json=payload)

    if response.status_code == 200:
        print(f"DAG {dag_id} sikeresen unpaused állapotba került.")
    else:
        print(f"Nem sikerült unpause-olni a DAG-ot {dag_id}. Status: {response.status_code} - {response.text}")
        

def get_task_logs(dag_id, dag_run_id, task_id, try_number=1):
    """
    Lekéri a logokat az Airflow API-ból.
    """
    if dag_run_id == "latest":
        pass

    # Figyelem: az endpoint már tartalmazza a 'dags/...'-t, 
    # de az AIRFLOW_URL már tartalmazza az '/api/v1'-et, szóval ne duplázzuk!
    endpoint = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
    
    # Javítva: Csak simán összefűzzük, mert az AIRFLOW_URL vége már '/api/v1'
    url = f"{AIRFLOW_URL}/{endpoint}"
    
    try:
        response = requests.get(url, auth=AUTH)
        
        if response.status_code == 200:
            try:
                # Néha content mezőben van
                return response.json().get('content', response.text)
            except:
                return response.text
        else:
            return f"Error fetching logs from Airflow (Status: {response.status_code}): {response.text}"
            
    except Exception as e:
        return f"Backend Error: {str(e)}"
    

def get_dag_run_logs(dag_id: str, execution_date: str) -> str:
    """
    Lekéri egy adott DAG futás logjait az Airflow API-n keresztül.
    """
    try:
        # 1. A legutóbbi DAG Run lekérése
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
        
        # 2. TaskInstance-ok listázása
        ti_response = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{latest_run_id}/taskInstances",
            auth=AUTH
        )
        ti_response.raise_for_status()
        task_instances = ti_response.json().get("task_instances", [])
        
        # 3. Logok begyűjtése (keressük a hibásat)
        target_task = None
        for ti in task_instances:
            if ti["state"] == "failed":
                target_task = ti
                break
        
        if not target_task and task_instances:
            # Ha nincs hiba, vesszük az utolsót
            target_task = task_instances[-1]
            
        if not target_task:
            return "No task instances found."

        # 4. A konkrét log lekérése
        task_id = target_task["task_id"]
        try_number = target_task["try_number"]
        
        log_response = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{latest_run_id}/taskInstances/{task_id}/logs/{try_number}",
            auth=AUTH
        )
        
        if log_response.status_code == 200:
            return log_response.text
        else:
            return f"Failed to fetch logs. Status: {log_response.status_code}"

    except Exception as e:
        return f"Error connecting to Airflow: {str(e)}"
    
def get_dag_run_logs(dag_id: str, execution_date: str) -> str:
    """
    Lekéri a DAG futáshoz tartozó ÖSSZES task logját, sorrendben.
    Így a felhasználó látja mind a 4 lépést (Extract -> Create -> Transform -> Load).
    """
    try:
        # 1. A legutóbbi DAG Run azonosítása
        # (Ha konkrét run_id-t kapnánk paraméterben, az pontosabb lenne, 
        # de a jelenlegi logikával a legutolsót vesszük, ami a History táblában frissült)
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
        
        # 2. Az összes TaskInstance lekérése ehhez a futáshoz
        ti_response = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{latest_run_id}/taskInstances",
            auth=AUTH
        )
        ti_response.raise_for_status()
        task_instances = ti_response.json().get("task_instances", [])

        if not task_instances:
            return "No task instances found."

        # 3. Rendezés időrendbe (Start Date alapján)
        # Így biztosan Extract -> Create -> Transform -> Load sorrendben jelennek meg
        # A 'start_date' lehet None, ha még nem futott le, ezeket a végére rakjuk
        task_instances.sort(key=lambda x: x.get('start_date') or '9999-12-31')

        # 4. Logok összegyűjtése egy nagy szövegbe
        full_log_output = []
        full_log_output.append(f"ALL LOGS FOR RUN ID: {latest_run_id}\n")

        for i, ti in enumerate(task_instances):
            task_id = ti["task_id"]
            state = ti["state"]
            try_number = ti["try_number"]
            
            # Fejléc generálása minden lépéshez
            header = f"\n{'='*60}\n"
            header += f"STEP {i+1}: {task_id.upper()} (State: {state})\n"
            header += f"{'='*60}\n"
            full_log_output.append(header)

            # Log lekérése az API-tól
            try:
                log_response = requests.get(
                    f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{latest_run_id}/taskInstances/{task_id}/logs/{try_number}",
                    auth=AUTH
                )
                
                if log_response.status_code == 200:
                    # Néha a válasz JSON, néha nyers szöveg
                    try:
                        content = log_response.json().get('content', log_response.text)
                    except:
                        content = log_response.text
                    
                    full_log_output.append(content)
                else:
                    full_log_output.append(f"[Error fetching logs: {log_response.status_code}]")
            
            except Exception as e:
                full_log_output.append(f"[Exception fetching log: {str(e)}]")
            
            full_log_output.append("\n") # Üres sor a taskok között

        return "".join(full_log_output)

    except Exception as e:
        return f"Error connecting to Airflow: {str(e)}"