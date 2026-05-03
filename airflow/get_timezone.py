import psycopg2
import os

def get_airflow_timezone():
    try:
        # Kapcsolódás az adatbázishoz (ugyanaz, amit az Airflow is használ)
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow_password"
        )
        cur = conn.cursor()
        # Lekérjük a beállított időzónát
        cur.execute("SELECT timezone FROM system_settings LIMIT 1;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            return row[0]
    except Exception as e:
        print(f"Error fetching timezone: {e}")
    
    return "UTC" # Fallback

if __name__ == "__main__":
    print(get_airflow_timezone())
