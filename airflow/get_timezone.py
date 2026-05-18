import psycopg2
import os

""" A szkript lekéri a rendszerbeállítások közül az aktuális időzónát az Airflow adatbázisából. """

# Lekérdezi az Airflow metaadatbázisából a felhasználó által beállított időzónát. Ha nem található, alapértelmezés szerint 'UTC'-t ad vissza.
def get_airflow_timezone():
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow_password"
        )
        cur = conn.cursor()

        # Lekérdezés a system_settings táblán a timezone mező kinyeréséhez.
        cur.execute("SELECT timezone FROM system_settings LIMIT 1;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row[0]
    except Exception as e:
        print(f"Error fetching timezone: {e}")
    return "UTC"

if __name__ == "__main__":
    print(get_airflow_timezone())
