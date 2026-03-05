FROM apache/airflow:2.7.2

# A felhasználó átváltása root-ra, ha esetleg rendszerfüggőség kell
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Visszaváltás az airflow felhasználóra a pip csomagok telepítéséhez
USER airflow

# Átmásoljuk a te specifikus csomagjaidat
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt