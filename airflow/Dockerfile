FROM apache/airflow:2.7.2
USER root

# Alapvető rendszerfüggőségek
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# KÓD BEÁGYAZÁSA: Bemásoljuk a DAG-okat és a Pluginokat az Image-be
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./plugins /opt/airflow/plugins

# ÚTVONALAK FIXÁLÁSA: Hogy a 'from logic import...' és társai működjenek
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/plugins"