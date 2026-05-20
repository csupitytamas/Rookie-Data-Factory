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
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# KÓD BEÁGYAZÁSA: Bemásoljuk a DAG-okat és a Pluginokat az Image-be
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root airflow/plugins /opt/airflow/plugins

# ÚTVONALAK FIXÁLÁSA
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/plugins"
COPY airflow/get_timezone.py /opt/airflow/get_timezone.py