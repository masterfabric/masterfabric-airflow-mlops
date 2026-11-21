# Apache Airflow 2.10.5 (Apache-2.0) with the ML stack the DAG tasks import.
# Airflow is pinned in the same pip call so no dependency can silently up- or downgrade it.
FROM apache/airflow:2.10.5-python3.12
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/requirements.txt
# Feast and Evidently live in their own virtualenv (Feast needs NumPy 2); tasks call /home/airflow/mf-venv/bin/python.
COPY monitor/requirements.txt /tmp/monitor-requirements.txt
RUN python -m venv /home/airflow/mf-venv && PIP_USER=false /home/airflow/mf-venv/bin/pip install --no-cache-dir -r /tmp/monitor-requirements.txt
