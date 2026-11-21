# Apache Airflow 2.10.5 (Apache-2.0) with the ML stack the DAG tasks import.
# Airflow is pinned in the same pip call so no dependency can silently up- or downgrade it.
FROM apache/airflow:2.10.5-python3.12
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/requirements.txt
