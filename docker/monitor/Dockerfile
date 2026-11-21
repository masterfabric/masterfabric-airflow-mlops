# Feast (feature server, UI) and Evidently UI, from the same pinned requirements the Airflow tasks use.
FROM python:3.12-slim
ENV DO_NOT_TRACK=1 EVIDENTLY_DISABLE_TELEMETRY=1 FEAST_USAGE=False PYTHONPATH=/repo
COPY monitor/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /repo
