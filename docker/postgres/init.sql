-- One PostgreSQL instance, three databases: Airflow metadata, MLflow backend store, and the
-- monitoring tables the DAG writes (data quality, drift, decisions, realised performance).
CREATE DATABASE mlflow;
CREATE DATABASE mlops;
