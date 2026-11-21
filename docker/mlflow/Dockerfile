# MLflow tracking server + model registry (Apache-2.0) with the PostgreSQL driver and the S3 client
# needed for a database backend store and an S3 artifact store.
FROM ghcr.io/mlflow/mlflow:v2.22.0
RUN pip install --no-cache-dir psycopg2-binary==2.9.10 boto3==1.37.38
