# Online churn scoring API: serves the model version that holds the registry alias "champion".
FROM python:3.12-slim
WORKDIR /app
COPY serving/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY mlops /app/mlops
COPY serving /app/serving
EXPOSE 8000
CMD ["uvicorn", "serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
