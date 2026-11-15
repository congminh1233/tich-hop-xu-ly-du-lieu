FROM apache/airflow:3.1.0-python3.12
RUN pip install --no-cache-dir "apache-airflow==3.1.0" \
    "apache-airflow-providers-docker==4.4.3" \
    "apache-airflow-providers-postgres==6.3.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt"
