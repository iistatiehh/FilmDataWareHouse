# Dockerfile
FROM apache/airflow:2.7.2
USER airflow
RUN pip install --user pymysql
USER airflow
