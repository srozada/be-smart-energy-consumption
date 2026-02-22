FROM apache/airflow:2.10.4

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    && apt-get clean

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir \
    entsoe-py \
    psycopg2-binary \
    pandas \
    sqlalchemy