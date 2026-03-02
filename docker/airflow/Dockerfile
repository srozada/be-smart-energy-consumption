FROM apache/airflow:2.10.4

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    && apt-get clean

USER airflow

RUN pip install --no-cache-dir --upgrade \
    entsoe-py \
    psycopg2-binary \
    pandas \
    SQLAlchemy==1.4.46


