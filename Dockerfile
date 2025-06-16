FROM apache/airflow:2.9.0

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

USER airflow
