FROM apache/airflow:2.11.0-python3.10

# 1) airflow 유저로 전환 (pip 설치는 root가 아닌 airflow 유저에서)
USER airflow

# 2) requirements.txt 복사
COPY requirements.txt /requirements.txt

# 3) pip 업그레이드 및 requirements 설치
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt