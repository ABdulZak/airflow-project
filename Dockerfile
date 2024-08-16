FROM apache/airflow:2.9.3-python3.11

ADD requirements.txt .
RUN pip install -r requirements.txt