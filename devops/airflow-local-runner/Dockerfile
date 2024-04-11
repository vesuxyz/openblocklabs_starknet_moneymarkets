FROM apache/airflow:2.8.1

WORKDIR /opt/airflow

USER airflow

COPY requirements.txt requirements.txt
RUN python3 -m pip install -r requirements.txt

ADD config/init.sh .
