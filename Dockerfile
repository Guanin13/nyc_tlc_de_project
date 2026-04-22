FROM apache/airflow:3.2.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

COPY src / /opt/airflow/dags/
COPY dbt_project /opt/airflow/dbt_project/
COPY dags /opt/airflow/dags/
COPY logs /opt/airflow/logs/
COPY plugins /opt/airflow/plugins/
