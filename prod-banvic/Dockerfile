FROM apache/airflow:2.11.0-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    python3-venv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN python3 -m venv /home/airflow/meltano_env && \
    /home/airflow/meltano_env/bin/pip install --no-cache-dir --upgrade pip && \
    /home/airflow/meltano_env/bin/pip install --no-cache-dir "meltano"

RUN pip install --no-cache-dir apache-airflow-providers-postgres

COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root . /opt/airflow/prod-banvic/

WORKDIR /opt/airflow/prod-banvic
RUN /home/airflow/meltano_env/bin/meltano install