FROM apache/airflow:3.3.2-python3.13

USER root
RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        ca-certificates unixodbc unixodbc-dev gcc g++ msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    python-dotenv \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt"

COPY --chown=airflow:root requirements.txt requirements-dbt.txt /tmp/project-requirements/
RUN python -m venv /opt/airflow/venvs/ingestion \
    && /opt/airflow/venvs/ingestion/bin/pip install --no-cache-dir \
        -r /tmp/project-requirements/requirements.txt \
    && python -m venv /opt/airflow/venvs/dbt \
    && /opt/airflow/venvs/dbt/bin/pip install --no-cache-dir \
        -r /tmp/project-requirements/requirements-dbt.txt

COPY --chown=airflow:root . /opt/airflow/project
ENV PYTHONPATH=/opt/airflow/project
