FROM apache/airflow:2.11.2-python3.11

ARG AIRFLOW_VERSION=2.11.2
ARG PYTHON_VERSION=3.11

# Certificado de AWS para TLS con DocumentDB (y RDS).
USER root
RUN mkdir -p /opt/airflow/certs \
    && curl -sSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
       -o /opt/airflow/certs/global-bundle.pem \
    && chmod 644 /opt/airflow/certs/global-bundle.pem
USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "Faker==37.0.0" \
    "PyMySQL==1.1.2" \
    "pymongo==4.16.0" \
    boto3 \
    redshift_connector \
    && pip check

# dbt vive en su propio entorno para no chocar con las dependencias de Airflow.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
      "dbt-core==1.9.4" \
      "dbt-redshift>=1.9.0,<1.10" \
    && /home/airflow/dbt-venv/bin/pip check \
    && ln -s /home/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
