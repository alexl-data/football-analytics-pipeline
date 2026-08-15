FROM apache/airflow:2.10.4-python3.11

# Constrains apache-airflow itself (and all providers) to versions compatible with
# the base image's Airflow version. Without this, pip's dependency resolver can
# silently upgrade Airflow to a newer major version while installing the extra
# packages below, breaking the image (see docs/architecture.md).
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"
