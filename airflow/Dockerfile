FROM python:3.12-slim

# Airflow env
ENV AIRFLOW_HOME=/airflow
ENV PYTHONUNBUFFERED=1

# System dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libffi-dev \
        libssl-dev \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Create airflow folders
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins

WORKDIR /

# Default command
CMD ["airflow", "version"]
