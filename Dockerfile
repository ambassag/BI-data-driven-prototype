FROM apache/airflow:2.8.1-python3.11

# Installer dépendances système (MySQL client etc.)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Copier requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Installer paquets Python en tant qu'utilisateur airflow
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
