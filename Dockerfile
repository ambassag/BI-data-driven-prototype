FROM apache/airflow:2.8.1-python3.11

# Installer dépendances système (en root)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Copier requirements
COPY requirements.txt /

# Repasser à l’utilisateur airflow pour installer les paquets Python
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
FROM apache/airflow:2.8.1-python3.11

# Installer dépendances système (en root)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Copier requirements
COPY requirements.txt /

# Repasser à l’utilisateur airflow pour installer les paquets Python
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
