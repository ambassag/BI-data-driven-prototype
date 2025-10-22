FROM apache/airflow:2.8.1-python3.11

USER root

# dépendances système pour compiler certains paquets (mysqlclient, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-mysql-client \
    default-libmysqlclient-dev \
    python3-dev \
    gcc \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Copier requirements (assure toi que requirements.txt est à la racine du contexte)
COPY requirements.txt /opt/airflow/requirements.txt

# donner la propriété à l'utilisateur airflow (évite problèmes de droits)
RUN chown -R airflow: /opt/airflow

# passer à l'utilisateur airflow et installer les paquets en mode --user
USER airflow

# s'assurer que le dossier .local/bin est dans le PATH et installer les paquets Python
ENV PATH=/home/airflow/.local/bin:$PATH
RUN python -m pip install --upgrade --user pip \
 && python -m pip install --no-cache-dir --user -r /opt/airflow/requirements.txt

# revenir à l'utilisateur airflow (déjà)
USER airflow
