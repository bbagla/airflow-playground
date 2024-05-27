#!/usr/bin/env bash
set -e

mkdir -p logs plugins

echo "installing virtualenv"
pip3 install virtualenv
virtualenv venv

echo "activating virtualenv"
. venv/bin/activate

echo "install python packages to virtual env"
pip3 install -r requirements.txt

echo "Initializing Airflow"

docker-compose up airflow-init

echo "Creating Database for storage purposes"

postgresContainer=$(docker ps --format "{{.Names}}" | grep postgres)
docker exec -it $postgresContainer psql -U airflow -c  "CREATE DATABASE datastore"
docker exec -it $postgresContainer psql -U airflow -c  "GRANT ALL PRIVILEGES ON DATABASE datastore TO airflow;"

echo "Database datastore created"

docker-compose up -d