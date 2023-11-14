#!/bin/bash

rm -rf ./tmp

sh -c "
cat << EOF > ./.env
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW_UID=$(id -u)
AIRFLOW_GID=0
_PIP_ADDITIONAL_REQUIREMENTS=xlsx2csv==0.7.8 faker==8.12.1 py2neo==2021.2.3 apache-airflow-providers-mongo==2.3.1 apache-airflow-providers-docker==2.1.0
EOF
"

docker-compose up airflow-init
docker-compose down
# If airflow-init container is not removed, we will stop and remove it manually
if [ "$(docker ps -aq -f status=exited -f name=airflow-init)" ]; then
    docker stop airflow-init
    docker rm airflow-init
fi

docker-compose up -d

# Add connection to airflow
docker exec -it airflow_webserver airflow connections add 'deng_staging' --conn-uri 'postgresql://postgres:@postgres:5432/deng_staging'
docker exec -it airflow_webserver airflow connections add 'deng_production' --conn-uri 'postgresql://postgres:@postgres:5432/deng_production'

# Create deng_staging and deng_production database
docker exec -it airflow_postgres psql -U airflow -c "CREATE DATABASE IF NOT EXISTS deng_staging;"
docker exec -it airflow_postgres psql -U airflow -c "CREATE DATABASE IF NOT EXISTS deng_production;"

docker build -t nmngo248/star-schema:latest ./star_schema