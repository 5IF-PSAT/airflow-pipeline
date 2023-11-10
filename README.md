# Project Meteorif
## Table of Contents
- [Project Meteorif](#project-meteorif)
  - [Table of Contents](#table-of-contents)
- [Introduction](#introduction)
- [Architecture](#architecture)
  - [Airflow](#airflow)
  - [Pipeline API](#pipeline-api)
  - [Star Schema](#star-schema)
- [Pipelines](#pipelines)
  - [Ingestion](#ingestion)
  - [Staging](#staging)
  - [Enrichment](#enrichment)
  - [Production](#production)
- [Future development](#future-development)
- [Project Submission Checklist](#project-submission-checklist)
- [How to run](#how-to-run)
- [References](#references)
- [License](#license)
- [Contact](#contact)
- [Acknowledgements](#acknowledgements)

# Introduction
This project is part of the Foundation of Data Engineering course in [INSA Lyon](https://www.insa-lyon.fr/en/). 
The goal of this project is to build multiple data pipelines to process data from the weather and the bus delay time in Toronto from 2017 to 2022 and make it available for analysis.

# Architecture
The architecture of this project is shown in the figure below. The data is ingested from the [Toronto Open Data](https://open.toronto.ca/) and [Open Weather Map](https://openweathermap.org/). The data is then processed by the data pipeline. The data is then available for analysis.

![Architecture](./assets/architecture.png)

## Airflow
The data pipeline are built using [Apache Airflow](https://airflow.apache.org/). The data pipeline is built using the [DAG](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#dags) concept in Airflow. The DAGs are defined in the `dags` folder.

## Pipeline API
Pipeline API is a Restful API that is built using [Django](https://www.djangoproject.com/) and [Django Rest Framework](https://www.django-rest-framework.org/). The API is used to trigger the data pipeline and to get the status of the data pipeline. The API is defined in the `pipeline_api` folder.
API Documentation can be found [here](https://github.com/5IF-Data-Engineering/pipeline-api/blob/main/README.md#api-documentation).

## Star Schema
The data is stored in the [PostgreSQL](https://www.postgresql.org/) database. The data is stored in the star schema. The star schema is shown in the figure below.

[//]: # (![Star Schema]&#40;./assets/star_schema.png&#41;)

# Pipelines
The data pipeline is divided into 4 stages: Ingestion, Staging, Enrichment, and Production.

## Ingestion

## Staging

## Enrichment

## Production

# Future development
- [ ] Add more data sources
- [ ] Add more data pipelines
- [ ] Add more data analysis
- [ ] Add more data visualization

# Project Submission Checklist
- [ ] Repository with the code, well documented
- [x] Docker-compose file to run the environment
- [ ] Detailed description of the various steps
- [x] Report with the project design steps divided per area
- [x] Example dataset: the project testing should work offline, i.e., you need to have some sample data points.
- [ ] Slides for the project presentation. You can do them too in markdown too.
- [ ] Use airflow + pandas + mongodb + postgres + neo4j
- [x] Using REDIS for speeding up steps
- [x] STAR schema design includes maintenance upon updates
- [ ] Creativity: data viz, serious analysis, performance analysis, extensive cleansing.
- [x] Launching docker containers via airflow to schedule job

# How to run

# References

# License

# Contact
- [Minh NGO](mailto:ngoc-minh.ngo@insa-lyon.fr)

# Acknowledgements
- [INSA Lyon](https://www.insa-lyon.fr/en/)
- [Open Weather Map](https://openweathermap.org/)
- [Toronto Open Data](https://open.toronto.ca/)
- [GitHub Copilot](https://copilot.github.com/)
- [ChatGPT](https://chat.openai.com/)
- [Apache Airflow](https://airflow.apache.org/)
- [Django](https://www.djangoproject.com/)
- [Django Rest Framework](https://www.django-rest-framework.org/)
- [PostgreSQL](https://www.postgresql.org/)