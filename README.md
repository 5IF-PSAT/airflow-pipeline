# Project Meteorif
## Description
We create a data pipeline to process the data from the weather and bus delay in the city of Toronto, from 2017 to 2022. 

## Data Source
- [Weather](https://www.kaggle.com/sumanthvrao/daily-climate-time-series-data)
- [Bus Delay](https://open.toronto.ca/dataset/ttc-bus-delay-data/)
- [Bus Route](https://open.toronto.ca/dataset/ttc-routes-and-schedules/)
- [Bus Stop](https://open.toronto.ca/dataset/ttc-routes-and-schedules/)
- [Bus Stop Location](https://open.toronto.ca/dataset/ttc-routes-and-schedules/)
- [Bus Stop Time](https://open.toronto.ca/dataset/ttc-routes-and-schedules/)

## Data Pipeline

## How to run (first time)
1. Clone the repo to your local machine using `git clone https://github.com/5IF-Data-Engineering/deng-project.git`
### Initialize Airflow
1. `docker-compose up airflow-init`. If the exit code is 0, then you can stop the container using `docker-compose down`.

### Set up the MongoDB shard cluster
1. Config Server. 
- Run `docker-compose up -d configsvr1 configsvr2 configsvr3`.
- Run `docker-compose exec -it configsvr1 sh` to enter the Config Server container
- Run `mongosh mongodb://localhost:27017`
