FROM python:3.9.13

ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_USERNAME=postgres
ENV POSTGRES_PASSWORD=None
ENV POSTGRES_DB_STAGING=deng_staging
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

# Path: /app
RUN mkdir -p /app
WORKDIR /app

COPY . /app/

RUN chmod +x set_env.sh
CMD [ "/app/set_env.sh" ]

RUN pip install -r requirements.txt

# Path: /app
# CMD ["python", "main.py"]