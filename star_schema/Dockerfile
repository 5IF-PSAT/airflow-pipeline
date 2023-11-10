FROM python:3.9.13

ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_USERNAME=postgres
ENV POSTGRES_PASSWORD=None
ENV POSTGRES_DB=deng

# Path: /app
RUN mkdir -p /app
WORKDIR /app

COPY . /app/
RUN pip install -r requirements.txt

# Path: /app
CMD ["python", "main.py"]