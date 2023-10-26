FROM python:3.8.14

RUN mkdir -p /app
WORKDIR /app

COPY . /app/
RUN pip install -r requirements.txt

EXPOSE 8001
ENTRYPOINT ["python", "main.py"]