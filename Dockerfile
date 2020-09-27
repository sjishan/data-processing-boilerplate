FROM python:3.7-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y libpq-dev gcc

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN pip install -e .