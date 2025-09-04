# ingestor/Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas psycopg2-binary

COPY ingestion.py /app/ingestion.py
