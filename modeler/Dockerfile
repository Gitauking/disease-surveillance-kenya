# modeler/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Light, reliable stack for time series
RUN pip install --no-cache-dir pandas numpy psycopg2-binary statsmodels

COPY model_train.py /app/model_train.py
