# Disease Surveillance & Prediction Pipeline (Kenya 2007–2022)

## 🚀 Overview
This project ingests disease surveillance data (2007–2022, Kenya) into a Postgres database, 
then visualizes insights and forecasts using Grafana.

## ⚡ Tech Stack
- Docker + Docker Compose
- PostgreSQL (data store)
- Adminer (database admin UI)
- Grafana (dashboards)
- Python Ingestor (CSV → Postgres)
- Python Modeler (synthetic yearly series + Holt-Winters forecasting)

## 📂 Project Structure
disease_surv_project/
├── data/ # CSV data
│ └── kenya_outbreaks_2007_2022.csv
├── ingestor/ # Data ingestion service
│ ├── Dockerfile
│ └── ingestion.py
├── modeler/ # Forecasting service
│ ├── Dockerfile
│ └── model_train.py
├── docker-compose.yml
├── .env.example
└── README.md# disease-surveillance-kenya
