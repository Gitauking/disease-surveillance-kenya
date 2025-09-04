# ingestor/ingestion.py
import os
import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "healthdb")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "$amuelGitau1")

CSV_PATH = "/app/data/kenya_outbreaks_2007_2022.csv"

DDL = '''
CREATE TABLE IF NOT EXISTS kenya_outbreaks_2007_2022 (
    disease TEXT PRIMARY KEY,
    cases INTEGER,
    case_proportion_pct NUMERIC(6,2),
    case_rank INTEGER,
    deaths INTEGER,
    death_proportion_pct NUMERIC(6,2),
    death_rank INTEGER,
    outbreak_reports INTEGER,
    mortality_rate_pct NUMERIC(6,2)
);
'''

UPSERT = '''
INSERT INTO kenya_outbreaks_2007_2022
(disease, cases, case_proportion_pct, case_rank, deaths, death_proportion_pct, death_rank, outbreak_reports, mortality_rate_pct)
VALUES %s
ON CONFLICT (disease) DO UPDATE SET
  cases = EXCLUDED.cases,
  case_proportion_pct = EXCLUDED.case_proportion_pct,
  case_rank = EXCLUDED.case_rank,
  deaths = EXCLUDED.deaths,
  death_proportion_pct = EXCLUDED.death_proportion_pct,
  death_rank = EXCLUDED.death_rank,
  outbreak_reports = EXCLUDED.outbreak_reports,
  mortality_rate_pct = EXCLUDED.mortality_rate_pct;
'''

def connect():
    for i in range(20):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
            )
            conn.autocommit = True
            return conn
        except Exception as e:
            print(f"DB not ready yet ({i+1}/20): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Postgres")

def main():
    print("Reading CSV:", CSV_PATH)
    df = pd.read_csv(CSV_PATH)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        rows = [
            (
                r["disease"],
                int(r["cases"]),
                float(r["case_proportion_pct"]),
                int(r["case_rank"]),
                int(r["deaths"]),
                float(r["death_proportion_pct"]),
                int(r["death_rank"]),
                int(r["outbreak_reports"]),
                float(r["mortality_rate_pct"]),
            )
            for _, r in df.iterrows()
        ]
        execute_values(cur, UPSERT, rows)
        print(f"Upserted {len(rows)} rows into kenya_outbreaks_2007_2022")

if __name__ == "__main__":
    main()
