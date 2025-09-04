import os
import time
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from statsmodels.tsa.holtwinters import ExponentialSmoothing

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "healthdb")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "$amuelGitau1")

YEARS = list(range(2007, 2023))  # 2007–2022 inclusive
FORECAST_HORIZON = 3             # forecast next 3 years (2023–2025)

def connect():
    for i in range(20):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS
            )
            conn.autocommit = True
            return conn
        except Exception as e:
            print(f"[modeler] DB not ready ({i+1}/20): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Postgres")

def ensure_schema(cur):
    # Where we EXPECT yearly time series if you later load real per-year data
    cur.execute("""
    CREATE TABLE IF NOT EXISTS disease_cases_yearly (
        disease TEXT NOT NULL,
        year INT NOT NULL,
        cases INT NOT NULL,
        source TEXT NOT NULL DEFAULT 'synthetic',
        PRIMARY KEY (disease, year)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS disease_forecasts (
        disease TEXT NOT NULL,
        year INT NOT NULL,
        forecast_cases NUMERIC(18,2),
        lower_ci NUMERIC(18,2),
        upper_ci NUMERIC(18,2),
        method TEXT NOT NULL,
        data_source TEXT NOT NULL,     -- 'real' or 'synthetic'
        created_at TIMESTAMP DEFAULT now(),
        PRIMARY KEY (disease, year)
    );
    """)

def table_has_real_yearly_data(cur):
    cur.execute("SELECT COUNT(*) FROM disease_cases_yearly WHERE source <> 'synthetic';")
    return cur.fetchone()[0] > 0

def build_synthetic_if_empty(conn):
    """Synthesize a per-year series by distributing totals with a gentle trend + noise.
       This is only for pipeline wiring and should be replaced when real yearly data is available.
    """
    with conn.cursor() as cur:
        # Check if we already have rows (real or synthetic)
        cur.execute("SELECT COUNT(*) FROM disease_cases_yearly;")
        if cur.fetchone()[0] > 0:
            print("[modeler] disease_cases_yearly already populated — skipping synthetic fill.")
            return

        print("[modeler] No yearly data found. Building synthetic series from aggregates (kenya_outbreaks_2007_2022).")
        cur.execute("SELECT disease, cases FROM kenya_outbreaks_2007_2022 ORDER BY cases DESC;")
        totals = cur.fetchall()

        rows = []
        rng = np.random.default_rng(42)
        for disease, total_cases in totals:
            # create a gentle upward or flat trend
            base = total_cases / len(YEARS)
            slope = (0.15 if total_cases > 10000 else 0.05) * base / len(YEARS)  # bigger slope for high-burden diseases
            yearly = []
            remaining = total_cases

            # generate a provisional series with trend + noise, then scale to match exact total
            for i, y in enumerate(YEARS):
                val = max(0.0, base + slope * i + rng.normal(0, base * 0.1))
                yearly.append(val)
            scale = total_cases / max(1e-9, sum(yearly))
            yearly = [max(0, int(round(v * scale))) for v in yearly]

            # correct rounding drift to match total exactly
            diff = total_cases - sum(yearly)
            if diff != 0:
                yearly[-1] += diff

            for y, c in zip(YEARS, yearly):
                rows.append((disease, y, int(c), 'synthetic'))

        execute_values(cur, """
            INSERT INTO disease_cases_yearly (disease, year, cases, source)
            VALUES %s
            ON CONFLICT (disease, year) DO NOTHING;
        """, rows)
        print(f"[modeler] Inserted synthetic yearly series for {len(set(r[0] for r in rows))} diseases.")

def fetch_timeseries(conn):
    """Return a dict: {disease: DataFrame(year, cases)} using disease_cases_yearly."""
    df = pd.read_sql_query("""
        SELECT disease, year, cases, source
        FROM disease_cases_yearly
        ORDER BY disease, year;
    """, conn)
    out = {}
    for disease, g in df.groupby('disease'):
        out[disease] = g[['year','cases']].reset_index(drop=True)
    return out, ('real' if (df['source'] != 'synthetic').any() else 'synthetic')

def fit_and_forecast(series_df, horizon=FORECAST_HORIZON):
    """Fit Exponential Smoothing (trend only) and forecast horizon years."""
    s = series_df.set_index('year')['cases'].astype(float).sort_index()
    # If too short, fall back to naive mean
    if len(s) < 4 or s.sum() == 0:
        f = [float(s.mean())] * horizon
        return np.array(f), np.array(f), np.array(f), 'naive-mean'

    try:
        model = ExponentialSmoothing(
            s, trend='add', seasonal=None, initialization_method='estimated'
        ).fit(optimized=True)
        fc = model.forecast(horizon)
        # crude CI using residual std
        resid = s - model.fittedvalues
        sd = resid.std(ddof=1) if len(resid) > 1 else (0.15 * s.mean() if s.mean() > 0 else 1.0)
        lower = fc - 1.96 * sd
        upper = fc + 1.96 * sd
        return fc.values, lower.values, upper.values, 'holt-winters-additive'
    except Exception as e:
        # fallback
        f = [float(s.mean())] * horizon
        return np.array(f), np.array(f), np.array(f), f'fallback-mean ({e})'

def write_forecasts(conn, forecasts_rows):
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO disease_forecasts
                (disease, year, forecast_cases, lower_ci, upper_ci, method, data_source)
            VALUES %s
            ON CONFLICT (disease, year) DO UPDATE SET
                forecast_cases = EXCLUDED.forecast_cases,
                lower_ci = EXCLUDED.lower_ci,
                upper_ci = EXCLUDED.upper_ci,
                method = EXCLUDED.method,
                data_source = EXCLUDED.data_source;
        """, forecasts_rows)
    print(f"[modeler] Wrote {len(forecasts_rows)} forecast rows to disease_forecasts.")

def main():
    with connect() as conn, conn.cursor() as cur:
        ensure_schema(cur)

    # Seed synthetic yearly series if table empty
    with connect() as conn:
        build_synthetic_if_empty(conn)

    # Train & forecast
    with connect() as conn:
        ts_map, data_source = fetch_timeseries(conn)

        all_rows = []
        for disease, df in ts_map.items():
            fc, lo, hi, method = fit_and_forecast(df, horizon=FORECAST_HORIZON)
            start_year = int(df['year'].max()) + 1
            for i, y in enumerate(range(start_year, start_year + FORECAST_HORIZON)):
                all_rows.append((
                    disease, int(y),
                    float(fc[i]), float(lo[i]), float(hi[i]),
                    method, data_source
                ))
        write_forecasts(conn, all_rows)

if __name__ == "__main__":
    main()
