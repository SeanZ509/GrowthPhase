from datetime import datetime, timedelta
import os, zipfile, pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DATASET_SLUG = "peopledatalabssf/free-7-million-company-dataset" 
DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
RAW_ZIP = os.path.join(DATA_DIR, "companies.zip")
RAW_CSV = os.path.join(DATA_DIR, "companies.csv")
CLEAN_CSV = os.path.join(DATA_DIR, "companies_clean_sample.csv")
KAGGLE_CONFIG_DIR = os.getenv("KAGGLE_CONFIG_DIR", "/opt/airflow/secrets")

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def unzip_file():
    if not os.path.exists(RAW_ZIP):
        raise FileNotFoundError(f"Zip not found: {RAW_ZIP}")
    with zipfile.ZipFile(RAW_ZIP, "r") as z:
        csv_candidates = [f for f in z.namelist() if f.lower().endswith(".csv")]
        if not csv_candidates:
            raise RuntimeError("No CSV found in Kaggle zip.")
        target = csv_candidates[0]
        with z.open(target) as f_in, open(RAW_CSV, "wb") as f_out:
            f_out.write(f_in.read())

def quick_clean_sample():
    # Read a chunk so this runs fast
    df_iter = pd.read_csv(RAW_CSV, on_bad_lines="skip", low_memory=False, chunksize=200_000)
    df = next(df_iter)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "domain" in df.columns:
        df = df[df["domain"].notna()]
    keep = {"company_name","domain","industry","location","founded_year","employees_range"}
    cols = [c for c in df.columns if c in keep]
    if cols:
        df = df[cols]
    df.to_csv(CLEAN_CSV, index=False)

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="growthphase_ingest_etl",
    start_date=datetime(2025, 11, 1),
    schedule_interval="@once",  # run once for dev
    catchup=False,
    default_args=default_args,
    tags=["growth","kaggle","etl"],
) as dag:

    make_dirs = PythonOperator(
        task_id="ensure_dirs",
        python_callable=ensure_dirs,
    )

    download_from_kaggle = BashOperator(
        task_id="download_kaggle_dataset",
        bash_command=(
            'export KAGGLE_CONFIG_DIR="{{ params.kaggle_dir }}"; '
            'kaggle datasets download -d {{ params.slug }} -p "{{ params.data_dir }}" -o && '
            'mv "{{ params.data_dir }}"/*.zip "{{ params.raw_zip }}"'
        ),
        params={
            "slug": DATASET_SLUG,
            "data_dir": DATA_DIR,
            "raw_zip": RAW_ZIP,
            "kaggle_dir": KAGGLE_CONFIG_DIR,
        },
    )

    unzip = PythonOperator(
        task_id="unzip_to_csv",
        python_callable=unzip_file,
    )

    clean_sample = PythonOperator(
        task_id="quick_clean_sample",
        python_callable=quick_clean_sample,
    )

    make_dirs >> download_from_kaggle >> unzip >> clean_sample