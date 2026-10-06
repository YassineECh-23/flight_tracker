from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
from io import StringIO
from dotenv import load_dotenv
from supabase import create_client, Client

# On s'assure de charger le .env qui est à la racine du projet
load_dotenv(os.path.join(os.getenv("AIRFLOW_HOME", ""), "..", ".env"))

def process_and_load_ntsb():
    # 1. Connexion Supabase
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    # 2. Simulation du CSV NTSB téléchargé (données brutes)
    mock_csv_data = """EventDate,AircraftCategory,PhaseOfFlight,ProbableCause,Latitude,Longitude
    2023-01-15,Airplane,TAKEOFF,Engine Failure,40.6413,-73.7781
    2023-03-22,Helicopter,MANEUVERING,Pilot Error,34.0522,-118.2437
    """
    
    # 3. Nettoyage et insertion
    reader = csv.DictReader(StringIO(mock_csv_data.strip()))
    inserted_count = 0
    
    for row in reader:
        # Mapping vers dim_accidents
        record = {
            "date_accident": row["EventDate"],
            "type_appareil": row["AircraftCategory"],
            "phase_vol": row["PhaseOfFlight"],
            "cause": row["ProbableCause"],
            "lat": float(row["Latitude"]),
            "lon": float(row["Longitude"])
        }
        # Insert dans Supabase
        supabase.table('dim_accidents').insert(record).execute()
        inserted_count += 1
        
    print(f"✅ {inserted_count} accidents NTSB insérés dans Supabase.")

# Configuration du DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ntsb_daily_ingestion',
    default_args=default_args,
    description='Ingestion quotidienne des accidents NTSB vers Supabase',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['flight_tracker'],
) as dag:

    ingest_task = PythonOperator(
        task_id='process_and_load_ntsb_task',
        python_callable=process_and_load_ntsb
    )