from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
from faker import Faker
import random
import pandas as pd
import os

# File path for CSV
path = "/opt/airflow/faker_output/fake_sensor.csv"
ist = timezone(timedelta(hours=5, minutes=30))
# Faker instance
fake = Faker()

# Define the sensor data generation function
def faker_to_csv():
    data = []
    for i in range(10):
        loaded_time = datetime.now(ist)
        loaded_time = loaded_time.replace(microsecond=0)
        f_sensor = {
            "Sensor_id": f"Sensor_{random.randint(1000, 9999)}",
            "location": fake.city(),
            "timestamp": loaded_time,
            "temp_celsius": round(random.uniform(-10, 60), 1),
            "humidity_percent": round(random.uniform(30.0, 90.0), 2),
            "rain_mm": round(random.uniform(200, 2000), 2)
        }
        data.append(f_sensor)
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.isfile(path):
        df.to_csv(path, mode='w', header=True, index=False)
    else:
        df.to_csv(path, mode='a', header=False, index=False)

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='fake_to_csv',
    default_args=default_args,
    description='Generates fake sensor data every minute',
    schedule_interval='* * * * *',  # every 1 minute
    start_date=datetime(2025, 6, 8),
    catchup=False,
    tags=['faker', 'sensor', 'csv']
) as dag:

    generate_sensor_data = PythonOperator(
        task_id='generate_fake_sensor_data',
        python_callable=faker_to_csv
    )

    generate_sensor_data
