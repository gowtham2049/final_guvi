from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine,text
from urllib.parse import quote_plus
from faker import Faker
import pandas as pd
import random
from datetime import datetime, timezone, timedelta


# Initialize Faker
fake = Faker()
ist = timezone(timedelta(hours=5, minutes=30))

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Connection string setup
password = "mysqlpass@123"
encoded_password = quote_plus(password)
MYSQL_CONN_STR = f"mysql+mysqlconnector://root:{encoded_password}@host.docker.internal:3306/db_guvi"
#MYSQL_CONN_STR = "mysql+mysqlconnector://root:password@host.docker.internal:3306/airflow_db"

def run_stored_procedure():
    engine = create_engine(MYSQL_CONN_STR)
    with engine.begin() as conn:
        conn.execute(text("CALL sp_update_last_loaded_time();"))


def faker_to_mysql():
    station_data = []
    for i in range(15):
        min_temp = round(random.uniform(-10, 30), 1)
        max_temp = round(random.uniform(min_temp + 1, min_temp + 15), 1)
        #loaded_time = datetime.now().replace(microsecond=0)
        loaded_time = datetime.now(ist).replace(microsecond=0)


        f_station = {
            "station_id": f"S{random.randint(1000, 9999)}",
            "date": fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
            "min_temp": min_temp,
            "max_temp": max_temp,
            "avg_humidity": round(random.uniform(30.0, 90.0), 2),
            "precipitation_mm": round(random.uniform(0, 20), 2),
            "loaded_time": loaded_time
        }
        station_data.append(f_station)
    df = pd.DataFrame(station_data)
    df['date'] = pd.to_datetime(df['date'])
    
    # Save to MySQL
    engine = create_engine(MYSQL_CONN_STR)
    df.to_sql('station_data', con=engine, if_exists='append', index=False)

with DAG(
    dag_id='faker_to_mysql',
    default_args=default_args,
    schedule_interval= "0 14 * * *",
    catchup=False,
    tags=['mysql', 'sqlalchemy'],
) as dag:

    run_sp_task = PythonOperator(
        task_id='run_stored_procedure',
        python_callable=run_stored_procedure
    )

    insert_task = PythonOperator(
        task_id='insert_with_sqlalchemy',
        python_callable=faker_to_mysql
    )

    run_sp_task >> insert_task
