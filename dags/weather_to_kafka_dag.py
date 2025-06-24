from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from airflow.models import Variable

# --- Weather and Kafka settings ---
CITY_NAME = "chennai"
API_KEY = Variable.get("api_key")
KAFKA_BOOTSTRAP_SERVERS = 'host.docker.internal:9092'
KAFKA_TOPIC = 'weather-topic'
#host.docker.internal:9092
# --- Function to fetch weather data ---
def get_weather_data():
    response = requests.get(
        f"https://api.openweathermap.org/data/2.5/weather?q={CITY_NAME}&appid={API_KEY}"
    )
    if response.status_code == 200:
        data = response.json()
        data["ingestion_time"] = datetime.utcnow().isoformat() + "Z"
        return {
            "location_timestamp": data["ingestion_time"],
            "city": data["name"],
            "latitude": data["coord"]["lat"],
            "longitude": data["coord"]["lon"],
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "wind_speed": data["wind"]["speed"],
            "weather_description": data["weather"][0]["description"],
        }
    else:
        print("API error:", response.status_code)
        return None

# --- Function to send data to Kafka ---
def send_to_kafka(data):
    from kafka import KafkaProducer  # Imported here to avoid DAG parse-time errors
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(KAFKA_TOPIC, value=data)
    producer.flush()
    producer.close()

# --- ETL function to run as Airflow task ---
def run_weather_etl():
    data = get_weather_data()
    if data:
        send_to_kafka(data)

# --- Airflow DAG setup ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(
    dag_id='weather_to_kafka_dag',
    default_args=default_args,
    description='Fetch Chennai weather data and send to Kafka every minute',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    start_date=datetime(2025, 5, 19),
    catchup=False,
    tags=['weather', 'kafka', 'etl'],
) as dag:

    etl_task = PythonOperator(
        task_id='fetch_weather_and_send_to_kafka',
        python_callable=run_weather_etl,
    )

    etl_task

