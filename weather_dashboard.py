import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from streamlit_autorefresh import st_autorefresh

# Kafka config
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'weather-topic'

# Streamlit config
st.set_page_config(page_title="Kafka Real-Time Weather Dashboard", layout="centered")
st.title("🌤️ Kafka Real-Time Weather Dashboard")

# Auto-refresh every 3 seconds
st_autorefresh(interval=3000, key="kafka_weather_refresh")

# Cache the consumer
@st.cache_resource(show_spinner=False)
def get_kafka_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='weather-dashboard-group',
        consumer_timeout_ms=1000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

# Get or initialize the Kafka consumer
consumer = get_kafka_consumer()

# Initialize session state if not already
if "last_weather" not in st.session_state:
    st.session_state.last_weather = None

# Attempt to get latest message
latest_weather = None
for msg in consumer:
    latest_weather = msg.value
    break

# If new message received, update session state
if latest_weather:
    st.session_state.last_weather = latest_weather

# Display data
if st.session_state.last_weather:
    weather = st.session_state.last_weather
    st.subheader(f"📍 City: {weather['city']}")
    st.metric("🌡️ Temperature", f"{weather['temperature']} °C")
    st.metric("💧 Humidity", f"{weather['humidity']} %")
    st.metric("🔽 Pressure", f"{weather['pressure']} hPa")
    st.metric("🌬️ Wind Speed", f"{weather['wind_speed']} m/s")
    st.text(f"📝 Weather: {weather['weather_description']}")
    st.write("Last updated at:", weather["location_timestamp"])

    df = pd.DataFrame([{
        "lat": weather["latitude"],
        "lon": weather["longitude"]
    }])
    st.map(df)
else:
    st.warning("⚠️ No data received from Kafka yet. Waiting for first message...")
