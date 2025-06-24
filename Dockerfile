# Start from the official Airflow image
FROM apache/airflow:2.10.0

RUN pip install --no-cache-dir kafka-python requests mysql-connector-python sqlalchemy Faker pandas

# Switch back to airflow user
USER airflow