from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import to_timestamp
from dotenv import load_dotenv
import os

#streaming_kafka_to_parquet.py
load_dotenv(r"C:\Users\gowth\Desktop\Final_project\.env")
output_path = os.getenv("OUTPUT_PATH")
checkpoint = os.getenv("CHECKPOINT_PATH")
topic_name = os.getenv("TOPIC_NAME")



spark = SparkSession.builder \
    .appName("KafkaSparkStream") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.driver.extraJavaOptions", "-javaagent:C:/spark/jmx_exporter/jmx_prometheus_javaagent-0.20.0.jar=9310:C:/spark/jmx_exporter/spark.yml") \
    .getOrCreate()

kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe",topic_name) \
            .option("startingOffsets", "latest") \
            .load()


schema = StructType() \
    .add("location_timestamp", StringType()) \
    .add("city", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", IntegerType()) \
    .add("pressure", IntegerType()) \
    .add("wind_speed", DoubleType()) \
    .add("weather_description", StringType())

# Parse value
json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

data_with_time = json_df.withColumn("loaded_time", to_timestamp("location_timestamp"))

query = data_with_time.writeStream \
            .format("parquet").option("header", "true") \
            .option("path", output_path) \
            .option("checkpointLocation",checkpoint) \
            .trigger(processingTime="5 minutes") \
            .start()

query.awaitTermination()
