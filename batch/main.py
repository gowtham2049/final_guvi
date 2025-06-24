from functions import (
    spark_create,
    spark_mysql_connection,
    weather_data_mysql,
    read_sensor_csv,
    spark_to_mysql,
    snowflake_loading,
    upload_to_s3,
    run_stored_procedure
)
from pyspark.sql.functions import col, expr

# Create Spark session
spark = spark_create()
print("spark creating")
spark.sparkContext.setLogLevel("WARN")  # or "WARN"

# Get MySQL connection
jdbc_url, connection_properties = spark_mysql_connection(spark)
print("jdbc con done")


# Read incremental data from MySQL
station_df = weather_data_mysql(spark, jdbc_url, connection_properties)
print("weather data")
# Read CSV data
sensor_df = read_sensor_csv(spark)
print("sensor data")
cr_station_df = station_df.withColumn("loaded_time", expr("loaded_time + INTERVAL 5 HOURS 30 MINUTES"))
cr_station_df = cr_station_df.dropDuplicates(["station_id", "date"])
station_count = cr_station_df.count()
sensor_count = sensor_df.count()
print(f"Number of station records being written to MySQL: {station_count}")
print(f"Number of sensor records being written to MySQL: {sensor_count}")

# Write to MySQL
spark_to_mysql(cr_station_df, spark, jdbc_url, "final_station", connection_properties)
spark_to_mysql(sensor_df, spark, jdbc_url, "final_sensor", connection_properties)
print("loaded to mysql")
run_stored_procedure()

# Write to Snowflake
snowflake_loading(cr_station_df, spark, "weather_station_data")
snowflake_loading(sensor_df, spark, "sensor_data")
spark.stop()
print("spark stop")


upload_to_s3()
print("uploaded to s3")