from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from dotenv import load_dotenv
import os
import boto3
from datetime import datetime
from urllib.parse import quote_plus
from sqlalchemy import create_engine,text


load_dotenv()
sf_url = os.getenv("SFURL")
sf_user=os.getenv("SFUSER")
sf_password = os.getenv("SFPASSWORD")
sf_database = os.getenv("SFDATABASE")
sf_schema = os.getenv("SFSCHEMA")
sf_warehouse = os.getenv("SFWAREHOUSE")
sf_role = os.getenv("SFROLE")
my_pass=os.getenv("PASSWORD")

#spark creation
def spark_create():
    spark = SparkSession.builder \
    .appName("MySQL_Snowflake_Integration") \
    .config("spark.jars", r"C:\Users\gowth\Desktop\Final_project\mysql-connector-j-9.3.0\mysql-connector-j-9.3.0.jar") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:3.1.1,net.snowflake:snowflake-jdbc:3.13.6") \
    .getOrCreate()
    return spark
#spark-mysql connection
def spark_mysql_connection(spark):
    jdbc_url = "jdbc:mysql://localhost:3306/db_guvi?useSSL=false&serverTimezone=UTC"
    connection_properties = {
    "user": "root",
    "password": my_pass,
    "driver": "com.mysql.cj.jdbc.Driver",
    "timestampTimezone": "Asia/Kolkata"
    }
    return jdbc_url, connection_properties
#mysql final data 
def weather_data_mysql(spark,jdbc_url,connection_properties):
    last_load_df = spark.read.jdbc(
    url=jdbc_url,
    table="load_time_tracker",
    properties=connection_properties)
    last_load_df = last_load_df.withColumn("loaded_time_corrected", expr("last_loaded_time - INTERVAL 5 HOURS 30 MINUTES"))
    corrected_time = last_load_df.select("loaded_time_corrected").first()[0]

    query = f"(SELECT * FROM station_data WHERE loaded_time > '{corrected_time}') AS filtered_data"

    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=connection_properties
    )
    final_df=df.withColumn("loaded_time", expr("loaded_time - INTERVAL 5 HOURS 30 MINUTES"))
    return final_df

encoded_password = quote_plus(my_pass)
MYSQL_CONN_STR = f"mysql+mysqlconnector://root:{encoded_password}@localhost:3306/db_guvi"
def run_stored_procedure():
    engine = create_engine(MYSQL_CONN_STR)
    with engine.begin() as conn:
        conn.execute(text("CALL sp_update_last_loaded_time();"))
#csv data
def read_sensor_csv(spark):
    df=spark.read.option("header","True") \
        .option("inferSchema","True") \
        .csv(r"C:\Users\gowth\Desktop\Final_project\faker_output")
    return df
#load final data to mysql
def spark_to_mysql(df,spark,jdbc_url,tablename,connection_props):
    df.write.jdbc(
    url=jdbc_url,
    table=tablename,
    mode="append",
    properties=connection_props)


#snowflake connection

def snowflake_loading(df,spark,tablename):
    sfOptions = {
    "sfURL": sf_url,
    "sfUser": sf_user,
    "sfPassword": sf_password,
    "sfDatabase": sf_database,
    "sfSchema": sf_schema,
    "sfWarehouse": sf_warehouse,
    "sfRole": sf_role }

    df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", tablename) \
    .mode("append") \
    .save()

def upload_to_s3():
    aws_access_key=os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key =os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = 'backupdataguvi'
    s3_folder = 'sensor_backup/'
    local_folder = r"C:\Users\gowth\Desktop\Final_project\faker_output"
    s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key)

    for filename in os.listdir(local_folder):
        if filename.endswith('.csv'):
            print(filename)
            local_path = os.path.join(local_folder, filename)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = s3_folder + f"{timestamp}_{filename}"
            try:
                s3_client.upload_file(local_path, bucket_name, s3_key)
                print(f"Uploaded: {filename} -> s3://{bucket_name}/{s3_key}")
                os.remove(local_path)
                print(f"🗑️ Deleted local file: {filename}")
            except Exception as e:
                print(f"Failed to upload {filename}: {e}")
    





    