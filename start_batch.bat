@echo off

echo Starting Zookeeper...
start cmd /k "cd C:\kafka && .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"

timeout /t 20 /nobreak

echo Starting Kafka Broker...
start cmd /k "cd C:\kafka && .\bin\windows\kafka-server-start.bat .\config\server.properties"

timeout /t 10 /nobreak
start cmd /k "cd C:\kafka && .\bin\windows\kafka-console-consumer.bat --topic weather-topic --bootstrap-server localhost:9092 --from-beginning"


timeout /t 15 /nobreak

echo Starting Spark Job...
start cmd /k "cd C:\Users\gowth\Desktop\Final_project\ && python spark_to_parquet.py"

timeout /t 10 /nobreak

echo Starting Streamlit App...
start cmd /k "cd C:\Users\gowth\Desktop\Final_project\ && streamlit run weather_dashboard.py"
