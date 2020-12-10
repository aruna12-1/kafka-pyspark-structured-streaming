

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Sample Final Project").master("local[2]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "2")

spark.sparkContext.setLogLevel("ERROR")

kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "tamilboomi").load().selectExpr("cast(value as string)", "timestamp")

kafka_df.printSchema()

# [2020-09-19 06:17:53, RaspberryPi-4, 15.77, 17.15, 22.23, 20.92]
value_df = kafka_df.withColumn("Body", split(kafka_df["value"], ","))\
    .withColumn("TimeStamp", col("Body").getItem(0))\
    .withColumn("Device_Name", col("Body").getItem(1))\
    .withColumn("Temperature", col("Body").getItem(2).cast("Double"))\
    .withColumn("Humidity", col("Body").getItem(3).cast("Double"))\
    .withColumn("Pressure", col("Body").getItem(4).cast("Double"))\
    .withColumn("WaterLevel", col("Body").getItem(5).cast("Double"))

value_df_1 = value_df.select("TimeStamp", "Device_Name", "Temperature", "Humidity", "Pressure", "WaterLevel")

value_df_1.printSchema()

value_df_1.writeStream.format("console").option("truncate", "false").outputMode("append").start()

#  Windowing


import mysql.connector # pip install mysql-connector
from datetime import datetime
import time


class ConnectDatabase:
    def process(self, row):
        connection = mysql.connector.connect(host="localhost", database="test",
                                             user="root", password="root")
        cursor = connection.cursor()
        query = "INSERT INTO agg_rpy_1 (StartTime, EndTime, Device_Name, Avg_Temperature, Avg_Humidity, Avg_Pressure, Avg_WaterLevel) VALUES ('" + \
                str(row.StartTime) + "','" + \
                str(row.EndTime) + "','" + \
                str(row.Device_Name) + "','" + \
                str(row.Avg_Temperature) + "','" + \
                str(row.Avg_Humidity) + "','" + \
                str(row.Avg_Pressure) + "','" + \
                str(row.Avg_WaterLevel) + "');"
        print("\n\n SQL query\n", query)
        try:
            cursor.execute(query)
            connection.commit()
            cursor.close()
            connection.close()
            del connection
        except Exception as e:
            cursor.close()
            connection.close()
            del connection
            print("Exception: ", e)


# Groupy by Windowing
window_agg = value_df_1.groupBy(window(value_df_1.TimeStamp, "30 seconds", "15 seconds"), value_df_1.Device_Name).mean()\
    .withColumnRenamed("avg(Temperature)", "Avg_Temperature").withColumnRenamed("avg(Humidity)", "Avg_Humidity")\
    .withColumnRenamed("avg(Pressure)", "Avg_Pressure").withColumnRenamed("avg(WaterLevel)", "Avg_WaterLevel")

window_agg_1 = window_agg.withColumn("StartTime", col("window.start"))\
    .withColumn("EndTime", col("window.end")).drop("window")

window_agg_1.printSchema()

window_agg_1.writeStream.foreach(ConnectDatabase()).outputMode("complete").start()

# Write to HDFS as csv
value_df_1.writeStream.format("parquet").option("path", "/user/tamilboomi/kafka_data_1")\
    .option("checkpointLocation", "/user/tamilboomi/checkpoint_kafka_1").outputMode("append").start()

window_agg_1.writeStream.format("console").option("truncate", "false").outputMode("complete").start().awaitTermination()

# create external table raw_rpy (datevalue string, device string, temperature bigint, humitidy bigint, pressure bigint, level bigint) row format delimited fields terminated by ',' stored as textfile location '/user/tamilboomi/kafka_data';


# create external table raw_rpy_par (TimeStamps string, Device_Name string, Temperature double, Humidity double, Pressure double, WaterLevel double) row format delimited fields terminated by ',' stored as parquet location '/user/tamilboomi/kafka_data_1';