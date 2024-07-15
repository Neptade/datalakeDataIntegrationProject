from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

conf = SparkConf().setMaster("spark://spark-master:7077").set("shutdownhookmanager","OFF")
spark = SparkSession.builder.appName("weatherData").getOrCreate()

@dag(
    default_args=default_args,
    description='a pipeline for weather data DAG',
    catchup=False,
)

def weathPipeline():
    @task
    def weathimport():
        df2016 = spark.read.csv("hdfs://namenode:9000/data/NW2016.csv")
        df2017 = spark.read.csv("hdfs://namenode:9000/data/NW2017.csv")
        df2018 = spark.read.csv("hdfs://namenode:9000/data/NW2018.csv")

        df = df2016.unionAll(df2017).unionAll(df2018)

        return df.write.csv("hdfs://namenode:9000/data/NW2016-2018.csv")

    
    weathimport()

dag = weathPipeline()