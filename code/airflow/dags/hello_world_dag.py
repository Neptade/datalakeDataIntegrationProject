from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

conf = SparkConf().setMaster("spark://spark-master:7077").set("shutdownhookmanager","OFF")
spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

@dag(
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='0 9 * * *', # This cron expression means daily at 09:00
    start_date=datetime(2023, 7, 1, 9, 33), # Set a past date for immediate testing
    catchup=False,
    tags=['example'],
)
def hello_world():
    @task
    def print_hello():
        # Create an RDD
        rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
        # Transformation: Map each element to its square
        squared_rdd = rdd.map(lambda x: x**2)
        # Action: Collect the results
        result = squared_rdd.collect()
        print(result) # [1, 4, 9, 16, 25]

        logging.info("Executing print_hello task")
        print("Hello, Airflow!")
        return result
    print_hello()
    
dag = hello_world()