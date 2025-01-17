from pyspark.sql import SparkSession
import os
import json
import sys

def postgresql_sync(db_ip, db_port, db_name, db_username, db_password, table_name, path, strategy, unique_id):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("PostgreSQL Sync") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # PostgreSQL JDBC URL
    jdbc_url = f"jdbc:postgresql://{db_ip}:{db_port}/{db_name}"

    # PostgreSQL connection properties
    properties = {
        "user": db_username,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    # Task 2: Read and print rows from PostgreSQL table
    print("Reading data from PostgreSQL table...")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
    df.show()
