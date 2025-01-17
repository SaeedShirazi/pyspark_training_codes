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

    # Task 3: Query and save data
    metadata_path = os.path.join(os.path.dirname(path), "metadata")
    os.makedirs(metadata_path, exist_ok=True)
    offset_file = os.path.join(metadata_path, "offset")
    schema_file = os.path.join(metadata_path, "schema")

    if os.path.exists(offset_file):
        # Read offset file
        with open(offset_file, "r") as f:
            max_id = int(f.read().strip())
        print(f"Found offset: {max_id}. Querying rows with {unique_id} > {max_id}")
        filtered_df = df.filter(f"{unique_id} > {max_id}")
        write_mode = "append"

        # Check schema consistency
        if os.path.exists(path):
            previous_schema_path = os.path.join(metadata_path, "schema")
            if os.path.exists(previous_schema_path):
                with open(previous_schema_path, "r") as f:
                    previous_schema = json.loads(f.read())
                current_schema = json.loads(filtered_df.schema.json())
                if previous_schema != current_schema:
                    print("ERROR: Schema mismatch detected. Exiting without writing data.")
                    spark.stop()
                    sys.exit(1)
                else:
                    print("Schema matches with previously written data. Proceeding with writing.")
    else:
        print("No offset file found. Querying all data.")
        filtered_df = df
        write_mode = "overwrite"

    # Save data to path
    filtered_df.write.mode(write_mode).parquet(path)
    print(f"Data saved to {path} in {write_mode} mode.")

    # Update offset and schema
    max_processed_id = filtered_df.agg({unique_id: "max"}).collect()[0][0]
    if max_processed_id is not None:
        with open(offset_file, "w") as f:
            f.write(str(max_processed_id))
        print(f"Updated offset file with max ID: {max_processed_id}")

    with open(schema_file, "w") as f:
        f.write(filtered_df.schema.json())
    print(f"Schema saved to {schema_file}.")

    spark.stop()
