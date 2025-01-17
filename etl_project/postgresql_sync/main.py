import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="ETL project with Spark and PostgreSQL")
    parser.add_argument("--db_type", type=str, required=True, help="Database type (e.g., postgresql)")
    parser.add_argument("--db_ip", type=str, required=True, help="Database IP address")
    parser.add_argument("--db_port", type=int, required=True, help="Database port")
    parser.add_argument("--db_name", type=str, required=True, help="Database name")
    parser.add_argument("--db_username", type=str, required=True, help="Database username")
    parser.add_argument("--db_password", type=str, required=True, help="Database password")
    parser.add_argument("--table_name", type=str, required=True, help="Table name to sync")
    parser.add_argument("--path", type=str, required=True, help="Path to save data")
    parser.add_argument("--strategy", type=str, required=True, help="Write strategy (e.g., append, overwrite)")
    parser.add_argument("--unique_id", type=str, required=True, help="Unique ID column")
    return parser.parse_args()

def main():
    # Parse arguments
    args = parse_arguments()

    # Print parsed arguments
    print("Parsed Arguments:")
    for arg, value in vars(args).items():
        print(f"{arg}: {value}")

#output
# Parsed Arguments:
# db_type: postgresql
# db_ip: localhost
# db_port: 5432
# db_name: pyspark_training
# db_username: pyspark_user
# db_password: password
# table_name: etl_transformed_data
# path: /home/saeed/PycharmProjects/pyspark_training_codes/etl_project/postgresql_sync/pyspark_training/data
# strategy: append
# unique_id: id


if __name__ == "__main__":
    main()
