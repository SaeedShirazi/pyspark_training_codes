import argparse
from etl import postgresql_sync

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


  # Task 2: Call PostgreSQL sync function if db_type is postgresql
    if args.db_type.lower() == "postgresql":
        postgresql_sync(
            db_ip=args.db_ip,
            db_port=args.db_port,
            db_name=args.db_name,
            db_username=args.db_username,
            db_password=args.db_password,
            table_name=args.table_name,
            path=args.path,
            strategy=args.strategy,
            unique_id=args.unique_id
        )

if __name__ == "__main__":
    main()
