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


if __name__ == "__main__":
    main()
