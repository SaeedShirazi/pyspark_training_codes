import pandas as pd
import random
from faker import Faker

# Initialize Faker
fake = Faker()

# Number of rows for the primary dataset
num_rows = 100000
chunk_size = 5000

# Predefined regions for consistency
regions = ["North", "South", "East", "West"]

# File paths
primary_file = "primary_data.csv"
secondary_file = "secondary_data.csv"

# Generate primary dataset in chunks
primary_columns = [
    "id", "name", "email", "age", "join_date", "last_purchase_date",
    "total_spent", "is_active", "region"
]
with open(primary_file, mode='w', newline='', encoding='utf-8') as file:
    pd.DataFrame(columns=primary_columns).to_csv(file, index=False)

for start_id in range(0, num_rows, chunk_size):
    chunk = pd.DataFrame({
        "id": range(start_id + 1, start_id + chunk_size + 1),
        "name": [fake.name() for _ in range(chunk_size)],
        "email": [fake.email() for _ in range(chunk_size)],
        "age": [random.randint(18, 80) for _ in range(chunk_size)],
        "join_date": [fake.date_between(start_date='-5y', end_date='today') for _ in range(chunk_size)],
        "last_purchase_date": [
            fake.date_between(start_date='-1y', end_date='today') if random.random() > 0.1 else None
            for _ in range(chunk_size)
        ],
        "total_spent": [round(random.uniform(100.0, 10000.0), 2) for _ in range(chunk_size)],
        "is_active": [random.choice([True, False]) for _ in range(chunk_size)],
        "region": [random.choice(regions) for _ in range(chunk_size)],  # Ensure consistent regions
    })
    chunk.to_csv(primary_file, mode='a', header=False, index=False)

# Generate secondary dataset
secondary_data = pd.DataFrame({
    "region": regions,  # Predefined regions
    "region_manager": [fake.name() for _ in regions],
    "region_budget": [round(random.uniform(50000.0, 200000.0), 2) for _ in regions],
    "established_date": [fake.date_between(start_date='-10y', end_date='-5y') for _ in regions],
})
secondary_data.to_csv(secondary_file, index=False)

print(f"Datasets generated:\n- Primary dataset: {primary_file}\n- Secondary dataset: {secondary_file}")
