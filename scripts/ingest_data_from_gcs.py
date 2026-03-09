import duckdb
import os
from dotenv import load_dotenv

def ingest_cycle_hire_from_gcs():
    # 1. Initialize local DuckDB database (The Warehouse Layer)
    con = duckdb.connect('../data/warehouse/london_bikes.db')
    con.execute("CREATE SCHEMA IF NOT EXISTS london_bicycles")
    
    # 2. Install and load the httpfs extension for cloud connectivity
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # 3. Configure GCS Authentication (Best Practice: Use Service Account)
    # GCS is S3-compatible; use the following endpoint settings
    load_dotenv()
    s3_access_key_id = os.getenv("s3_access_key_id")
    s3_secret_access_key = os.getenv("s3_secret_access_key")
    con.execute("SET s3_endpoint='storage.googleapis.com';")
    con.execute("SET s3_access_key_id='" + s3_access_key_id + "';")
    con.execute("SET s3_secret_access_key='" + s3_secret_access_key + "';")
    con.execute("SET s3_region='eu';") # Mandatory EU region [3]

    # 4. Define the GCS path using wildcards to handle multiple sharded files
    # This supports the large volume (9.57GB) of the cycle_hire table
    gcs_path = 's3://london-bikes-data-lake/*cycle_hire*'

    print("Beginning ingestion from GCS to DuckDB cycle_hire table...")
    
    # 5. Create the raw staging table directly from Parquet files
    # This follows the ELT model where data enters the warehouse in raw form [1]
    con.execute(f"""
        CREATE OR REPLACE TABLE london_bicycles.cycle_hire AS 
        SELECT * FROM read_parquet('{gcs_path}')
    """)

    # 6. Verify the integrity of the ingestion
    count = con.execute("SELECT COUNT(*) FROM london_bicycles.cycle_hire").fetchone()
    print(f"Ingestion Complete: {count} records loaded into cycle_hire table.")
    
    con.close()

def ingest_cycle_station_from_gcs():
    # 1. Initialize local DuckDB database (The Warehouse Layer)
    con = duckdb.connect('../data/warehouse/london_bikes.db')
    con.execute("CREATE SCHEMA IF NOT EXISTS london_bicycles")
    
    # 2. Install and load the httpfs extension for cloud connectivity
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # 3. Configure GCS Authentication (Best Practice: Use Service Account)
    # GCS is S3-compatible; use the following endpoint settings
    load_dotenv()
    s3_access_key_id = os.getenv("s3_access_key_id")
    s3_secret_access_key = os.getenv("s3_secret_access_key")
    con.execute("SET s3_endpoint='storage.googleapis.com';")
    con.execute("SET s3_access_key_id='" + s3_access_key_id + "';")
    con.execute("SET s3_secret_access_key='" + s3_secret_access_key + "';")
    con.execute("SET s3_region='eu';") # Mandatory EU region [3]

    # 4. Define the GCS path using wildcards to handle multiple sharded files
    # This supports the large volume (9.57GB) of the cycle_hire table
    gcs_path = 's3://london-bikes-data-lake/*cycle_station*'

    print("Beginning ingestion from GCS to DuckDB cycle_stations table...")
    
    # 5. Create the raw staging table directly from Parquet files
    # This follows the ELT model where data enters the warehouse in raw form [1]
    con.execute(f"""
        CREATE OR REPLACE TABLE london_bicycles.cycle_stations AS 
        SELECT * FROM read_parquet('{gcs_path}')
    """)

    # 6. Verify the integrity of the ingestion
    count = con.execute("SELECT COUNT(*) FROM london_bicycles.cycle_stations").fetchone()
    print(f"Ingestion Complete: {count} records loaded into cycle_stations table.")
    
    con.close()

if __name__ == "__main__":
    ingest_cycle_hire_from_gcs()
    ingest_cycle_station_from_gcs()