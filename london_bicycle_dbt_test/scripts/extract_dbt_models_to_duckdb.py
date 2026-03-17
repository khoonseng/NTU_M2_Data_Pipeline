import os
import duckdb
from google.cloud import bigquery

def main():
    print("=== Extract DBT Models to DuckDB ===")
    project_id = input("Enter your Google Cloud Project ID: ").strip()
    if not project_id:
        print("Error: Project ID cannot be empty.")
        return

    dataset_name = input("Enter the BigQuery dataset name (default: london_bicycles_star): ").strip()
    if not dataset_name:
        dataset_name = "london_bicycles_star"

    # 1) Save to '../data/london_bicycle.duckdb' (create the folder if no exist)
    output_dir = '../data/warehouse'
    os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist
    db_path = os.path.join(output_dir, 'london_bikes.db')

    print(f"\nInitializing BigQuery client for project '{project_id}'...")
    try:
        bq_client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Failed to initialize BigQuery client: {e}")
        return

    # List of tables to extract based on the star schema and seeds
    tables_to_extract = [
        "dim_bike",
        "dim_station",
        "dim_date",
        "fact_bike_daily_usage",
        "fact_hire",
        "next_gen_station_table" #
    ]

    print(f"Connecting to DuckDB at '{db_path}'...")
    with duckdb.connect(db_path) as con:
        for table in tables_to_extract:
            query = f"SELECT * FROM `{project_id}.{dataset_name}.{table}`"
            print(f"Extracting '{table}' from BigQuery ({project_id}.{dataset_name}.{table})...")
            try:
                # Query BigQuery and fetch results directly into a Pandas DataFrame
                df = bq_client.query(query).to_dataframe()
                print(f"  -> Extracted {len(df)} rows. Writing to DuckDB...")
                
                # Overwrite table in DuckDB if it exists, otherwise create it
                con.execute(f"DROP TABLE IF EXISTS {table}")
                con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
                print(f"  -> Successfully saved '{table}' to DuckDB.")
            except Exception as e:
                print(f"  -> Error processing '{table}': {e}")

    print(f"\nExtraction finished! Data saved to '{db_path}'.")

if __name__ == "__main__":
    main()
