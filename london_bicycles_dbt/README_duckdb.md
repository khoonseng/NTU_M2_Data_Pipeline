# BigQuery to DuckDB Extraction and Verification


## Step 1: Extract Data to DuckDB (`extract_dbt_models_to_duckdb.py`)

This script connects to your BigQuery dataset, queries the star schema tables (`dim_bike`, `dim_station`, `dim_date`, `fact_bike_daily_usage`, `fact_hire`), and writes them to a local DuckDB database.

### How to Run:
1. Open your terminal and navigate to the `london_bicycles_dbt` directory.
2. Execute the extraction script:
   ```bash
   python extract_dbt_models_to_duckdb.py
   ```
3. The script will prompt you for two inputs:
   - **Google Cloud Project ID:** Enter your GCP project ID (e.g., `ntu-project-489202`).
   - **BigQuery dataset name:** If your data lives in `london_bicycles_star`, you can just press **Enter** to use the default, or type the name of the alternate dataset.
4. **Output:** The script will automatically create a `./data/` folder (if it doesn't already exist) and save the DuckDB database file as `./data/london_bicycle.duckdb`. Wait for all tables to successfully save.

---

## Step 2: Verify the Local Database (`query_duckdb.py`)

Once the extraction is complete, you can use the verification script to confirm the tables are loaded correctly. This script connects to the local DuckDB database and prints out the number of distinct values for every column in all tables.

### How to Run:
1. Ensure `extract_dbt_models_to_duckdb.py` has completely finished running (to avoid any database lock issues).
2. Execute the verification script:
   ```bash
   python query_duckdb.py
   ```
3. **Output:** The script will scan the DuckDB database at `./data/london_bicycle.duckdb`. It iterates through every table within the database, executing dynamic queries to count the `DISTINCT` records for each column, and then neatly displays the results in tabular format using Polars.

### Common Troubleshooting

- **`IO Error: Could not set lock on file ... Conflicting lock is held`**
  This means DuckDB is currently locked by another process (e.g., the extraction script is still running). Ensure the first script is closed or finished before running `query_duckdb.py`.

- **Google Cloud Application Default Credentials Error**
  If BigQuery cannot authenticate, ensure you have run `gcloud auth application-default login` in your terminal and you are logged into the correct Google account with permissions for the project.
