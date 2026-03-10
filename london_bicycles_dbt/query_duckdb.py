import duckdb
import os
import polars as pl

def profile_table_columns(db_path: str):
    """
    Connects to DuckDB and displays the distinct count for every column 
    in fact and dimension tables using Polars.
    """
    print(f"Connecting to DuckDB database at: {db_path}")
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        # Connect in read-only mode
        conn = duckdb.connect(db_path, read_only=True)
        
        # Get table names
        tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        all_tables = [row[0] for row in conn.execute(tables_query).fetchall()]
        
        target_tables = all_tables

        for table in target_tables:
            print(f"\n{'='*60}")
            print(f" DISTINCT COUNTS FOR TABLE: {table}")
            print(f"{'='*60}")
            
            # 1. Get column names for the current table
            columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
            columns = [row[0] for row in conn.execute(columns_query).fetchall()]
            
            # 2. Build a dynamic SQL query to count distinct values for each column
            # We use COUNT(DISTINCT "col") to handle reserved words or spaces
            select_parts = [f'COUNT(DISTINCT "{col}") AS "{col}"' for col in columns]
            distinct_query = f"SELECT {', '.join(select_parts)} FROM {table}"
            
            # 3. Execute and fetch into a Polars DataFrame
            # .pl() converts the DuckDB result set directly to a Polars DF
            df_distinct = conn.execute(distinct_query).pl()
            
            # 4. Transpose for better readability (Columns as Rows)
            # This makes it easier to read if you have 20+ columns
            df_melted = df_distinct.unpivot(variable_name="Column Name", value_name="Distinct Count")
            
            print(df_melted)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print(f"\nDatabase connection closed.")

if __name__ == "__main__":
    DB_PATH = "./data/london_bicycle.duckdb"
    profile_table_columns(DB_PATH)