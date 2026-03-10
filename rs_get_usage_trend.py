import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Configuration
DB_PATH = './data/london_bikes.db'
OUTPUT_DIR = './outputs/'
SCHEMA = 'london_bicycles'

def setup_environment():
    """Ensures output directory exists."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created directory: {OUTPUT_DIR}")

def extract_and_aggregate():
    """Connects to DuckDB and performs fast SQL aggregation."""
    print("--- 1. CONNECT & EXTRACT & AGGREGATE ---")
    query = f"""
    SELECT
        strftime(start_date, '%Y-%m') AS year_month,
        CASE 
            WHEN bike_model IS NULL OR TRIM(bike_model) = '' THEN 'Pre-Model Era'
            ELSE bike_model
        END AS bike_model,
        COUNT(*) AS trip_count
    FROM {SCHEMA}.staging_cycle_hire
    WHERE start_date IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1, 2;
    """
    with duckdb.connect(DB_PATH) as con:
        # Fetch directly as pandas DataFrame for visualization
        df = con.execute(query).df()
    
    print(f"Aggregation complete. Output shape: {df.shape}")
    print(df.head())
    
    # Pivot for visualization
    pivot_df = df.pivot(index='year_month', columns='bike_model', values='trip_count').fillna(0)
    
    return pivot_df

def visualize_trends(pivot_df):
    """Generates charts for the aggregated data."""
    print("\n--- 2. VISUALIZATION ---")
    sns.set_theme(style="whitegrid", palette="colorblind")
    
    # Chart A: Overall Stacked Area
    plt.figure(figsize=(12, 6))
    pivot_df.plot(kind='area', stacked=True, ax=plt.gca(), alpha=0.7)
    
    plt.title("Monthly Bike Usage Trend (Overall)")
    plt.xlabel("Month")
    plt.ylabel("Total Trips")
    
    # Add ticks every 3 months for overall trend to avoid crowding
    x_ticks = range(0, len(pivot_df), 3)
    x_labels = pivot_df.index[::3]
    plt.xticks(ticks=x_ticks, labels=x_labels, rotation=45)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'monthly_usage_stacked_area.png'))
    plt.close()

    # Chart B: Modern Era Focus (When e-bikes were introduced)
    # The first appearance of PBSC_EBIKE or CLASSIC
    modern_cols = [c for c in pivot_df.columns if c != 'Pre-Model Era']
    if modern_cols:
        modern_start = pivot_df[modern_cols].any(axis=1).idxmax()
        modern_df = pivot_df.loc[modern_start:]

        plt.figure(figsize=(10, 6))
        # Drop Pre-Model Era if it's mostly 0 or we just want to focus on new models
        # Let's keep it to show the transition
        modern_df.plot(kind='bar', stacked=True, ax=plt.gca(), alpha=0.7)
        
        plt.title(f"Modern Era Bike Usage (Since {modern_start})")
        plt.xlabel("Month")
        plt.ylabel("Total Trips")
        plt.xticks(rotation=45)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'modern_era_usage_stacked_bar.png'))
        plt.close()
        
        # Chart C: 100% Stacked Bar for Modern Era
        # This shows the proportion of PBSC_EBIKE vs CLASSIC clearly
        modern_pct_df = modern_df.div(modern_df.sum(axis=1), axis=0) * 100
        
        plt.figure(figsize=(10, 6))
        modern_pct_df.plot(kind='bar', stacked=True, ax=plt.gca(), alpha=0.7)
        
        plt.title(f"Modern Era Bike Usage % (Relative Share)")
        plt.xlabel("Month")
        plt.ylabel("Percentage (%)")
        plt.xticks(rotation=45)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'modern_era_usage_percentage_bar.png'))
        plt.close()

    print(f"LOG: Charts saved to {OUTPUT_DIR}")
    
   

def main():
    setup_environment()
    pivot_table = extract_and_aggregate()
    visualize_trends(pivot_table)
  

if __name__ == "__main__":
    main()