import os

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


DB_PATH = "/home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db"
OUTPUT_DIR = "/home/shaun/NTU_M2_Data_Pipeline/docs/images"


EVENTS_WITH_THRESHOLD_SQL = """
WITH rides AS (
  SELECT
    rental_id,
    bike_id,
    start_date,
    end_date,
    start_station_logical_terminal AS start_station_id,
    end_station_logical_terminal AS end_station_id
  FROM london_bicycles.staging_cycle_hire
  WHERE bike_id IS NOT NULL
),
paired AS (
  SELECT
    bike_id,
    rental_id,
    end_date AS prev_end_time,
    end_station_id AS prev_end_station,
    LEAD(start_date) OVER (
      PARTITION BY bike_id
      ORDER BY start_date, rental_id
    ) AS next_start_time,
    LEAD(start_station_id) OVER (
      PARTITION BY bike_id
      ORDER BY start_date, rental_id
    ) AS next_start_station
  FROM rides
),
candidate_events AS (
  SELECT
    bike_id,
    prev_end_station,
    next_start_station,
    prev_end_time,
    next_start_time,
    DATE_DIFF('minute', prev_end_time, next_start_time) AS gap_minutes
  FROM paired
  WHERE next_start_time IS NOT NULL
    AND next_start_station IS NOT NULL
    AND next_start_station <> prev_end_station
    AND DATE_DIFF('minute', prev_end_time, next_start_time) >= 0
),
threshold AS (
  SELECT quantile_cont(gap_minutes, 0.95) AS p95_gap_minutes
  FROM candidate_events
),
labeled AS (
  SELECT
    c.*,
    t.p95_gap_minutes,
    (c.gap_minutes >= t.p95_gap_minutes) AS is_suspected
  FROM candidate_events c
  CROSS JOIN threshold t
)
SELECT
  bike_id,
  prev_end_station,
  next_start_station,
  prev_end_time,
  next_start_time,
  gap_minutes,
  p95_gap_minutes,
  is_suspected
FROM labeled
"""


def _save_full_distribution(df: pd.DataFrame) -> None:
  fig, ax = plt.subplots(figsize=(10, 6))
  sns.histplot(df["gap_hours"], bins=80, kde=True, color="#1f77b4", ax=ax)
  p95_hours = df["p95_gap_minutes"].iloc[0] / 60.0
  ax.axvline(p95_hours, color="#d62728", linestyle="--", linewidth=2, label="95th percentile cutoff")
  ax.set_xscale("log")
  ax.set_title("All Relocation Gaps: Full Distribution (Log Scale)")
  ax.set_xlabel("Gap Duration (Hours, log scale)")
  ax.set_ylabel("Number of Events")
  ax.grid(alpha=0.25)
  ax.legend()
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_gap_distribution.png"), dpi=150)
  plt.close(fig)


def _save_suspected_distribution(df: pd.DataFrame) -> None:
  fig, ax = plt.subplots(figsize=(10, 6))
  sns.histplot(df["gap_hours"], bins=30, kde=True, color="#9467bd", ax=ax)
  ax.set_title("Top 5% Longest Relocation Gaps (Suspected Events Only)")
  ax.set_xlabel("Gap Duration (Hours)")
  ax.set_ylabel("Number of Suspected Events")
  ax.grid(alpha=0.25)
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_gap_distribution_suspected.png"), dpi=150)
  plt.close(fig)


def _save_linear_distribution(df: pd.DataFrame) -> None:
  fig, ax = plt.subplots(figsize=(10, 6))
  sns.histplot(df["gap_hours"], bins=80, kde=True, color="#17becf", ax=ax)
  p95_hours = df["p95_gap_minutes"].iloc[0] / 60.0
  ax.axvline(p95_hours, color="#d62728", linestyle="--", linewidth=2, label="95th percentile cutoff")
  ax.set_title("All Relocation Gaps: Full Distribution (Linear Scale)")
  ax.set_xlabel("Gap Duration (Hours)")
  ax.set_ylabel("Number of Events")
  ax.grid(alpha=0.25)
  ax.legend()
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_gap_distribution_linear.png"), dpi=150)
  plt.close(fig)


def _save_log10_with_normal_overlay(df: pd.DataFrame) -> None:
  vals = np.log10(df["gap_hours"].to_numpy())
  mu = float(np.mean(vals))
  sigma = float(np.std(vals, ddof=0))

  fig, ax = plt.subplots(figsize=(10, 6))
  sns.histplot(vals, bins=40, stat="density", color="#8c564b", alpha=0.5, ax=ax)

  x = np.linspace(vals.min(), vals.max(), 500)
  if sigma > 0:
    y = (1.0 / (sigma * np.sqrt(2.0 * np.pi))) * np.exp(-0.5 * ((x - mu) / sigma) ** 2)
    ax.plot(x, y, color="#d62728", linewidth=2, label="Normal curve (reference fit)")

  ax.set_title("log10(Gap Hours): Histogram with Normal Reference")
  ax.set_xlabel("log10(Gap Hours)")
  ax.set_ylabel("Density")
  ax.grid(alpha=0.25)
  ax.legend()
  fig.tight_layout()
  fig.savefig(
    os.path.join(OUTPUT_DIR, "maintenance_gap_log10_normal_reference.png"),
    dpi=150,
  )
  plt.close(fig)


def _save_monthly_trend(df: pd.DataFrame) -> None:
  monthly = (
    df.assign(month=df["prev_end_time"].dt.to_period("M").dt.to_timestamp())
    .groupby("month", as_index=False)
    .agg(
      relocation_events=("bike_id", "size"),
      suspected_events=("is_suspected", "sum"),
    )
  )
  monthly["suspected_rate_pct"] = 100.0 * monthly["suspected_events"] / monthly["relocation_events"]
  monthly["suspected_rate_pct_3m"] = monthly["suspected_rate_pct"].rolling(3, min_periods=1).mean()

  fig, axes = plt.subplots(2, 1, figsize=(12, 9), sharex=True)

  sns.lineplot(
    data=monthly,
    x="month",
    y="relocation_events",
    color="#1f77b4",
    linewidth=1.5,
    label="All relocation events",
    ax=axes[0],
  )
  sns.lineplot(
    data=monthly,
    x="month",
    y="suspected_events",
    color="#d62728",
    linewidth=1.5,
    label="Suspected events",
    ax=axes[0],
  )
  axes[0].set_title("Monthly Context: Relocations vs Suspected Maintenance-like Events")
  axes[0].set_ylabel("Event Count")
  axes[0].grid(alpha=0.25)
  axes[0].legend()

  sns.lineplot(
    data=monthly,
    x="month",
    y="suspected_rate_pct",
    color="#2ca02c",
    alpha=0.35,
    linewidth=1,
    label="Monthly rate",
    ax=axes[1],
  )
  sns.lineplot(
    data=monthly,
    x="month",
    y="suspected_rate_pct_3m",
    color="#2ca02c",
    linewidth=2,
    label="3-month avg rate",
    ax=axes[1],
  )
  axes[1].set_title("Monthly Suspected Rate (% of relocation events)")
  axes[1].set_xlabel("Month")
  axes[1].set_ylabel("Suspected Rate (%)")
  axes[1].grid(alpha=0.25)
  axes[1].legend()

  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_events_monthly.png"), dpi=150)
  plt.close(fig)


def _save_day_hour_heatmap(df: pd.DataFrame) -> None:
  day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
  heat_df = (
    df.assign(day=df["prev_end_time"].dt.day_name(), hour=df["prev_end_time"].dt.hour)
    .groupby(["day", "hour"], as_index=False)
    .size()
    .rename(columns={"size": "event_count"})
  )

  pivot = (
    heat_df.pivot(index="day", columns="hour", values="event_count")
    .reindex(day_order)
    .fillna(0)
  )

  fig, ax = plt.subplots(figsize=(12, 5))
  sns.heatmap(pivot, cmap="YlOrRd", linewidths=0.1, linecolor="white", ax=ax)
  ax.set_title("Likely Maintenance Events by Day of Week and Hour")
  ax.set_xlabel("Hour of Day")
  ax.set_ylabel("Day of Week")
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_events_day_hour_heatmap.png"), dpi=150)
  plt.close(fig)


def _save_top_transfers(df: pd.DataFrame) -> None:
  top = (
    df.groupby(["prev_end_station", "next_start_station"], as_index=False)
    .size()
    .rename(columns={"size": "event_count"})
    .sort_values("event_count", ascending=False)
    .head(15)
  )
  top["transition"] = (
    top["prev_end_station"].astype(str) + " -> " + top["next_start_station"].astype(str)
  )

  fig, ax = plt.subplots(figsize=(11, 7))
  sns.barplot(data=top, x="event_count", y="transition", color="#ff7f0e", ax=ax)
  ax.set_title("Top Station-to-Station Reappearance Paths")
  ax.set_xlabel("Number of Suspected Events")
  ax.set_ylabel("Station Transition")
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_top_station_transfers.png"), dpi=150)
  plt.close(fig)


def _save_duration_summary_comparison(df_all: pd.DataFrame, df_suspected: pd.DataFrame) -> None:
  mean_all_hours = float(df_all["gap_hours"].mean())
  p95_hours = float(df_all["p95_gap_minutes"].iloc[0] / 60.0)
  median_suspected_hours = float(df_suspected["gap_hours"].median())
  mean_suspected_hours = float(df_suspected["gap_hours"].mean())

  summary = pd.DataFrame(
    {
      "metric": [
        "Mean: All Relocations",
        "p95 Cutoff",
        "Median: Suspected",
        "Mean: Suspected",
      ],
      "hours": [
        mean_all_hours,
        p95_hours,
        median_suspected_hours,
        mean_suspected_hours,
      ],
    }
  )

  fig, ax = plt.subplots(figsize=(10, 6))
  sns.barplot(data=summary, x="hours", y="metric", palette=["#1f77b4", "#d62728", "#9467bd", "#ff7f0e"], ax=ax)
  ax.set_title("Gap Duration Comparison (Hours)")
  ax.set_xlabel("Hours")
  ax.set_ylabel("")
  ax.grid(alpha=0.25, axis="x")

  for i, v in enumerate(summary["hours"]):
    ax.text(v + max(summary["hours"]) * 0.01, i, f"{v:.2f}h", va="center")

  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_duration_comparison.png"), dpi=150)
  plt.close(fig)


def main() -> None:
  os.makedirs(OUTPUT_DIR, exist_ok=True)
  sns.set_theme(style="whitegrid")

  con = duckdb.connect(DB_PATH, read_only=True)
  df = con.sql(EVENTS_WITH_THRESHOLD_SQL).df()
  con.close()

  if df.empty:
    raise RuntimeError("No relocation events found. Plot generation skipped.")

  df["prev_end_time"] = pd.to_datetime(df["prev_end_time"])
  df["next_start_time"] = pd.to_datetime(df["next_start_time"])
  df["gap_hours"] = df["gap_minutes"] / 60.0
  df_suspected = df[df["is_suspected"]].copy()

  _save_full_distribution(df)
  _save_suspected_distribution(df_suspected)
  _save_linear_distribution(df)
  _save_log10_with_normal_overlay(df)
  _save_monthly_trend(df)
  _save_day_hour_heatmap(df_suspected)
  _save_top_transfers(df_suspected)
  _save_duration_summary_comparison(df, df_suspected)

  print("Generated plot files in:", OUTPUT_DIR)
  print("- maintenance_gap_distribution.png")
  print("- maintenance_gap_distribution_suspected.png")
  print("- maintenance_gap_distribution_linear.png")
  print("- maintenance_gap_log10_normal_reference.png")
  print("- maintenance_events_monthly.png")
  print("- maintenance_events_day_hour_heatmap.png")
  print("- maintenance_top_station_transfers.png")
  print("- maintenance_duration_comparison.png")


if __name__ == "__main__":
  main()
