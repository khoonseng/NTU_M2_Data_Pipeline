import os

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


DB_PATH = "/home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db"
OUTPUT_DIR = "/home/shaun/NTU_M2_Data_Pipeline/docs/images"
MIN_SERVICE_HOURS = 2.0
MAX_SERVICE_HOURS = 24.0 * 14.0
Z_BAND = 1.0


EVENTS_WITH_THRESHOLD_SQL = """
WITH rides AS (
  SELECT
    rental_id,
    bike_id,
    start_date,
    end_date,
    start_station_id,
    end_station_id
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
  gap_hours = df.loc[df["gap_hours"] > 0, "gap_hours"].to_numpy()
  if gap_hours.size == 0:
    return

  # Use log-spaced bins so the full heavy-tailed shape is visible on a log x-axis.
  bins = np.geomspace(gap_hours.min(), gap_hours.max(), 90)

  fig, ax = plt.subplots(figsize=(10, 6))
  ax.hist(
    gap_hours,
    bins=bins,
    color="#1f77b4",
    alpha=0.6,
    edgecolor="white",
    linewidth=0.3,
  )
  p95_hours = df["p95_gap_minutes"].iloc[0] / 60.0
  ax.axvline(p95_hours, color="#d62728", linestyle="--", linewidth=2, label="95th percentile cutoff")
  ax.set_xscale("log")
  ax.set_title("All Relocation Gaps: Full Distribution (Log Scale, Log-Spaced Bins)")
  ax.set_xlabel("Gap Duration (Hours, log scale)")
  ax.set_ylabel("Number of Events")
  ax.grid(alpha=0.25)
  ax.legend()
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "M01_maintenance_gap_distribution.png"), dpi=150)
  plt.close(fig)


def _save_suspected_distribution(df: pd.DataFrame) -> None:
  fig, ax = plt.subplots(figsize=(10, 6))
  sns.histplot(df["gap_hours"], bins=30, kde=True, color="#9467bd", ax=ax)
  ax.set_xscale("log")
  ax.set_title("Top 5% Longest Relocation Gaps (Suspected Events, Log Scale)")
  ax.set_xlabel("Gap Duration (Hours, log scale)")
  ax.set_ylabel("Number of Suspected Events")
  ax.grid(alpha=0.25)
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "M03_maintenance_gap_distribution_suspected.png"), dpi=150)
  plt.close(fig)


def _save_linear_distribution(df: pd.DataFrame) -> None:
  fig, ax = plt.subplots(figsize=(10, 6))
  sns.histplot(df["gap_hours"], bins=80, kde=True, color="#17becf", ax=ax)
  p95_hours = df["p95_gap_minutes"].iloc[0] / 60.0
  p99_hours = float(df["gap_hours"].quantile(0.99))
  ax.axvline(p95_hours, color="#d62728", linestyle="--", linewidth=2, label="95th percentile cutoff")
  ax.set_xlim(left=0, right=p99_hours)
  ax.set_title("All Relocation Gaps: Linear Scale (Zoomed to 99th Percentile)")
  ax.set_xlabel("Gap Duration (Hours)")
  ax.set_ylabel("Number of Events")
  ax.grid(alpha=0.25)
  ax.legend()
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "M02_maintenance_gap_distribution_linear.png"), dpi=150)
  plt.close(fig)


def _save_overall_gap_boxplot(df: pd.DataFrame) -> None:
  gap_hours = df.loc[df["gap_hours"] > 0, "gap_hours"].to_numpy()
  if gap_hours.size == 0:
    return

  q1, q2, q3 = np.quantile(gap_hours, [0.25, 0.5, 0.75])
  iqr = q3 - q1
  upper_fence = q3 + 1.5 * iqr
  outlier_pct = 100.0 * float(np.mean(gap_hours > upper_fence))

  fig, axes = plt.subplots(1, 2, figsize=(13, 5))

  sns.boxplot(x=gap_hours, whis=1.5, showfliers=False, color="#4e79a7", ax=axes[0])
  axes[0].axvline(upper_fence, color="#d62728", linestyle="--", linewidth=1.8, label="Upper fence (1.5 IQR)")
  axes[0].set_title("Overall Relocation Gap Boxplot (Hours)")
  axes[0].set_xlabel("Gap Hours")
  axes[0].legend(loc="upper right", fontsize=8)
  axes[0].grid(alpha=0.2)

  log_vals = np.log10(gap_hours)
  sns.boxplot(x=log_vals, whis=1.5, showfliers=False, color="#59a14f", ax=axes[1])
  axes[1].set_title("Overall Relocation Gap Boxplot (log10 Hours)")
  axes[1].set_xlabel("log10(Gap Hours)")
  axes[1].grid(alpha=0.2)

  fig.suptitle(
    f"Q1={q1:.2f}h, Median={q2:.2f}h, Q3={q3:.2f}h, Upper fence={upper_fence:.2f}h, Outliers={outlier_pct:.2f}%",
    fontsize=10,
  )
  fig.tight_layout(rect=(0, 0, 1, 0.95))
  fig.savefig(os.path.join(OUTPUT_DIR, "M13_maintenance_gap_boxplot_overall.png"), dpi=150)
  plt.close(fig)


def _save_log10_with_normal_overlay(df: pd.DataFrame) -> None:
  vals = np.log10(df.loc[df["gap_hours"] > 0, "gap_hours"].to_numpy())
  vals = vals[np.isfinite(vals)]
  if vals.size == 0:
    return
  mu = float(np.mean(vals))
  sigma = float(np.std(vals, ddof=0))

  fig, ax = plt.subplots(figsize=(10, 6))
  sns.histplot(vals, bins=40, stat="density", color="#8c564b", alpha=0.5, ax=ax)

  x = np.linspace(vals.min(), vals.max(), 500)
  if sigma > 0 and np.isfinite(mu):
    y = (1.0 / (sigma * np.sqrt(2.0 * np.pi))) * np.exp(-0.5 * ((x - mu) / sigma) ** 2)
    ax.plot(x, y, color="#d62728", linewidth=2, label="Normal curve (reference fit)")

  ax.set_title("log10(Gap Hours): Histogram with Normal Reference")
  ax.set_xlabel("log10(Gap Hours)")
  ax.set_ylabel("Density")
  ax.grid(alpha=0.25)
  if ax.get_legend_handles_labels()[0]:
    ax.legend()
  fig.tight_layout()
  fig.savefig(
    os.path.join(OUTPUT_DIR, "M04_maintenance_gap_log10_normal_reference.png"),
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

  # Top panel uses dual y-axes so small suspected counts are not flattened by large relocation counts.
  ax_left = axes[0]
  ax_right = ax_left.twinx()
  b1 = ax_left.bar(
    monthly["month"],
    monthly["relocation_events"],
    color="#1f77b4",
    alpha=0.35,
    width=25,
    label="All relocation events (left axis)",
  )
  l2 = ax_right.plot(
    monthly["month"],
    monthly["suspected_events"],
    color="#d62728",
    linewidth=1.8,
    marker="o",
    markersize=3,
    label="Suspected events (right axis)",
  )
  ax_left.set_title("Monthly Context: Relocations vs Suspected Maintenance-like Events")
  ax_left.set_ylabel("Relocation Events (count)")
  ax_right.set_ylabel("Suspected Events (count)")
  ax_left.grid(alpha=0.25)
  handles = [b1, l2[0]]
  labels = [h.get_label() for h in handles]
  ax_left.legend(handles, labels, loc="upper left")

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
  fig.savefig(os.path.join(OUTPUT_DIR, "M05_maintenance_events_monthly.png"), dpi=150)
  plt.close(fig)


def _save_monthly_suspected_zoom(df: pd.DataFrame) -> None:
  monthly = (
    df.assign(month=df["prev_end_time"].dt.to_period("M").dt.to_timestamp())
    .groupby("month", as_index=False)
    .agg(suspected_events=("is_suspected", "sum"))
  )
  monthly["suspected_events_3m"] = monthly["suspected_events"].rolling(3, min_periods=1).mean()

  fig, ax = plt.subplots(figsize=(12, 5))
  sns.lineplot(
    data=monthly,
    x="month",
    y="suspected_events",
    color="#d62728",
    linewidth=1.2,
    alpha=0.4,
    label="Monthly suspected events",
    ax=ax,
  )
  sns.lineplot(
    data=monthly,
    x="month",
    y="suspected_events_3m",
    color="#d62728",
    linewidth=2.0,
    label="3-month avg suspected events",
    ax=ax,
  )
  ax.set_title("Suspected Maintenance-like Events by Month (Zoomed)")
  ax.set_xlabel("Month")
  ax.set_ylabel("Suspected Events (count)")
  ax.grid(alpha=0.25)
  ax.legend()
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "M06_maintenance_events_monthly_suspected_zoom.png"), dpi=150)
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
  fig.savefig(os.path.join(OUTPUT_DIR, "M07_maintenance_events_day_hour_heatmap.png"), dpi=150)
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
  fig.savefig(os.path.join(OUTPUT_DIR, "M08_maintenance_top_station_transfers.png"), dpi=150)
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
  fig.savefig(os.path.join(OUTPUT_DIR, "M09_maintenance_duration_comparison.png"), dpi=150)
  plt.close(fig)


def _classify_events(df: pd.DataFrame) -> tuple[pd.DataFrame, float, float]:
  df_class = df.copy()
  positive = df_class["gap_hours"] > 0
  df_class["log10_gap_hours"] = np.nan
  df_class.loc[positive, "log10_gap_hours"] = np.log10(df_class.loc[positive, "gap_hours"])

  window_mask = (
    positive
    & (df_class["gap_hours"] >= MIN_SERVICE_HOURS)
    & (df_class["gap_hours"] <= MAX_SERVICE_HOURS)
  )
  window_vals = df_class.loc[window_mask, "log10_gap_hours"].to_numpy()
  mu = float(np.mean(window_vals)) if window_vals.size else 0.0
  sigma = float(np.std(window_vals, ddof=0)) if window_vals.size else 0.0

  if sigma > 0 and np.isfinite(mu):
    df_class["gap_z"] = (df_class["log10_gap_hours"] - mu) / sigma
  else:
    df_class["gap_z"] = 0.0

  states = np.full(len(df_class), "Preventive maintenance likely", dtype=object)
  states[df_class["gap_hours"] < MIN_SERVICE_HOURS] = "Operational / short idle (<2h)"
  states[df_class["gap_hours"] > MAX_SERVICE_HOURS] = "Out-of-service candidate (>14d)"
  mid_mask = (df_class["gap_hours"] >= MIN_SERVICE_HOURS) & (df_class["gap_hours"] <= MAX_SERVICE_HOURS)
  states[mid_mask & (df_class["gap_z"] < -Z_BAND)] = "Short repair likely"
  states[mid_mask & (df_class["gap_z"] > Z_BAND)] = "Long repair likely"
  df_class["inferred_state"] = states
  return df_class, mu, sigma


def _save_classification_counts(df_class: pd.DataFrame) -> None:
  order = [
    "Operational / short idle (<2h)",
    "Short repair likely",
    "Preventive maintenance likely",
    "Long repair likely",
    "Out-of-service candidate (>14d)",
  ]
  counts = (
    df_class["inferred_state"]
    .value_counts()
    .reindex(order, fill_value=0)
    .reset_index(name="event_count")
    .rename(columns={"index": "inferred_state"})
  )

  fig, ax = plt.subplots(figsize=(11, 6))
  sns.barplot(
    data=counts,
    x="event_count",
    y="inferred_state",
    palette=["#bdbdbd", "#e15759", "#4e79a7", "#f28e2b", "#7f3b08"],
    ax=ax,
  )
  ax.set_title("Inferred Event Types from Relocation Inactivity Gaps")
  ax.set_xlabel("Number of Events")
  ax.set_ylabel("")
  ax.grid(alpha=0.25, axis="x")
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "M10_maintenance_inferred_state_counts.png"), dpi=150)
  plt.close(fig)


def _save_classification_monthly(df_class: pd.DataFrame) -> None:
  order = [
    "Operational / short idle (<2h)",
    "Short repair likely",
    "Preventive maintenance likely",
    "Long repair likely",
    "Out-of-service candidate (>14d)",
  ]
  monthly = (
    df_class.assign(month=df_class["prev_end_time"].dt.to_period("M").dt.to_timestamp())
    .groupby(["month", "inferred_state"], as_index=False)
    .size()
    .rename(columns={"size": "event_count"})
  )
  pivot = (
    monthly.pivot(index="month", columns="inferred_state", values="event_count")
    .reindex(columns=order)
    .fillna(0)
  )

  fig, ax = plt.subplots(figsize=(12, 6))
  pivot.plot.area(
    ax=ax,
    stacked=True,
    linewidth=0.2,
    color=["#bdbdbd", "#e15759", "#4e79a7", "#f28e2b", "#7f3b08"],
    alpha=0.85,
  )
  ax.set_title("Monthly Inferred Event Mix (Full-Year Coverage)")
  ax.set_xlabel("Month")
  ax.set_ylabel("Event Count")
  ax.grid(alpha=0.2)
  ax.legend(loc="upper left", fontsize=8, title="Inferred State")
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "M11_maintenance_inferred_state_monthly_mix.png"), dpi=150)
  plt.close(fig)


def _save_service_class_boxplot(df_class: pd.DataFrame) -> None:
  service_states = [
    "Short repair likely",
    "Preventive maintenance likely",
    "Long repair likely",
  ]
  plot_df = df_class[
    df_class["inferred_state"].isin(service_states) & df_class["log10_gap_hours"].notna()
  ][["inferred_state", "log10_gap_hours"]]
  if plot_df.empty:
    return

  fig, ax = plt.subplots(figsize=(10, 6))
  sns.boxplot(
    data=plot_df,
    x="inferred_state",
    y="log10_gap_hours",
    order=service_states,
    palette=["#e15759", "#4e79a7", "#f28e2b"],
    showfliers=False,
    ax=ax,
  )
  ax.set_title("Service-Like Classes Compared on log10(Gap Hours)")
  ax.set_xlabel("")
  ax.set_ylabel("log10(Gap Hours)")
  ax.grid(alpha=0.25, axis="y")
  fig.tight_layout()
  fig.savefig(os.path.join(OUTPUT_DIR, "M12_maintenance_service_classes_log10_boxplot.png"), dpi=150)
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
  df_class, mu_log, sigma_log = _classify_events(df)

  _save_full_distribution(df)
  _save_suspected_distribution(df_suspected)
  _save_linear_distribution(df)
  _save_overall_gap_boxplot(df)
  _save_log10_with_normal_overlay(df)
  _save_monthly_trend(df)
  _save_monthly_suspected_zoom(df)
  _save_day_hour_heatmap(df_suspected)
  _save_top_transfers(df_suspected)
  _save_duration_summary_comparison(df, df_suspected)
  _save_classification_counts(df_class)
  _save_classification_monthly(df_class)
  _save_service_class_boxplot(df_class)

  print("Generated plot files in:", OUTPUT_DIR)
  print(f"- classification_log10_mu={mu_log:.4f}, classification_log10_sigma={sigma_log:.4f}")
  print("- M01_maintenance_gap_distribution.png")
  print("- M03_maintenance_gap_distribution_suspected.png")
  print("- M02_maintenance_gap_distribution_linear.png")
  print("- M13_maintenance_gap_boxplot_overall.png")
  print("- M04_maintenance_gap_log10_normal_reference.png")
  print("- M05_maintenance_events_monthly.png")
  print("- M06_maintenance_events_monthly_suspected_zoom.png")
  print("- M07_maintenance_events_day_hour_heatmap.png")
  print("- M08_maintenance_top_station_transfers.png")
  print("- M09_maintenance_duration_comparison.png")
  print("- M10_maintenance_inferred_state_counts.png")
  print("- M11_maintenance_inferred_state_monthly_mix.png")
  print("- M12_maintenance_service_classes_log10_boxplot.png")


if __name__ == "__main__":
  main()
