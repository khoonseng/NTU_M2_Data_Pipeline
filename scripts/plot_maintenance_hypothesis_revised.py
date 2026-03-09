# I import os so I can create folders and build file paths safely.
import os

# I import duckdb so I can query the warehouse database directly from Python.
import duckdb

# I import matplotlib so I can draw and save charts.
import matplotlib.pyplot as plt

# I import numpy for numeric calculations like quantiles and clipping.
import numpy as np

# I import pandas because it is the main table tool for analysis in Python.
import pandas as pd

# I import seaborn to get cleaner chart styles.
import seaborn as sns

# I define where the DuckDB warehouse file lives.
DB_PATH = "/home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db"

# I define where image outputs should be saved.
OUTPUT_DIR = "/home/shaun/NTU_M2_Data_Pipeline/docs/images"

# I define where table outputs (CSV) should be saved.
OUTPUT_DATA_DIR = "/home/shaun/NTU_M2_Data_Pipeline/docs/data"

# I define the max gap for likely demand rebalancing when destination is popular.
REBALANCE_MAX_HOURS = 8.0

# I define the lower bound of the preventive maintenance duration window.
PREVENTIVE_MIN_HOURS = 24.0

# I define the upper bound of the preventive maintenance duration window.
PREVENTIVE_MAX_HOURS = 96.0

# I define the lower bound for out-of-service candidate events.
OUT_OF_SERVICE_MIN_HOURS = 336.0

# I enforce the assumption that preventive maintenance should be at most twice per bike-year.
MAX_PREVENTIVE_PER_BIKE_YEAR = 2

# I define "popular station" as top 10% by ride starts.
POPULAR_STATION_PERCENTILE = 0.90

# I write one SQL query to load relocation events plus a popularity flag for destination station.
BASE_EVENTS_SQL = f"""
WITH station_popularity AS (
  SELECT
    start_station_id AS station_id,
    COUNT(*) AS station_start_count
  FROM london_bicycles.staging_cycle_hire
  WHERE start_station_id IS NOT NULL
  GROUP BY start_station_id
),
popularity_threshold AS (
  SELECT quantile_cont(station_start_count, {POPULAR_STATION_PERCENTILE}) AS popular_station_cutoff
  FROM station_popularity
),
station_flags AS (
  SELECT
    p.station_id,
    p.station_start_count,
    CASE WHEN p.station_start_count >= t.popular_station_cutoff THEN TRUE ELSE FALSE END AS is_top_demand_station
  FROM station_popularity p
  CROSS JOIN popularity_threshold t
),
rides AS (
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
relocations AS (
  SELECT
    p.bike_id,
    p.prev_end_time,
    p.prev_end_station,
    p.next_start_time,
    p.next_start_station,
    DATE_DIFF('minute', p.prev_end_time, p.next_start_time) AS gap_minutes,
    DATE_DIFF('minute', p.prev_end_time, p.next_start_time) / 60.0 AS gap_hours,
    EXTRACT(year FROM p.prev_end_time) AS event_year,
    DATE_TRUNC('month', p.prev_end_time) AS event_month,
    COALESCE(s.is_top_demand_station, FALSE) AS to_top_demand_station
  FROM paired p
  LEFT JOIN station_flags s
    ON p.next_start_station = s.station_id
  WHERE p.next_start_time IS NOT NULL
    AND p.next_start_station IS NOT NULL
    AND p.next_start_station <> p.prev_end_station
    AND DATE_DIFF('minute', p.prev_end_time, p.next_start_time) >= 0
)
SELECT
  bike_id,
  prev_end_time,
  next_start_time,
  prev_end_station,
  next_start_station,
  gap_minutes,
  gap_hours,
  event_year,
  event_month,
  to_top_demand_station
FROM relocations
"""


# I create a helper function that loads base relocation events from DuckDB.
def _load_base_events() -> pd.DataFrame:
  # I open the warehouse database in read-only mode for safety.
  con = duckdb.connect(DB_PATH, read_only=True)

  # I execute the SQL query and convert results into a pandas table.
  df = con.sql(BASE_EVENTS_SQL).df()

  # I close the database connection cleanly.
  con.close()

  # I convert timestamp columns to datetime for plotting/grouping.
  df["prev_end_time"] = pd.to_datetime(df["prev_end_time"])

  # I convert the next start timestamp to datetime as well.
  df["next_start_time"] = pd.to_datetime(df["next_start_time"])

  # I convert month field to datetime so monthly charts sort correctly.
  df["event_month"] = pd.to_datetime(df["event_month"])

  # I return the prepared base dataframe.
  return df


# I create a helper function that applies the revised hypothesis classification rules.
def _classify_events(df: pd.DataFrame) -> pd.DataFrame:
  # I copy input data so I do not mutate the caller's dataframe unexpectedly.
  out = df.copy()

  # I initialize every row as short repair by default before applying more specific rules.
  out["inferred_state_h2"] = "Short repair likely (8h-24h)"

  # I define mask for short gaps to top-demand stations (likely rebalancing).
  m_rebalance = (out["gap_hours"] < REBALANCE_MAX_HOURS) & (out["to_top_demand_station"])

  # I assign rebalancing label to those rows.
  out.loc[m_rebalance, "inferred_state_h2"] = "Demand rebalancing likely (<8h + top-demand destination)"

  # I define mask for short gaps not to top-demand stations.
  m_short_unknown = (out["gap_hours"] < REBALANCE_MAX_HOURS) & (~out["to_top_demand_station"])

  # I assign operational/unknown short idle label for those rows.
  out.loc[m_short_unknown, "inferred_state_h2"] = "Operational / unknown short idle (<8h, non-demand destination)"

  # I define mask for long repair window above preventive max up to out-of-service threshold.
  m_long_repair = (out["gap_hours"] > PREVENTIVE_MAX_HOURS) & (out["gap_hours"] <= OUT_OF_SERVICE_MIN_HOURS)

  # I assign long repair label.
  out.loc[m_long_repair, "inferred_state_h2"] = "Long repair likely (96h-336h)"

  # I define mask for out-of-service candidates.
  m_oos = out["gap_hours"] > OUT_OF_SERVICE_MIN_HOURS

  # I assign out-of-service label.
  out.loc[m_oos, "inferred_state_h2"] = "Out-of-service candidate (>336h)"

  # I define mask for preventive window candidates by duration.
  m_prev_window = (out["gap_hours"] >= PREVENTIVE_MIN_HOURS) & (out["gap_hours"] <= PREVENTIVE_MAX_HOURS)

  # I start everyone in preventive window as "repeat downtime" until cap rule promotes top ranks.
  out.loc[m_prev_window, "inferred_state_h2"] = "Repeat downtime beyond preventive cap (24h-96h)"

  # I build a sorted subset so rank logic is deterministic.
  ranked = out.loc[m_prev_window, ["bike_id", "event_year", "gap_hours", "prev_end_time"]].copy()

  # I sort by bike-year and then by longest gap first so top ranks are most maintenance-like.
  ranked = ranked.sort_values(["bike_id", "event_year", "gap_hours", "prev_end_time"], ascending=[True, True, False, True])

  # I compute candidate rank within each bike-year.
  ranked["preventive_candidate_rank"] = ranked.groupby(["bike_id", "event_year"]).cumcount() + 1

  # I merge computed ranks back to the main dataframe using row index alignment.
  out.loc[ranked.index, "preventive_candidate_rank"] = ranked["preventive_candidate_rank"].to_numpy()

  # I build mask for candidates that satisfy preventive cap rule.
  m_prev_kept = m_prev_window & (out["preventive_candidate_rank"] <= MAX_PREVENTIVE_PER_BIKE_YEAR)

  # I assign preventive label only to capped top-ranked candidates.
  out.loc[m_prev_kept, "inferred_state_h2"] = "Preventive maintenance likely (24h-96h, max 2 per bike-year)"

  # I return classified rows.
  return out


# I build class summary metrics for reporting.
def _build_class_summary(df_class: pd.DataFrame) -> pd.DataFrame:
  # I aggregate counts and durations by class.
  summary = (
    df_class.groupby("inferred_state_h2", as_index=False)
    .agg(
      event_count=("bike_id", "size"),
      mean_gap_hours=("gap_hours", "mean"),
      median_gap_hours=("gap_hours", "median"),
      p90_gap_hours=("gap_hours", lambda s: float(np.quantile(s, 0.90))),
    )
  )

  # I convert counts to percentage share.
  summary["share_pct"] = 100.0 * summary["event_count"] / summary["event_count"].sum()

  # I define class order for stable presentation.
  order = [
    "Demand rebalancing likely (<8h + top-demand destination)",
    "Operational / unknown short idle (<8h, non-demand destination)",
    "Short repair likely (8h-24h)",
    "Preventive maintenance likely (24h-96h, max 2 per bike-year)",
    "Repeat downtime beyond preventive cap (24h-96h)",
    "Long repair likely (96h-336h)",
    "Out-of-service candidate (>336h)",
  ]

  # I apply categorical ordering.
  summary["inferred_state_h2"] = pd.Categorical(summary["inferred_state_h2"], categories=order, ordered=True)

  # I sort rows by desired order.
  summary = summary.sort_values("inferred_state_h2").reset_index(drop=True)

  # I return summary table.
  return summary


# I build per-bike-year preventive frequency diagnostics.
def _build_bike_year_preventive(df_class: pd.DataFrame) -> pd.DataFrame:
  # I aggregate preventive counts by bike and year.
  bike_year = (
    df_class.assign(is_preventive=(df_class["inferred_state_h2"] == "Preventive maintenance likely (24h-96h, max 2 per bike-year)").astype(int))
    .groupby(["bike_id", "event_year"], as_index=False)
    .agg(preventive_event_count=("is_preventive", "sum"))
  )

  # I return bike-year table.
  return bike_year


# I build top-demand destination correlation by class.
def _build_demand_share(df_class: pd.DataFrame) -> pd.DataFrame:
  # I aggregate demand-destination percentages by class.
  out = (
    df_class.groupby("inferred_state_h2", as_index=False)
    .agg(
      event_count=("bike_id", "size"),
      pct_to_top_demand_station=("to_top_demand_station", lambda s: 100.0 * float(np.mean(s.astype(int)))),
    )
  )

  # I return demand-share table.
  return out


# I draw a threshold-aware full distribution chart.
def _save_h2_distribution(df_class: pd.DataFrame) -> None:
  # I keep positive values for log-scale plotting.
  vals = df_class.loc[df_class["gap_hours"] > 0, "gap_hours"].to_numpy()

  # I stop if there is nothing to draw.
  if vals.size == 0:
    return

  # I use log-spaced bins to represent heavy tails properly.
  bins = np.geomspace(vals.min(), vals.max(), 90)

  # I create a figure.
  fig, ax = plt.subplots(figsize=(11, 6))

  # I draw histogram counts.
  ax.hist(vals, bins=bins, color="#4e79a7", alpha=0.65, edgecolor="white", linewidth=0.25)

  # I add threshold lines that map to hypothesis boundaries.
  ax.axvline(REBALANCE_MAX_HOURS, color="#59a14f", linestyle="--", linewidth=1.8, label="8h rebalance boundary")

  # I add preventive lower bound.
  ax.axvline(PREVENTIVE_MIN_HOURS, color="#f28e2b", linestyle="--", linewidth=1.8, label="24h preventive min")

  # I add preventive upper bound.
  ax.axvline(PREVENTIVE_MAX_HOURS, color="#e15759", linestyle="--", linewidth=1.8, label="96h preventive max")

  # I add out-of-service threshold.
  ax.axvline(OUT_OF_SERVICE_MIN_HOURS, color="#7f3b08", linestyle="--", linewidth=1.8, label=">336h out-of-service")

  # I set x-axis to log scale.
  ax.set_xscale("log")

  # I set title.
  ax.set_title("Revised Hypothesis: Relocation Gap Distribution with Decision Thresholds")

  # I set x label.
  ax.set_xlabel("Gap Duration (Hours, log scale)")

  # I set y label.
  ax.set_ylabel("Number of Events")

  # I add grid.
  ax.grid(alpha=0.2)

  # I add legend.
  ax.legend(loc="upper right", fontsize=8)

  # I tighten layout.
  fig.tight_layout()

  # I save chart.
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_h2_gap_distribution_thresholds.png"), dpi=150)

  # I close figure to free memory.
  plt.close(fig)


# I draw class-count comparison chart.
def _save_h2_class_counts(summary: pd.DataFrame) -> None:
  # I create figure.
  fig, ax = plt.subplots(figsize=(12, 6))

  # I draw horizontal bars of event counts by class.
  sns.barplot(data=summary, x="event_count", y="inferred_state_h2", color="#4e79a7", ax=ax)

  # I set title.
  ax.set_title("Revised Hypothesis: Event Counts by Inferred State")

  # I set x label.
  ax.set_xlabel("Event Count")

  # I set y label blank for readability.
  ax.set_ylabel("")

  # I add light grid on x-axis.
  ax.grid(alpha=0.25, axis="x")

  # I add share labels next to bars.
  for i, row in summary.reset_index(drop=True).iterrows():
    ax.text(row["event_count"] * 1.005, i, f"{row['share_pct']:.2f}%", va="center", fontsize=8)

  # I tighten layout.
  fig.tight_layout()

  # I save chart.
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_h2_class_counts.png"), dpi=150)

  # I close figure.
  plt.close(fig)


# I draw monthly mix chart for revised classes.
def _save_h2_monthly_mix(df_class: pd.DataFrame) -> None:
  # I aggregate monthly counts by class.
  monthly = (
    df_class.groupby(["event_month", "inferred_state_h2"], as_index=False)
    .agg(event_count=("bike_id", "size"))
  )

  # I pivot to wide format for area chart.
  pivot = monthly.pivot(index="event_month", columns="inferred_state_h2", values="event_count").fillna(0)

  # I create figure.
  fig, ax = plt.subplots(figsize=(12, 6))

  # I draw stacked area plot.
  pivot.plot.area(ax=ax, stacked=True, alpha=0.85, linewidth=0.2)

  # I set title.
  ax.set_title("Revised Hypothesis: Monthly Mix of Inferred States")

  # I set x label.
  ax.set_xlabel("Month")

  # I set y label.
  ax.set_ylabel("Event Count")

  # I add grid.
  ax.grid(alpha=0.2)

  # I place legend.
  ax.legend(loc="upper left", fontsize=8, title="Inferred State")

  # I tighten layout.
  fig.tight_layout()

  # I save chart.
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_h2_monthly_mix.png"), dpi=150)

  # I close figure.
  plt.close(fig)


# I draw preventive frequency distribution per bike-year.
def _save_h2_preventive_frequency(bike_year: pd.DataFrame) -> None:
  # I create a clipped count for a clean axis (0,1,2,3+).
  clipped = bike_year["preventive_event_count"].clip(upper=3)

  # I map 3 to label "3+".
  labels = clipped.map({0: "0", 1: "1", 2: "2", 3: "3+"})

  # I count each bucket.
  freq = labels.value_counts().reindex(["0", "1", "2", "3+"], fill_value=0).reset_index()

  # I rename columns for plotting.
  freq.columns = ["preventive_events_per_bike_year", "bike_year_count"]

  # I compute share percent per bucket.
  freq["share_pct"] = 100.0 * freq["bike_year_count"] / freq["bike_year_count"].sum()

  # I create figure.
  fig, ax = plt.subplots(figsize=(9, 5))

  # I draw bars.
  sns.barplot(data=freq, x="preventive_events_per_bike_year", y="bike_year_count", color="#f28e2b", ax=ax)

  # I set title.
  ax.set_title("Preventive Events per Bike-Year (Revised Cap Assumption)")

  # I set x label.
  ax.set_xlabel("Preventive Event Count per Bike-Year")

  # I set y label.
  ax.set_ylabel("Bike-Year Count")

  # I add grid.
  ax.grid(alpha=0.25, axis="y")

  # I add percentage labels above bars.
  for i, row in freq.iterrows():
    ax.text(i, row["bike_year_count"] * 1.01, f"{row['share_pct']:.1f}%", ha="center", va="bottom", fontsize=8)

  # I tighten layout.
  fig.tight_layout()

  # I save chart.
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_h2_preventive_per_bike_year.png"), dpi=150)

  # I close figure.
  plt.close(fig)


# I draw top-demand destination share by class.
def _save_h2_demand_share(demand_share: pd.DataFrame) -> None:
  # I sort rows by demand share descending for readability.
  ds = demand_share.sort_values("pct_to_top_demand_station", ascending=False).copy()

  # I create figure.
  fig, ax = plt.subplots(figsize=(12, 6))

  # I draw bars of demand-destination percentage.
  sns.barplot(data=ds, x="pct_to_top_demand_station", y="inferred_state_h2", color="#59a14f", ax=ax)

  # I set title.
  ax.set_title("Destination Popularity Correlation by Inferred State")

  # I set x label.
  ax.set_xlabel("% of Events Ending at Top-Demand Stations")

  # I set y label blank.
  ax.set_ylabel("")

  # I set x range to percent bounds.
  ax.set_xlim(0, 100)

  # I add grid.
  ax.grid(alpha=0.25, axis="x")

  # I add value labels.
  for i, row in ds.reset_index(drop=True).iterrows():
    ax.text(row["pct_to_top_demand_station"] + 0.8, i, f"{row['pct_to_top_demand_station']:.2f}%", va="center", fontsize=8)

  # I tighten layout.
  fig.tight_layout()

  # I save chart.
  fig.savefig(os.path.join(OUTPUT_DIR, "maintenance_h2_top_demand_share_by_class.png"), dpi=150)

  # I close figure.
  plt.close(fig)


# I save result tables as CSV for transparent documentation.
def _save_tables(summary: pd.DataFrame, bike_year: pd.DataFrame, demand_share: pd.DataFrame) -> None:
  # I make sure output data folder exists.
  os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)

  # I save class summary table.
  summary.to_csv(os.path.join(OUTPUT_DATA_DIR, "maintenance_h2_class_summary.csv"), index=False)

  # I save bike-year preventive counts.
  bike_year.to_csv(os.path.join(OUTPUT_DATA_DIR, "maintenance_h2_bike_year_preventive_counts.csv"), index=False)

  # I save demand-share table.
  demand_share.to_csv(os.path.join(OUTPUT_DATA_DIR, "maintenance_h2_demand_share_by_class.csv"), index=False)


# I coordinate the full revised pipeline here.
def main() -> None:
  # I make sure chart output folder exists.
  os.makedirs(OUTPUT_DIR, exist_ok=True)

  # I apply a clean seaborn style.
  sns.set_theme(style="whitegrid")

  # I load base relocation events.
  df = _load_base_events()

  # I fail fast if no rows are available.
  if df.empty:
    raise RuntimeError("No relocation events found in source table.")

  # I run revised classification logic.
  df_class = _classify_events(df)

  # I build class summary.
  summary = _build_class_summary(df_class)

  # I build bike-year preventive diagnostics.
  bike_year = _build_bike_year_preventive(df_class)

  # I build destination popularity correlation table.
  demand_share = _build_demand_share(df_class)

  # I save charts.
  _save_h2_distribution(df_class)

  # I save class counts chart.
  _save_h2_class_counts(summary)

  # I save monthly mix chart.
  _save_h2_monthly_mix(df_class)

  # I save preventive frequency chart.
  _save_h2_preventive_frequency(bike_year)

  # I save destination popularity chart.
  _save_h2_demand_share(demand_share)

  # I save CSV tables.
  _save_tables(summary, bike_year, demand_share)

  # I print a small run summary for terminal visibility.
  print("Revised hypothesis artifacts generated.")

  # I print summary rows to make review easy.
  print(summary.to_string(index=False))

  # I print key output locations.
  print("Saved images in:", OUTPUT_DIR)

  # I print data output location.
  print("Saved tables in:", OUTPUT_DATA_DIR)


# I run main only when this file is executed directly.
if __name__ == "__main__":
  # I call the pipeline entry point.
  main()
