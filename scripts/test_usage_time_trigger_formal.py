import os
import math

import duckdb
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score


DB_PATH = "/home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db"
OUTPUT_DATA_DIR = "/home/shaun/NTU_M2_Data_Pipeline/docs/data"
OUTPUT_MD_PATH = "/home/shaun/NTU_M2_Data_Pipeline/docs/data/maintenance_usage_time_formal_tests_generated.md"


EVENT_USAGE_SQL = """
WITH rides AS (
  SELECT
    rental_id,
    bike_id,
    end_date,
    start_date,
    start_station_id,
    end_station_id,
    COALESCE(duration, 0) / 60.0 AS ride_minutes,
    EXTRACT(year FROM end_date) AS ride_year
  FROM london_bicycles.staging_cycle_hire
  WHERE bike_id IS NOT NULL
),
rides_enriched AS (
  SELECT
    bike_id,
    rental_id,
    end_date,
    ride_year,
    ride_minutes,
    SUM(ride_minutes) OVER (
      PARTITION BY bike_id, ride_year
      ORDER BY end_date, rental_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_usage_minutes_event,
    SUM(ride_minutes) OVER (
      PARTITION BY bike_id, ride_year
    ) AS total_usage_minutes_year
  FROM rides
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
    p.rental_id,
    EXTRACT(year FROM p.prev_end_time) AS event_year,
    p.prev_end_time,
    DATE_DIFF('minute', p.prev_end_time, p.next_start_time) / 60.0 AS gap_hours
  FROM paired p
  WHERE p.next_start_time IS NOT NULL
    AND p.next_start_station IS NOT NULL
    AND p.next_start_station <> p.prev_end_station
    AND DATE_DIFF('minute', p.prev_end_time, p.next_start_time) >= 0
),
joined AS (
  SELECT
    r.bike_id,
    r.rental_id,
    r.event_year,
    r.prev_end_time,
    r.gap_hours,
    re.cum_usage_minutes_event,
    re.total_usage_minutes_year
  FROM relocations r
  JOIN rides_enriched re
    ON r.bike_id = re.bike_id
   AND r.rental_id = re.rental_id
   AND r.event_year = re.ride_year
),
sequenced AS (
  SELECT
    bike_id,
    rental_id,
    event_year,
    prev_end_time,
    gap_hours,
    cum_usage_minutes_event,
    total_usage_minutes_year,
    ROW_NUMBER() OVER (
      PARTITION BY bike_id, event_year
      ORDER BY prev_end_time, rental_id
    ) AS relocation_seq_in_year,
    COUNT(*) OVER (
      PARTITION BY bike_id, event_year
    ) AS relocation_events_in_year
  FROM joined
)
SELECT
  bike_id,
  rental_id,
  event_year,
  prev_end_time,
  gap_hours,
  cum_usage_minutes_event,
  total_usage_minutes_year,
  relocation_seq_in_year,
  relocation_events_in_year
FROM sequenced
"""


def cliffs_delta(x: np.ndarray, y: np.ndarray) -> float:
  x = np.asarray(x)
  y = np.asarray(y)
  x = x[np.isfinite(x)]
  y = y[np.isfinite(y)]
  if x.size == 0 or y.size == 0:
    return float("nan")

  x_sorted = np.sort(x)
  y_sorted = np.sort(y)
  gt = 0
  lt = 0
  j_low = 0
  j_high = 0
  for xi in x_sorted:
    while j_low < y_sorted.size and y_sorted[j_low] < xi:
      j_low += 1
    while j_high < y_sorted.size and y_sorted[j_high] <= xi:
      j_high += 1
    gt += j_low
    lt += (y_sorted.size - j_high)
  total_pairs = x_sorted.size * y_sorted.size
  return (gt - lt) / total_pairs


def load_event_usage() -> pd.DataFrame:
  con = duckdb.connect(DB_PATH, read_only=True)
  df = con.sql(EVENT_USAGE_SQL).df()
  con.close()
  df["prev_end_time"] = pd.to_datetime(df["prev_end_time"])
  df["is_24_96"] = df["gap_hours"].between(24.0, 96.0, inclusive="both")
  return df


def build_first_hit_table(df: pd.DataFrame) -> pd.DataFrame:
  cols = [
    "bike_id",
    "event_year",
    "prev_end_time",
    "gap_hours",
    "cum_usage_minutes_event",
    "total_usage_minutes_year",
    "relocation_seq_in_year",
    "relocation_events_in_year",
  ]
  first = (
    df.loc[df["is_24_96"], cols]
    .sort_values(["bike_id", "event_year", "prev_end_time", "relocation_seq_in_year"])
    .groupby(["bike_id", "event_year"], as_index=False)
    .first()
  )
  first["usage_hours_before_first_24_96"] = first["cum_usage_minutes_event"] / 60.0
  first["usage_share_of_year_at_first_24_96"] = np.where(
    first["total_usage_minutes_year"] > 0,
    first["cum_usage_minutes_event"] / first["total_usage_minutes_year"],
    np.nan,
  )
  first["seq_share_of_year_at_first_24_96"] = first["relocation_seq_in_year"] / first["relocation_events_in_year"]
  return first


def test_d_first_hit_early(first: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
  usage_share = first["usage_share_of_year_at_first_24_96"].to_numpy()
  usage_share = usage_share[np.isfinite(usage_share)]

  seq_share = first["seq_share_of_year_at_first_24_96"].to_numpy()
  seq_share = seq_share[np.isfinite(seq_share)]

  w_usage = stats.wilcoxon(usage_share - 0.5, alternative="less", zero_method="wilcox")
  w_seq = stats.wilcoxon(seq_share - 0.5, alternative="less", zero_method="wilcox")

  lt_usage = int(np.sum(usage_share < 0.5))
  n_usage = int(usage_share.size)
  binom_usage = stats.binomtest(lt_usage, n_usage, p=0.5, alternative="greater")

  lt_seq = int(np.sum(seq_share < 0.5))
  n_seq = int(seq_share.size)
  binom_seq = stats.binomtest(lt_seq, n_seq, p=0.5, alternative="greater")

  summary = {
    "bike_year_with_24_96": int(first.shape[0]),
    "mean_usage_minutes_before_first": float(first["cum_usage_minutes_event"].mean()),
    "median_usage_minutes_before_first": float(first["cum_usage_minutes_event"].median()),
    "p25_usage_minutes_before_first": float(first["cum_usage_minutes_event"].quantile(0.25)),
    "p75_usage_minutes_before_first": float(first["cum_usage_minutes_event"].quantile(0.75)),
    "p90_usage_minutes_before_first": float(first["cum_usage_minutes_event"].quantile(0.90)),
    "mean_usage_share_at_first": float(np.mean(usage_share)),
    "median_usage_share_at_first": float(np.median(usage_share)),
    "mean_seq_share_at_first": float(np.mean(seq_share)),
    "median_seq_share_at_first": float(np.median(seq_share)),
    "wilcoxon_usage_stat": float(w_usage.statistic),
    "wilcoxon_usage_p_less_than_half": float(w_usage.pvalue),
    "wilcoxon_seq_stat": float(w_seq.statistic),
    "wilcoxon_seq_p_less_than_half": float(w_seq.pvalue),
    "binom_usage_lt_half_successes": lt_usage,
    "binom_usage_n": n_usage,
    "binom_usage_p": float(binom_usage.pvalue),
    "binom_seq_lt_half_successes": lt_seq,
    "binom_seq_n": n_seq,
    "binom_seq_p": float(binom_seq.pvalue),
  }

  buckets = pd.cut(
    first["cum_usage_minutes_event"],
    bins=[-np.inf, 0, 30, 60, 120, 240, 480, np.inf],
    labels=["0 min", "0-30 min", "30-60 min", "1-2 h", "2-4 h", "4-8 h", ">8 h"],
    right=True,
  )
  bucket_df = (
    buckets.value_counts(dropna=False)
    .rename_axis("usage_bucket")
    .reset_index(name="bike_year_count")
    .sort_values("usage_bucket")
    .reset_index(drop=True)
  )
  bucket_df["share_pct"] = 100.0 * bucket_df["bike_year_count"] / bucket_df["bike_year_count"].sum()

  return summary, bucket_df


def test_e_event_level_association(df: pd.DataFrame) -> dict:
  y = df["is_24_96"].astype(int).to_numpy()
  x_minutes = df["cum_usage_minutes_event"].to_numpy(dtype=float)
  x_log = np.log1p(x_minutes)

  point_r, point_p = stats.pointbiserialr(y, x_log)
  mw = stats.mannwhitneyu(x_log[y == 1], x_log[y == 0], alternative="two-sided")
  cd = cliffs_delta(x_log[y == 1], x_log[y == 0])

  model = LogisticRegression(max_iter=300, solver="lbfgs")
  X = x_log.reshape(-1, 1)
  model.fit(X, y)
  proba = model.predict_proba(X)[:, 1]
  auc = roc_auc_score(y, proba)

  coef = float(model.coef_[0, 0])
  intercept = float(model.intercept_[0])
  odds_ratio_per_1_log_unit = float(math.exp(coef))

  baseline = float(np.mean(y))

  return {
    "n_events": int(df.shape[0]),
    "prevalence_24_96": baseline,
    "pointbiserial_r_logusage_vs_is24_96": float(point_r),
    "pointbiserial_p": float(point_p),
    "mannwhitney_u": float(mw.statistic),
    "mannwhitney_p": float(mw.pvalue),
    "cliffs_delta_logusage_24_96_vs_non": float(cd),
    "logit_coef_log1p_usage": coef,
    "logit_intercept": intercept,
    "logit_odds_ratio_per_1_logunit": odds_ratio_per_1_log_unit,
    "logit_auc": float(auc),
  }


def write_markdown(d: dict, e: dict, bucket_df: pd.DataFrame) -> None:
  bucket_lines = []
  for _, r in bucket_df.iterrows():
    bucket_lines.append(f"| {r['usage_bucket']} | {int(r['bike_year_count']):,} | {r['share_pct']:.2f}% |")

  md = f"""# Generated Appendix: Usage Time Before First 24-96h Relocation

## Purpose
This report tests whether bikes typically reach a `24-96h` relocation gap after substantial accumulated usage time, using **all rides** (not relocation counts only).

## Data and Construction
1. Build all relocation events as before (same bike, next ride exists, station changes, non-negative gap).
2. For each bike-year, compute cumulative ride usage minutes from all rides (`duration`) up to each relocation event.
3. Identify the **first** relocation event in that bike-year where `gap_hours` is between `24` and `96`.

## H-D: Early-Trigger Test (Usage Share at First 24-96h)
### Hypothesis
- H0: First `24-96h` event occurs around mid-year usage exposure (usage-share median >= 0.5).
- H1: First `24-96h` event occurs earlier than mid-year usage exposure (usage-share median < 0.5).

### Technical results
- Bike-years with at least one `24-96h`: {d['bike_year_with_24_96']:,}
- Mean usage minutes before first `24-96h`: {d['mean_usage_minutes_before_first']:.2f}
- Median usage minutes before first `24-96h`: {d['median_usage_minutes_before_first']:.2f}
- P25/P75 usage minutes: {d['p25_usage_minutes_before_first']:.2f} / {d['p75_usage_minutes_before_first']:.2f}
- P90 usage minutes: {d['p90_usage_minutes_before_first']:.2f}
- Mean usage-share at first event: {d['mean_usage_share_at_first']:.4f}
- Median usage-share at first event: {d['median_usage_share_at_first']:.4f}
- Wilcoxon test (usage-share < 0.5): p = {d['wilcoxon_usage_p_less_than_half']:.3e}
- Binomial sign test (share < 0.5): {d['binom_usage_lt_half_successes']:,}/{d['binom_usage_n']:,}, p = {d['binom_usage_p']:.3e}

### Layperson interpretation
The first `24-96h` event usually appears **early** in a bike's yearly usage timeline, not after most yearly usage has accumulated.
That weakens the idea that `24-96h` is a direct usage-threshold trigger by itself.

Usage buckets before first `24-96h`:
| Usage Before First 24-96h | Bike-Years | Share |
|---|---:|---:|
{os.linesep.join(bucket_lines)}

## H-E: Event-Level Association with Cumulative Usage Time
### Hypothesis
- H0: cumulative usage time has no association with whether an event is in `24-96h`.
- H1: cumulative usage time is associated with `24-96h` occurrence.

### Technical results
- Event count: {e['n_events']:,}
- 24-96h prevalence: {100.0*e['prevalence_24_96']:.2f}%
- Point-biserial r(log-usage, is_24_96): {e['pointbiserial_r_logusage_vs_is24_96']:.4f}, p = {e['pointbiserial_p']:.3e}
- Mann-Whitney U p-value: {e['mannwhitney_p']:.3e}
- Cliff's delta: {e['cliffs_delta_logusage_24_96_vs_non']:.4f}
- Logistic coefficient on log-usage: {e['logit_coef_log1p_usage']:.4f}
- Logistic odds ratio per +1 log unit usage: {e['logit_odds_ratio_per_1_logunit']:.4f}
- Logistic AUC (1-feature model): {e['logit_auc']:.4f}

### Layperson interpretation
Usage time has a statistically detectable relationship with `24-96h` events, but predictive strength is limited if usage is the only feature.
So usage contributes, but does not fully explain the class.

## Bottom Line
1. First `24-96h` events tend to happen early in yearly usage exposure.
2. This supports your concern that `24-96h` is not a pure preventive-maintenance trigger.
3. Usage matters, but on its own it is not enough to label maintenance confidently.
4. The revised capped framework remains useful, but should be treated as conservative inference until work-order labels are available.
5. Primary narrative report: `/home/shaun/NTU_M2_Data_Pipeline/docs/maintenance_hypothesis_formal_tests.md`.
"""

  with open(OUTPUT_MD_PATH, "w", encoding="utf-8") as f:
    f.write(md)


def main() -> None:
  os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)

  df = load_event_usage()
  if df.empty:
    raise RuntimeError("No event rows loaded for usage-time formal tests.")

  first = build_first_hit_table(df)
  d_summary, d_buckets = test_d_first_hit_early(first)
  e_summary = test_e_event_level_association(df)

  pd.DataFrame([d_summary]).to_csv(
    os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_D_usage_time_first_hit.csv"),
    index=False,
  )
  d_buckets.to_csv(
    os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_D_usage_time_buckets.csv"),
    index=False,
  )
  first.to_csv(
    os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_D_first_hit_rows.csv"),
    index=False,
  )
  pd.DataFrame([e_summary]).to_csv(
    os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_E_usage_time_association.csv"),
    index=False,
  )

  write_markdown(d_summary, e_summary, d_buckets)

  print("Usage-time formal tests complete.")
  print("-", OUTPUT_MD_PATH)
  print("-", os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_D_usage_time_first_hit.csv"))
  print("-", os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_D_usage_time_buckets.csv"))
  print("-", os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_E_usage_time_association.csv"))


if __name__ == "__main__":
  main()
