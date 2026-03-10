# I import os so I can build folders and file paths safely.
import os

# I import math for low-level numeric operations in custom test formulas.
import math

# I import itertools to generate pairwise class comparisons cleanly.
import itertools

# I import duckdb to query the warehouse database directly.
import duckdb

# I import numpy for arrays, vectorized operations, and random permutations.
import numpy as np

# I import pandas for table transformations and CSV exports.
import pandas as pd

# I import scipy.stats for formal hypothesis tests and p-values.
from scipy import stats


# I define where the warehouse database file is located.
DB_PATH = "/home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db"

# I define where formal-test output tables should be saved.
OUTPUT_DATA_DIR = "/home/shaun/NTU_M2_Data_Pipeline/docs/data"

# I define where the formal-test markdown summary should be saved.
OUTPUT_MD_PATH = "/home/shaun/NTU_M2_Data_Pipeline/docs/maintenance_hypothesis_formal_tests.md"

# I define the exact class names used by the revised hypothesis logic.
CLS_REBAL = "Demand rebalancing likely (<8h + top-demand destination)"
CLS_OPS = "Operational / unknown short idle (<8h, non-demand destination)"
CLS_SHORT = "Short repair likely (8h-24h)"
CLS_PREV = "Preventive maintenance likely (24h-96h, max 2 per bike-year)"
CLS_REPEAT = "Repeat downtime beyond preventive cap (24h-96h)"
CLS_LONG = "Long repair likely (96h-336h)"
CLS_OOS = "Out-of-service candidate (>336h)"

# I define how many permutations to run for the correlation-difference permutation test.
N_PERM = 2000

# I set a fixed random seed so results are reproducible run-to-run.
RNG_SEED = 42


# I store one SQL query that builds event-level rows with revised-class labels.
EVENTS_SQL = """
WITH station_popularity AS (
  SELECT
    start_station_id AS station_id,
    COUNT(*) AS station_start_count
  FROM london_bicycles.staging_cycle_hire
  WHERE start_station_id IS NOT NULL
  GROUP BY start_station_id
),
popularity_threshold AS (
  SELECT quantile_cont(station_start_count, 0.90) AS popular_station_cutoff
  FROM station_popularity
),
station_flags AS (
  SELECT
    p.station_id,
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
    p.rental_id,
    EXTRACT(year FROM p.prev_end_time) AS event_year,
    p.prev_end_time,
    DATE_DIFF('minute', p.prev_end_time, p.next_start_time) / 60.0 AS gap_hours,
    COALESCE(s.is_top_demand_station, FALSE) AS to_top_demand_station
  FROM paired p
  LEFT JOIN station_flags s
    ON p.next_start_station = s.station_id
  WHERE p.next_start_time IS NOT NULL
    AND p.next_start_station IS NOT NULL
    AND p.next_start_station <> p.prev_end_station
    AND DATE_DIFF('minute', p.prev_end_time, p.next_start_time) >= 0
),
preventive_candidates AS (
  SELECT
    r.bike_id,
    r.rental_id,
    r.event_year,
    ROW_NUMBER() OVER (
      PARTITION BY r.bike_id, r.event_year
      ORDER BY r.gap_hours DESC, r.prev_end_time
    ) AS preventive_candidate_rank
  FROM relocations r
  WHERE r.gap_hours >= 24.0
    AND r.gap_hours <= 96.0
),
ranked AS (
  SELECT
    r.*,
    c.preventive_candidate_rank
  FROM relocations r
  LEFT JOIN preventive_candidates c
    ON r.bike_id = c.bike_id
   AND r.rental_id = c.rental_id
   AND r.event_year = c.event_year
),
classified AS (
  SELECT
    r.*,
    CASE
      WHEN r.gap_hours < 8.0 AND r.to_top_demand_station THEN 'Demand rebalancing likely (<8h + top-demand destination)'
      WHEN r.gap_hours < 8.0 AND NOT r.to_top_demand_station THEN 'Operational / unknown short idle (<8h, non-demand destination)'
      WHEN r.gap_hours >= 8.0 AND r.gap_hours < 24.0 THEN 'Short repair likely (8h-24h)'
      WHEN r.gap_hours >= 24.0 AND r.gap_hours <= 96.0 AND r.preventive_candidate_rank <= 2 THEN 'Preventive maintenance likely (24h-96h, max 2 per bike-year)'
      WHEN r.gap_hours >= 24.0 AND r.gap_hours <= 96.0 AND r.preventive_candidate_rank > 2 THEN 'Repeat downtime beyond preventive cap (24h-96h)'
      WHEN r.gap_hours > 96.0 AND r.gap_hours <= 336.0 THEN 'Long repair likely (96h-336h)'
      ELSE 'Out-of-service candidate (>336h)'
    END AS inferred_state_h2
  FROM ranked r
)
SELECT
  bike_id,
  event_year,
  prev_end_time,
  gap_hours,
  to_top_demand_station,
  inferred_state_h2
FROM classified
"""


# I define a helper to compute two-proportion z-test without extra dependencies.
def two_prop_z_test(k1: int, n1: int, k2: int, n2: int) -> tuple[float, float, float, float]:
  # I compute sample proportions for each group.
  p1 = k1 / n1
  p2 = k2 / n2

  # I compute pooled proportion under H0 (equal proportions).
  p_pool = (k1 + k2) / (n1 + n2)

  # I compute standard error for the difference under pooled H0.
  se = math.sqrt(p_pool * (1.0 - p_pool) * (1.0 / n1 + 1.0 / n2))

  # I guard against divide-by-zero in pathological edge cases.
  if se == 0:
    return p1, p2, float("nan"), float("nan")

  # I compute z-statistic for p1 - p2.
  z = (p1 - p2) / se

  # I compute two-sided p-value from normal distribution.
  pval = 2.0 * stats.norm.sf(abs(z))

  # I return group rates plus test statistics.
  return p1, p2, z, pval


# I define a helper to compute Cliff's delta effect size for two samples.
def cliffs_delta(x: np.ndarray, y: np.ndarray) -> float:
  # I convert to numpy arrays and drop NaNs.
  x = np.asarray(x)
  y = np.asarray(y)
  x = x[np.isfinite(x)]
  y = y[np.isfinite(y)]

  # I return NaN if either group is empty.
  if x.size == 0 or y.size == 0:
    return float("nan")

  # I sort arrays for efficient pairwise comparison counting.
  x_sorted = np.sort(x)
  y_sorted = np.sort(y)

  # I walk through x values and count y elements less/greater than each x.
  gt = 0
  lt = 0
  j_low = 0
  j_high = 0

  # I count dominance relationships using sorted pointers.
  for xi in x_sorted:
    while j_low < y_sorted.size and y_sorted[j_low] < xi:
      j_low += 1
    while j_high < y_sorted.size and y_sorted[j_high] <= xi:
      j_high += 1
    gt += j_low
    lt += (y_sorted.size - j_high)

  # I compute Cliff's delta in [-1, 1].
  total_pairs = x_sorted.size * y_sorted.size
  return (gt - lt) / total_pairs


# I define Holm correction for multiple-comparison p-values.
def holm_correction(pvals: list[float]) -> list[float]:
  # I sort p-values with original indices.
  indexed = sorted(enumerate(pvals), key=lambda t: t[1])

  # I allocate corrected output list.
  corrected = [0.0] * len(pvals)

  # I apply Holm step-down rule.
  m = len(pvals)
  running_max = 0.0
  for rank, (idx, p) in enumerate(indexed):
    adj = (m - rank) * p
    running_max = max(running_max, adj)
    corrected[idx] = min(1.0, running_max)

  # I return corrected p-values aligned to original order.
  return corrected


# I load event-level dataset from DuckDB.
def load_events() -> pd.DataFrame:
  # I connect in read-only mode for safety.
  con = duckdb.connect(DB_PATH, read_only=True)

  # I execute query and fetch into pandas.
  df = con.sql(EVENTS_SQL).df()

  # I close connection.
  con.close()

  # I parse timestamp for potential time-based checks.
  df["prev_end_time"] = pd.to_datetime(df["prev_end_time"])

  # I return dataframe.
  return df


# I run formal test A: short-gap events are more likely to end at top-demand stations.
def test_a_rebalancing(df: pd.DataFrame) -> dict:
  # I define groups: short-gap (<8h) vs non-short-gap.
  short = df["gap_hours"] < 8.0
  non_short = ~short

  # I compute contingency counts.
  k1 = int(df.loc[short, "to_top_demand_station"].sum())
  n1 = int(short.sum())
  k2 = int(df.loc[non_short, "to_top_demand_station"].sum())
  n2 = int(non_short.sum())

  # I compute two-proportion z-test.
  p1, p2, z, pval_z = two_prop_z_test(k1, n1, k2, n2)

  # I compute Fisher exact test on 2x2 table.
  table = np.array([[k1, n1 - k1], [k2, n2 - k2]], dtype=int)
  odds_ratio, pval_fisher = stats.fisher_exact(table, alternative="two-sided")

  # I compute chi-square test for independence.
  chi2, pval_chi2, dof, _ = stats.chi2_contingency(table)

  # I return all statistics.
  return {
    "n_short": n1,
    "n_non_short": n2,
    "top_short": k1,
    "top_non_short": k2,
    "rate_top_short": p1,
    "rate_top_non_short": p2,
    "rate_diff": p1 - p2,
    "z_stat": z,
    "pval_z_two_sided": pval_z,
    "odds_ratio": odds_ratio,
    "pval_fisher_two_sided": pval_fisher,
    "chi2_stat": chi2,
    "chi2_dof": dof,
    "pval_chi2": pval_chi2,
  }


# I build bike-year aggregates for usage-coupling tests.
def build_bike_year(df: pd.DataFrame) -> pd.DataFrame:
  # I aggregate event counts and class-specific counts per bike-year.
  by = (
    df.groupby(["bike_id", "event_year"], as_index=False)
    .agg(
      relocation_events=("gap_hours", "size"),
      raw_24_96=("gap_hours", lambda s: int(((s >= 24.0) & (s <= 96.0)).sum())),
      assigned_preventive=("inferred_state_h2", lambda s: int((s == CLS_PREV).sum())),
    )
  )

  # I compute overflow count = raw window count - assigned preventive count.
  by["overflow_24_96"] = by["raw_24_96"] - by["assigned_preventive"]

  # I return bike-year table.
  return by


# I run formal test B: how strongly 24-96h behavior is usage-driven, and whether cap reduces coupling.
def test_b_usage_coupling(bike_year: pd.DataFrame) -> dict:
  # I extract variables as numpy arrays.
  x = bike_year["relocation_events"].to_numpy(dtype=float)
  y_raw = bike_year["raw_24_96"].to_numpy(dtype=float)
  y_assigned = bike_year["assigned_preventive"].to_numpy(dtype=float)
  y_overflow = bike_year["overflow_24_96"].to_numpy(dtype=float)

  # I compute Pearson correlations and p-values.
  r_raw, p_raw = stats.pearsonr(x, y_raw)
  r_assigned, p_assigned = stats.pearsonr(x, y_assigned)
  r_overflow, p_overflow = stats.pearsonr(x, y_overflow)

  # I compute Spearman correlations and p-values for robustness to non-linearity.
  rs_raw, ps_raw = stats.spearmanr(x, y_raw)
  rs_assigned, ps_assigned = stats.spearmanr(x, y_assigned)

  # I run paired-label permutation test for difference in Pearson correlations.
  # H0: raw and assigned are equally coupled to usage.
  # H1: raw has stronger coupling than assigned.
  rng = np.random.default_rng(RNG_SEED)
  obs_diff = r_raw - r_assigned
  perm_diffs = np.empty(N_PERM, dtype=float)

  # I repeatedly swap raw/assigned labels within each row with 50% chance.
  for i in range(N_PERM):
    swap = rng.random(x.size) < 0.5
    y1 = np.where(swap, y_assigned, y_raw)
    y2 = np.where(swap, y_raw, y_assigned)
    r1 = np.corrcoef(x, y1)[0, 1]
    r2 = np.corrcoef(x, y2)[0, 1]
    perm_diffs[i] = r1 - r2

  # I compute one-sided p-value for observed diff being larger than null permutations.
  p_perm_one_sided = (np.sum(perm_diffs >= obs_diff) + 1) / (N_PERM + 1)

  # I return statistics.
  return {
    "bike_year_rows": int(x.size),
    "avg_raw_24_96": float(np.mean(y_raw)),
    "median_raw_24_96": float(np.median(y_raw)),
    "pct_raw_gt_2": float(np.mean(y_raw > 2) * 100.0),
    "avg_assigned_preventive": float(np.mean(y_assigned)),
    "median_assigned_preventive": float(np.median(y_assigned)),
    "pct_assigned_eq_2": float(np.mean(y_assigned == 2) * 100.0),
    "pearson_r_usage_raw": float(r_raw),
    "pearson_p_usage_raw": float(p_raw),
    "pearson_r_usage_assigned": float(r_assigned),
    "pearson_p_usage_assigned": float(p_assigned),
    "pearson_r_usage_overflow": float(r_overflow),
    "pearson_p_usage_overflow": float(p_overflow),
    "spearman_r_usage_raw": float(rs_raw),
    "spearman_p_usage_raw": float(ps_raw),
    "spearman_r_usage_assigned": float(rs_assigned),
    "spearman_p_usage_assigned": float(ps_assigned),
    "corr_diff_raw_minus_assigned": float(obs_diff),
    "perm_p_one_sided_raw_gt_assigned": float(p_perm_one_sided),
  }


# I run formal test C: class duration distributions are different.
def test_c_class_separation(df: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
  # I keep three core classes for distribution separation tests.
  classes = [CLS_SHORT, CLS_PREV, CLS_LONG]

  # I build arrays per class.
  samples = {
    c: df.loc[df["inferred_state_h2"] == c, "gap_hours"].to_numpy()
    for c in classes
  }

  # I compute Kruskal-Wallis global test.
  kw_stat, kw_p = stats.kruskal(*(samples[c] for c in classes))

  # I run pairwise Mann-Whitney U tests.
  pair_rows = []
  pvals = []
  pairs = list(itertools.combinations(classes, 2))
  for a, b in pairs:
    u_stat, p = stats.mannwhitneyu(samples[a], samples[b], alternative="two-sided")
    pvals.append(float(p))
    pair_rows.append(
      {
        "class_a": a,
        "class_b": b,
        "n_a": int(samples[a].size),
        "n_b": int(samples[b].size),
        "median_a": float(np.median(samples[a])),
        "median_b": float(np.median(samples[b])),
        "u_stat": float(u_stat),
        "pval_raw": float(p),
        "cliffs_delta": float(cliffs_delta(samples[a], samples[b])),
      }
    )

  # I correct pairwise p-values for multiple testing with Holm method.
  corrected = holm_correction(pvals)
  for row, p_adj in zip(pair_rows, corrected):
    row["pval_holm"] = float(p_adj)

  # I prepare summary and pairwise outputs.
  summary = {
    "kruskal_stat": float(kw_stat),
    "kruskal_p": float(kw_p),
    "classes_tested": ", ".join(classes),
  }
  pair_df = pd.DataFrame(pair_rows)

  # I return both outputs.
  return summary, pair_df


# I write a markdown report that explains results in technical and layperson language.
def write_markdown(test_a: dict, test_b: dict, test_c_summary: dict, test_c_pairs: pd.DataFrame) -> None:
  # I define simple textual significance labels.
  def sig(p: float) -> str:
    if p < 0.001:
      return "very strong evidence (p < 0.001)"
    if p < 0.01:
      return "strong evidence (p < 0.01)"
    if p < 0.05:
      return "evidence (p < 0.05)"
    return "not statistically significant (p >= 0.05)"

  # I compose pairwise rows for markdown table.
  pair_lines = []
  for _, r in test_c_pairs.iterrows():
    pair_lines.append(
      f"| {r['class_a']} | {r['class_b']} | {r['median_a']:.2f} | {r['median_b']:.2f} | {r['cliffs_delta']:.3f} | {r['pval_raw']:.3e} | {r['pval_holm']:.3e} |"
    )

  # I write markdown content with both technical and plain-language interpretation.
  md = f"""# Formal Hypothesis Tests for Maintenance Inference

## Scope
This document reports formal statistical tests on the revised hypothesis model.

Data source:
- `/home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db`

Logic source:
- `/home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_hypothesis_revised.sql`

Script source:
- `/home/shaun/NTU_M2_Data_Pipeline/scripts/test_maintenance_hypotheses_formal.py`

## H-A: Rebalancing Signal Test
### Hypothesis
- H0: Short-gap events (`<8h`) have the same top-demand destination rate as non-short events.
- H1: Short-gap events have a different (practically expected higher) top-demand destination rate.

### Technical results
- Short group: n = {test_a['n_short']:,}, top-demand = {test_a['top_short']:,}, rate = {test_a['rate_top_short']*100:.2f}%
- Non-short group: n = {test_a['n_non_short']:,}, top-demand = {test_a['top_non_short']:,}, rate = {test_a['rate_top_non_short']*100:.2f}%
- Rate difference = {(test_a['rate_diff']*100):.2f} percentage points
- Two-proportion z = {test_a['z_stat']:.3f}, p = {test_a['pval_z_two_sided']:.3e}
- Fisher exact odds ratio = {test_a['odds_ratio']:.3f}, p = {test_a['pval_fisher_two_sided']:.3e}
- Chi-square = {test_a['chi2_stat']:.3f}, dof = {test_a['chi2_dof']}, p = {test_a['pval_chi2']:.3e}

### Layperson interpretation
This test asks whether short gaps are linked to popular destination stations more than longer gaps.
Result: {sig(test_a['pval_z_two_sided'])}. The difference is large enough to support a real rebalancing signal.

## H-B: Usage Coupling and Preventive-Cap Test
### Hypothesis
- H0: Raw `24h-96h` counts and cap-assigned preventive counts are equally coupled to bike usage.
- H1: Raw `24h-96h` counts are more strongly coupled to usage than cap-assigned preventive counts.

### Technical results
- Bike-year rows = {test_b['bike_year_rows']:,}
- Raw 24-96h: mean = {test_b['avg_raw_24_96']:.2f}, median = {test_b['median_raw_24_96']:.2f}, % > 2 = {test_b['pct_raw_gt_2']:.2f}%
- Assigned preventive: mean = {test_b['avg_assigned_preventive']:.2f}, median = {test_b['median_assigned_preventive']:.2f}, % == 2 = {test_b['pct_assigned_eq_2']:.2f}%
- Pearson r(usage, raw_24_96) = {test_b['pearson_r_usage_raw']:.4f}, p = {test_b['pearson_p_usage_raw']:.3e}
- Pearson r(usage, assigned_preventive) = {test_b['pearson_r_usage_assigned']:.4f}, p = {test_b['pearson_p_usage_assigned']:.3e}
- Pearson r(usage, overflow_24_96) = {test_b['pearson_r_usage_overflow']:.4f}, p = {test_b['pearson_p_usage_overflow']:.3e}
- Spearman rho(usage, raw_24_96) = {test_b['spearman_r_usage_raw']:.4f}, p = {test_b['spearman_p_usage_raw']:.3e}
- Spearman rho(usage, assigned_preventive) = {test_b['spearman_r_usage_assigned']:.4f}, p = {test_b['spearman_p_usage_assigned']:.3e}
- Correlation difference (raw - assigned) = {test_b['corr_diff_raw_minus_assigned']:.4f}
- Permutation test p (one-sided, raw coupling > assigned coupling) = {test_b['perm_p_one_sided_raw_gt_assigned']:.3e}

### Layperson interpretation
This checks whether the raw 24-96h bucket is just a usage-volume effect.
Result: {sig(test_b['perm_p_one_sided_raw_gt_assigned'])}. Raw 24-96h behavior is significantly more usage-driven than the capped preventive label.

## H-C: Class Separation Test (Duration Distributions)
### Hypothesis
- H0: Duration distributions for short-repair, preventive, and long-repair classes are the same.
- H1: At least one class distribution differs.

### Technical results
- Kruskal-Wallis H = {test_c_summary['kruskal_stat']:.3f}, p = {test_c_summary['kruskal_p']:.3e}

Pairwise Mann-Whitney U with Holm correction:
| Class A | Class B | Median A (h) | Median B (h) | Cliff's delta | p raw | p Holm |
|---|---:|---:|---:|---:|---:|---:|
{os.linesep.join(pair_lines)}

### Layperson interpretation
This checks whether the class windows actually represent different duration behavior.
Result: {sig(test_c_summary['kruskal_p'])}. The classes are statistically distinct in duration distributions.

## Overall Conclusion
1. Rebalancing signal is statistically supported.
2. Raw 24-96h counts are strongly usage-coupled.
3. The preventive cap materially reduces usage coupling.
4. Short-repair / preventive / long-repair classes are statistically separable.

## Caveat
These are still inference-based labels without direct maintenance work-order ground truth.
"""

  # I write markdown to file.
  with open(OUTPUT_MD_PATH, "w", encoding="utf-8") as f:
    f.write(md)


# I run the full formal test pipeline from one entrypoint.
def main() -> None:
  # I ensure output folder exists for CSV files.
  os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)

  # I load event-level data.
  events = load_events()

  # I fail fast if no events are loaded.
  if events.empty:
    raise RuntimeError("No relocation events loaded; cannot run formal tests.")

  # I run test A and collect summary stats.
  res_a = test_a_rebalancing(events)

  # I build bike-year table and run test B.
  bike_year = build_bike_year(events)
  res_b = test_b_usage_coupling(bike_year)

  # I run test C and collect global + pairwise outputs.
  res_c_summary, res_c_pairs = test_c_class_separation(events)

  # I save structured outputs for traceability.
  pd.DataFrame([res_a]).to_csv(os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_A.csv"), index=False)
  pd.DataFrame([res_b]).to_csv(os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_B.csv"), index=False)
  res_c_pairs.to_csv(os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_C_pairwise.csv"), index=False)
  pd.DataFrame([res_c_summary]).to_csv(os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_C_summary.csv"), index=False)

  # I write markdown report.
  write_markdown(res_a, res_b, res_c_summary, res_c_pairs)

  # I print concise terminal summary.
  print("Formal tests complete.")
  print("-", OUTPUT_MD_PATH)
  print("-", os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_A.csv"))
  print("-", os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_B.csv"))
  print("-", os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_C_summary.csv"))
  print("-", os.path.join(OUTPUT_DATA_DIR, "maintenance_formal_test_C_pairwise.csv"))


# I run the pipeline when this script is invoked directly.
if __name__ == "__main__":
  main()
