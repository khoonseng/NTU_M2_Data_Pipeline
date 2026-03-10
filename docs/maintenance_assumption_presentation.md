# Revised Maintenance Inference - Presentation Version

## 1) Problem Statement
No official maintenance table exists, so I infer maintenance/repair behavior from relocation inactivity gaps.

## 2) User-Guided Assumptions Implemented
1. Very short gaps can be logistics/rebalancing, not maintenance.
2. Preventive maintenance should be a narrower duration window.
3. Preventive maintenance should not happen too many times per bike-year.
4. Longer gaps should be treated as repair or out-of-service candidates.

## 3) Revised Rule Set
- Rebalancing likely: `<8h` and destination is top-demand station (top 10% by starts)
- Operational/unknown short idle: `<8h` and destination is not top-demand
- Short repair likely: `8h-24h`
- Preventive maintenance likely: `24h-96h`, capped to `max 2` events per bike-year
- Repeat downtime beyond preventive cap: additional `24h-96h` events beyond cap
- Long repair likely: `96h-336h`
- Out-of-service candidate: `>336h`

## 4) Core Results
Class shares:
1. Demand rebalancing likely: `16.4156%`
2. Operational/unknown short idle: `17.5843%`
3. Short repair likely: `31.1152%`
4. Preventive maintenance likely (capped): `3.9618%`
5. Repeat downtime beyond preventive cap: `21.3783%`
6. Long repair likely: `7.6790%`
7. Out-of-service candidate: `1.8657%`

Preventive bike-year diagnostics:
- Avg preventive events per bike-year: `1.84`
- Median preventive events per bike-year: `2`
- `% bike-year with >2 preventive`: `0.00%` (cap rule enforced)

Demand-station signal:
- Short repair class to top-demand station: `33.61%`
- Preventive class to top-demand station: `19.56%`

I treat this as support for the idea that short downtime is more mixed with operational movement.

## 5) Math and Implementation Locations
SQL logic and summary math:
- `/home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_hypothesis_revised.sql`

Python charting/export logic:
- `/home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_hypothesis_revised.py`

Libraries:
- DuckDB SQL
- Python: `duckdb`, `pandas`, `numpy`, `matplotlib`, `seaborn`

## 6) Revised Chart Set
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M14_maintenance_h2_gap_distribution_thresholds.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M15_maintenance_h2_class_counts.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M16_maintenance_h2_monthly_mix.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M17_maintenance_h2_preventive_per_bike_year.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M18_maintenance_h2_top_demand_share_by_class.png`

## 7) Key Message for Stakeholders
- The older broad preventive band was too permissive.
- The revised model better separates likely logistics, repair, and capped preventive signals.
- I still treat this as inference; I validate against true maintenance logs when available.

## 8) Final Conclusion
1. Rebalancing signal is credible and consistent with destination-demand behavior.
2. Raw `24h-96h` counts correlate strongly with bike usage (`r=0.8801`), so duration-only preventive labeling is weak.
3. The preventive cap (`max 2 per bike-year`) makes the preventive class conservative and usable for monitoring.
4. Final stance:
   - I use this as an inference framework for operations.
   - I do not treat it as confirmed maintenance truth without work-order data.

## 9) Scheduling Statistic I Use Operationally
1. I use a quantile-based usage trigger, not mean duration.
2. Definition:
   - `T` = cumulative usage minutes before first risk event.
   - Current proxy risk event = first `24h-96h` event.
   - Schedule interval `S = Qp(T)`.
3. Policy choices:
   - `Q50`: aggressive
   - `Q75`: balanced (my recommended default)
   - `Q90`: conservative
4. Current proxy values:
   - `Q50 = 426 min` (`~7.10h`)
   - `Q75 = 1035 min` (`~17.25h`)
   - `Q90 = 2288 min` (`~38.13h`)
5. I start with `Q75` plus a calendar backstop, then tune using observed `>=96h` and `>336h` rates.
6. This is an operational policy recommendation, not proof of true maintenance scheduling.

## 10) Repro Commands
```bash
duckdb /home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db < /home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_hypothesis_revised.sql
```

```bash
conda run -n london-bikes-env python /home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_hypothesis_revised.py
```
