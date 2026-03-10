# Formal Hypothesis Tests for Maintenance Inference

## Scope
I report formal statistical tests on the revised hypothesis model.

## Revision Trail (Why the First Interpretation Was Updated)
1. The first formal pass (H-A to H-C) showed strong signals for rebalancing and duration-class separation.
2. I then tested my challenge: "Does actual bike usage time before first `24-96h` materially change the interpretation?"
3. That follow-up analysis is now integrated here as H-D and H-E (not treated as a separate final report).
4. Result: the first interpretation had to be updated because first `24-96h` events often appear early in usage exposure, and usage time alone has weak predictive power for `24-96h`.

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
- Short group: n = 1,960,614, top-demand = 946,608, rate = 48.28%
- Non-short group: n = 3,805,914, top-demand = 1,046,108, rate = 27.49%
- Rate difference = 20.79 percentage points
- Two-proportion z = 497.422, p = 0.000e+00
- Fisher exact odds ratio = 2.463, p = 0.000e+00
- Chi-square = 247427.848, dof = 1, p = 0.000e+00

### Layperson interpretation
I test whether short gaps are linked to popular destination stations more than longer gaps.
Result: very strong evidence (p < 0.001). The difference is large enough to support a real rebalancing signal.

## H-B: Usage Coupling and Preventive-Cap Test
### Hypothesis
- H0: Raw `24h-96h` counts and cap-assigned preventive counts are equally coupled to bike usage.
- H1: Raw `24h-96h` counts are more strongly coupled to usage than cap-assigned preventive counts.

### Technical results
- Bike-year rows = 124,161
- Raw 24-96h: mean = 11.77, median = 12.00, % > 2 = 84.28%
- Assigned preventive: mean = 1.84, median = 2.00, % == 2 = 88.92%
- Pearson r(usage, raw_24_96) = 0.8801, p = 0.000e+00
- Pearson r(usage, assigned_preventive) = 0.4831, p = 0.000e+00
- Pearson r(usage, overflow_24_96) = 0.8753, p = 0.000e+00
- Spearman rho(usage, raw_24_96) = 0.8875, p = 0.000e+00
- Spearman rho(usage, assigned_preventive) = 0.5192, p = 0.000e+00
- Correlation difference (raw - assigned) = 0.3971
- Permutation test p (one-sided, raw coupling > assigned coupling) = 4.998e-04

### Layperson interpretation
I check whether the raw 24-96h bucket is just a usage-volume effect.
Result: very strong evidence (p < 0.001). Raw 24-96h behavior is significantly more usage-driven than the capped preventive label.

## H-C: Class Separation Test (Duration Distributions)
### Hypothesis
- H0: Duration distributions for short-repair, preventive, and long-repair classes are the same.
- H1: At least one class distribution differs.

### Technical results
- Kruskal-Wallis H = 1499046.973, p = 0.000e+00

Pairwise Mann-Whitney U with Holm correction:
| Class A | Class B | Median A (h) | Median B (h) | Cliff's delta | p raw | p Holm |
|---|---:|---:|---:|---:|---:|---:|
| Short repair likely (8h-24h) | Preventive maintenance likely (24h-96h, max 2 per bike-year) | 15.23 | 75.48 | -1.000 | 0.000e+00 | 0.000e+00 |
| Short repair likely (8h-24h) | Long repair likely (96h-336h) | 15.23 | 155.28 | -1.000 | 0.000e+00 | 0.000e+00 |
| Preventive maintenance likely (24h-96h, max 2 per bike-year) | Long repair likely (96h-336h) | 75.48 | 155.28 | -1.000 | 0.000e+00 | 0.000e+00 |

### Layperson interpretation
I check whether the class windows actually represent different duration behavior.
Result: very strong evidence (p < 0.001). The classes are statistically distinct in duration distributions.

## H-D: Usage-Time Before First 24-96h Event (Follow-up Test)
### Context
After the first formal test set, I challenged my own interpretation of H-B.
I realized H-B used event-count usage (`relocation_events`) as exposure, but my key question was about **actual usage time** before a bike enters the `24-96h` band.
So I ran a follow-up formal test using cumulative ride duration (`duration`) from **all rides**, not just relocation counts.

My thought process for this follow-up was:
1. If `24-96h` is mostly preventive maintenance, then the first `24-96h` event should usually happen after substantial accumulated bike usage.
2. If first `24-96h` events happen very early in exposure, then the first formal interpretation (usage-driven preventive trigger) is likely overstated.
3. Therefore I should compute cumulative usage time before first `24-96h` and formally test whether that timing is early or late in each bike-year.

Supporting artifacts:
- `/home/shaun/NTU_M2_Data_Pipeline/scripts/test_usage_time_trigger_formal.py`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/data/maintenance_formal_test_D_usage_time_first_hit.csv`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/data/maintenance_formal_test_D_usage_time_buckets.csv`

### Hypothesis
- H0: The first `24-96h` event happens around mid-year exposure (`usage_share_at_first >= 0.5`).
- H1: The first `24-96h` event happens early in yearly exposure (`usage_share_at_first < 0.5`).

### Technical results
- Bike-years with at least one `24-96h` event: `118,061`
- Mean usage minutes before first `24-96h`: `1005.43` (about `16.76h`)
- Median usage minutes before first `24-96h`: `426.00` (about `7.10h`)
- P25/P75 usage minutes: `157.00` / `1035.00`
- P90 usage minutes: `2288.00` (about `38.13h`)
- Mean usage-share at first event: `0.1089`
- Median usage-share at first event: `0.0322`
- Mean sequence-share at first event: `0.1402`
- Median sequence-share at first event: `0.0556`
- Wilcoxon one-sided test (`usage_share < 0.5`): `p = 0.000e+00`
- Binomial sign test (`usage_share < 0.5`): `64,499 / 68,875`, `p = 0.000e+00`

Usage-time buckets before first `24-96h` event:
| Bucket | Bike-Years | Share |
|---|---:|---:|
| 0 min | 6 | 0.01% |
| 0-30 min | 6,640 | 5.62% |
| 30-60 min | 6,285 | 5.32% |
| 1-2 h | 10,802 | 9.15% |
| 2-4 h | 17,037 | 14.43% |
| 4-8 h | 22,460 | 19.02% |
| >8 h | 54,831 | 46.44% |

### Layperson interpretation
My follow-up changed the narrative materially.
My thought process was: “If `24-96h` truly reflects preventive maintenance triggered by heavy usage, the first event should usually appear later in yearly usage exposure.”
But the data shows first `24-96h` events usually appear early in yearly exposure share.
So this weakens a pure “usage-threshold causes preventive event” interpretation.

### Impact on the first formal test (H-B)
1. H-B still correctly showed strong coupling between raw `24-96h` counts and usage volume.
2. H-D adds that the **timing** of first `24-96h` events is usually early, not late.
3. Together, this means H-B alone was not enough to support a direct “wear-triggered preventive schedule” story.
4. I conclude the first formal interpretation is materially affected: the evidence now points to mixed operational causes, not a single usage-threshold mechanism.

## H-E: Next Formal Test Triggered by H-D Findings
### Context
Because H-D showed early first-hit behavior, I ran one more formal test to ask:
“Does cumulative usage time alone meaningfully predict whether an event is in `24-96h`?”

Supporting artifact:
- `/home/shaun/NTU_M2_Data_Pipeline/docs/data/maintenance_formal_test_E_usage_time_association.csv`

### Hypothesis
- H0: Cumulative usage time has no association with `24-96h` event membership.
- H1: Cumulative usage time is associated with `24-96h` event membership.

### Technical results
- Event count: `5,766,528`
- `24-96h` prevalence: `25.34%`
- Point-biserial correlation `r(log_usage, is_24_96) = -0.0622`, `p = 0.000e+00`
- Mann-Whitney U `p = 0.000e+00`
- Cliff's delta `= -0.0658`
- Logistic coefficient on `log1p(usage_minutes)`: `-0.1094`
- Logistic odds ratio per +1 log-unit usage: `0.8963`
- 1-feature logistic AUC: `0.5329`

### Layperson interpretation
Usage time has a statistically detectable relationship with `24-96h`, but it is weak as a standalone predictor.
In plain terms: usage time matters, but not enough to confidently label preventive maintenance by itself.
My second follow-up is the direct response to H-D: because first-hit timing looked early, I tested whether usage time alone can classify `24-96h`, and it cannot do that strongly (AUC `0.5329`).
That is why the revised capped framework remains useful as conservative inference, and why ground-truth maintenance logs are still needed for final confirmation.

## Hypothesis Decision After the Usage-Time Follow-up
### Decision statement (technical)
1. I reject the strong preventive-trigger hypothesis: "`24-96h` events are primarily a direct consequence of high accumulated usage."
2. I retain a weaker operational hypothesis: "`24-96h` events contain a mixed mechanism (some maintenance-like, some non-maintenance operational effects)."
3. I retain the revised capped rule as a conservative inference policy, not as a causal maintenance detector.

### Why I made this decision
1. First-hit timing is early, not late:
   - median usage-share at first `24-96h` = `0.0322`
   - mean usage-share at first `24-96h` = `0.1089`
   - Wilcoxon/binomial tests strongly reject a mid/late exposure interpretation (`p ~ 0`)
2. Event-level predictive strength from usage alone is weak:
   - point-biserial `r = -0.0622` (small effect)
   - Cliff's delta `= -0.0658` (small effect)
   - 1-feature logistic AUC `= 0.5329` (near-random discrimination)
3. Combined interpretation:
   - usage contributes signal, but not enough to explain `24-96h` events as a single usage-threshold process.

### What this means for the formal hypothesis chain
1. H-B is still valid as evidence of usage coupling in raw counts.
2. H-D/H-E refine H-B by showing that coupling does not imply a clean preventive trigger.
3. Therefore, my final model stance is:
   - rebalancing inference: strong support
   - preventive inference: conservative proxy under cap assumptions
   - causal preventive schedule claim: not supported from this data alone

### Layperson version
My follow-up result means I cannot honestly say that bikes usually go into `24-96h` because they first accumulate lots of usage.
The first `24-96h` events often appear too early for that story.
So I now treat `24-96h` as a mixed signal, not pure preventive maintenance.
I still keep the capped preventive rule because it is useful for monitoring, but I treat it as a practical estimate, not proof of workshop scheduling.

## Overall Conclusion
1. The rebalancing hypothesis is statistically supported at a very strong level.
2. Short-gap events (`<8h`) end at top-demand stations much more often than non-short events (about `48.28%` vs `27.49%`, a `20.79` percentage-point gap).
3. I consider this effect non-marginal; odds ratio (`2.463`) and all major tests (z, Fisher, chi-square) agree.
4. Raw `24h-96h` counts are heavily entangled with bike usage volume (`r=0.8801`), so duration-only preventive labeling is not reliable.
5. After applying the preventive cap rule (max 2 per bike-year), usage coupling drops materially (`r=0.4831`), and permutation testing confirms this reduction is statistically meaningful (`p=4.998e-04`).
6. The class windows for short-repair (`8h-24h`), preventive (`24h-96h` capped), and long-repair (`96h-336h`) are statistically separable (global Kruskal-Wallis and pairwise tests all highly significant).
7. Effect-size behavior (Cliff's delta near `-1.0` in pairwise comparisons) indicates these windows are not just statistically different but practically distinct in duration scale.
8. Operationally, the revised framework should be interpreted as a strong rebalancing detector for short demand-linked relocations, a conservative preventive proxy under explicit policy constraints, and a clearer split between short/long repair-like inactivity regimes.
9. I now treat this as a defensible inference framework for monitoring and prioritization, but it is still not equivalent to verified maintenance truth.
10. Final causal confirmation still requires maintenance/work-order labels; when those become available, this model should be recalibrated and validated as a supervised classifier.
11. The usage-time follow-up (H-D/H-E) materially affected interpretation of H-B by showing early first-hit timing and weak standalone predictive power of usage time.
12. Therefore, `24-96h` should not be treated as a direct preventive-maintenance trigger without additional operational evidence.
13. The updated conclusion is: usage contributes signal, but the observed pattern is not consistent with a clean, usage-only preventive schedule.

## Layperson Conclusion (Plain English)
1. I tested my ideas with formal statistics, not just visual charts, and the results are strong.
2. Very short inactivity events are much more likely to end at busy stations, which supports the idea that many short events are bike movement/rebalancing, not maintenance.
3. In simple likelihood terms, for short-gap events (`<8h`) the chance of ending at a top-demand station is `48.28%`, while for non-short events it is `27.49%`; that is a `20.79` percentage-point increase (about `2.46x` higher odds).
4. If I label all `24h-96h` gaps as preventive maintenance, that count is too influenced by how often a bike is used.
5. After applying my “max 2 preventive events per bike-year” rule, this usage effect drops a lot, which means the preventive label becomes more realistic and less inflated.
6. The three duration bands (`8-24h`, `24-96h`, `96-336h`) are clearly different in behavior, so they are not arbitrary buckets.
7. In practical terms, short demand-linked events are mostly operational movement; capped preventive is a conservative estimate of routine servicing; and longer gaps are more consistent with repair or out-of-service conditions.
8. I now have a strong operational framework for monitoring and reporting.
9. I still treat this as inference from ride behavior, not final proof of workshop actions unless matched against real maintenance/work-order records.
10. My follow-up question about usage time before first `24-96h` event was critical and changed the interpretation: many first hits happen too early in exposure to support a pure “maintenance only after heavy use” story.

## Practical Decision Guidance
1. Use rebalancing class outputs for operations planning and station-balancing analytics.
2. Use capped preventive class as a conservative maintenance indicator, not as a definitive maintenance count.
3. Track overflow (`24h-96h` beyond cap) as a risk signal for unresolved downtime or repeated interventions.
4. Prioritize investigation of long-repair and out-of-service classes for service-level impact.
5. Keep formal tests in the pipeline as regression checks whenever thresholds or data coverage change.

## Operational Preventive Scheduling Recommendation
### Why I need a scheduling proxy
I do not have ground-truth preventive-maintenance logs in this dataset, and my current inferential statistics do not prove a true preventive schedule.
Because of that, I use a risk-based operational proxy instead of claiming a verified maintenance cycle.

### Statistic I use to set the schedule
I use a **quantile of cumulative usage time to first high-risk downtime event**, not the mean.

Technical definition:
1. Let `T` be cumulative usage minutes before first event in a chosen risk class.
2. For long-repair risk, I ideally define risk class as `gap_hours >= 96`.
3. While waiting for full long-repair-trigger usage analysis, I use `24h-96h` first-hit usage as a temporary proxy.
4. I set schedule interval `S = Qp(T)` for a chosen percentile `p`.

### Why I use quantiles (not mean)
1. My duration data is right-skewed and mixed-mechanism, so mean is sensitive to tail behavior.
2. Quantiles are robust and directly map to service policy aggressiveness.
3. Quantiles allow transparent risk targeting.

### Policy options from quantiles
1. `S = Q50(T)` (median): aggressive schedule (more frequent interventions).
2. `S = Q75(T)`: balanced schedule (recommended default).
3. `S = Q90(T)`: conservative/cost-saving schedule (higher downtime risk tolerance).

### Current proxy values from this analysis (`24h-96h` first-hit usage)
1. `Q50(T) = 426` minutes (`~7.10` usage hours)
2. `Q75(T) = 1035` minutes (`~17.25` usage hours)
3. `Q90(T) = 2288` minutes (`~38.13` usage hours)

### Recommended operational starting point
1. I start with `S = Q75(T) ~ 1000-1050` usage minutes per bike.
2. I add a calendar backstop (for example monthly) to avoid under-servicing low-usage bikes.
3. I re-estimate `Q75` quarterly and monitor whether `>=96h` and `>336h` rates decline.

### Layperson version
I cannot prove a true preventive schedule from this data alone, so I set a practical schedule based on risk.
I look at how much bikes are used before they first show warning downtime, then I schedule maintenance before that point for most bikes.
Using the 75th percentile is my balanced option: it aims to catch risk early without over-servicing.

## Caveat
I still treat these labels as inference-based without direct maintenance work-order ground truth.
