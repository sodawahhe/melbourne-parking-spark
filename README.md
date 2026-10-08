# Melbourne Parking & Congestion Analysis with PySpark

Processing **72.9 million on-street parking sensor records (9.2 GB)** with Apache Spark to study parking demand, violation behaviour and their relationship with traffic volume across the City of Melbourne — then tuning the reporting query that runs on top of it down from **13 minutes to under 4**.

**Stack:** PySpark 4.1.1 (DataFrame API, Spark SQL, window functions, broadcast joins) · Catalyst plan inspection · Spark UI · matplotlib · Jupyter · Docker

---

## Headline Results

| | |
|---|---|
| Input scale | 72,912,582 parking records (9.2 GB CSV) + 10,956,114 traffic counts |
| Input partitions | 69, from the 128 MiB default split size |
| Data quality issues found | 408 records with departure before arrival; 25.2 M rows missing sign plate data |
| **Query optimisation** | **795.7 s → 230.1 s, a 3.46× speed-up**, results verified identical |
| Plan reduction | 38 operators → 28; two full scans of the 9.2 GB input → one |

Run locally on Spark 4.1.1 in a four-core Docker container (`local[4]`).

## The Data Problem

The sensor extract is not analysis-ready. Three things had to be handled before any result could be trusted:

**Schema inference is unsafe at this scale.** Both CSV sources and the nested JSON lookups are read with explicit `StructType` schemas. Letting Spark infer types means a second pass over 9.2 GB *and* silent coercion of malformed values — explicit schemas eliminate both.

**Missing values hide behind placeholder strings.** `NA`, `N/A`, `NULL`, `-`, `?` and friends are normalised to real nulls before counting, otherwise the null audit reports clean columns that are not clean. The audit found 25,195,820 rows (35%) with no sign plate data and one area row with a missing name.

**408 sessions have departure earlier than arrival.** Rather than dropping them unexamined, I broke them down by cause: 77 fall on daylight-saving transition days, 148 are negative by more than a full day, and 183 are neither. The three groups have different explanations — clock shift, date-field corruption, and likely sensor faults — so a single blanket rule would have been wrong. They are 0.0006% of the data and excluded from duration statistics.

## Analysis

- **Peak-hour classification** from weekday and minute-of-day arithmetic, then peak versus off-peak activity and violation rates per area.
- **Broadcast left joins** against the area and street lookup tables, which are small enough to ship to every executor and avoid a shuffle entirely.
- **Window functions** for high-turnover and long-stay bay rankings across 5,300 bays.
- **Time-block join** of parking sessions against traffic counts on 10-minute blocks: 3,444,626 area-time blocks matched, of which 821,208 are above average on both measures at once.

A sample of what comes out — Docklands carries the heaviest load by a wide margin, and its violation rate is *higher* in peak hours (6.48%) than off-peak (5.25%), while Jolimont shows the opposite pattern (2.74% peak vs 4.19% off-peak):

| AreaId | Area | Peak daily avg | Peak violation % | Off-peak daily avg | Off-peak violation % | Valid sessions |
|---|---|---|---|---|---|---|
| 35 | Docklands | 4,013.64 | 6.48 | 10,054.13 | 5.25 | 9,434,638 |
| 16 | Queensberry | 2,337.38 | 5.08 | 6,546.26 | 2.68 | 5,998,885 |
| 38 | Southbank | 2,101.92 | 4.68 | 5,309.37 | 3.56 | 4,973,044 |
| 24 | Jolimont | 2,117.11 | 2.74 | 3,995.09 | 4.19 | 4,021,552 |

## The Optimisation

The monthly top-five report was implemented twice — DataFrame API and Spark SQL — and verified equivalent with `exceptAll()` in **both** directions (0 rows each way). Spark compiled both to the same 38-operator plan, which is the expected result: they are two front-ends onto one Catalyst optimiser.

The Spark UI showed the cost was **not** shuffle — under 1 MiB moved. It was two full scans of the 9.2 GB input, because the monthly benchmark was being computed by re-aggregating the raw rows a second time.

Replacing that second aggregation with a **window sum over the already-aggregated area-month counts** removes the second scan entirely. That single change is what produces the speed-up:

| | Baseline | Optimised |
|---|---|---|
| Runtime | 795.7 s | 230.1 s |
| Plan operators | 38 | 28 |
| Scans of the 9.2 GB input | 2 | 1 |

Correctness was re-checked after optimising: `exceptAll()` returns 0 rows in both directions against the baseline result.

## Repository

```
notebooks/melbourne-parking-analysis.ipynb   full analysis, 92 cells executed end to end with outputs
report/melbourne-parking-analysis.pdf        the same notebook as a PDF, no Jupyter needed
screenshots/                                 Spark UI evidence (jobs, stages, task distribution, SQL DAGs)
```

The Spark UI screenshots are embedded in the notebook, so it renders completely on GitHub and in any viewer. The `screenshots/` folder keeps the original PNGs at full resolution.

## Data

The source data is not in this repository — `sensordata.csv` alone is 9.2 GB, far beyond GitHub's limits, and the datasets were supplied for coursework. They derive from the City of Melbourne's on-street parking sensor and vehicle count open data.

To re-run the notebook, place `sensordata.csv`, `traffic_count.csv`, `area.json` and `street.json` next to it — the code reads them by bare filename — and run it from a directory where `screenshots/` is a sibling of the notebook.

## Notes

Spark's local run times depend on the machine, so the timing figures reproduce as a ratio rather than exact seconds. The two measured runs were taken back to back in the same session with the cache cleared, so they are comparable to each other.

## Context

Individual assignment for FIT5202 Data Processing for Big Data, Monash University Malaysia, Semester 2 2026.
