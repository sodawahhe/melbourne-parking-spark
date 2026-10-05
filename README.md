# Melbourne Parking & Congestion Analysis with PySpark

Processing 72.9 million on-street parking sensor records (9.2 GB) with Apache Spark to study
parking demand, violation behaviour and its relationship with traffic volume in the City of
Melbourne — and then tuning the reporting query that runs on top of it.

Built as the major assignment for FIT5202 Data Processing for Big Data (Monash University,
2026 S2), run locally on Spark 4.1.1 in a four-core Docker container.

## Headline results

| | |
|---|---|
| Input scale | 72,912,582 parking records (9.2 GB CSV) + 10,956,114 traffic counts |
| Input partitions | 69, derived from the 128 MiB default split size |
| Data quality issues found | 408 records (0.0006%) with departure before arrival; 25.2 M rows missing sign plate data |
| Query optimisation | **3.46× speed-up — 795.7 s → 230.1 s**, with identical results |
| Plan reduction | 38 operators → 28, two full scans of the 9.2 GB input → one |

## What the analysis does

**Loading and cleansing.** Explicit `StructType` schemas for both CSV sources and the nested
JSON lookups; 12-hour clock timestamps parsed with `to_timestamp`; missing-value markers
(`NA`, `N/A`, `NULL`, `-`, `?`, …) normalised to nulls; a parked-duration column derived from
the two timestamps; and a validation summary counting every parse and conversion failure.
Reversed arrival/departure pairs cluster on the two daylight-saving transition dates, which
rules out a pure sensor-fault explanation.

**Analysis.** Peak-hour flags from weekday and minute-of-day arithmetic; broadcast left joins
against the area and street lookup tables; peak versus off-peak activity and violation rates by
area; high-turnover and long-stay bay rankings with window functions; and a join of parking
sessions against traffic counts on 10-minute time blocks to find when an area is busy on both
measures at once (3.44 M matched blocks, 821 K above average on both).

**Optimisation.** The monthly top-five report was implemented twice — DataFrame API and Spark
SQL — and verified equivalent with `exceptAll()` in both directions. Spark compiled both to the
same 38-operator plan. The Spark UI showed the cost was not shuffle (under 1 MiB) but two full
scans of the 9.2 GB input: the monthly benchmark was being computed by re-aggregating the raw
rows. Replacing it with a window sum over the already-aggregated area-month counts removes the
second scan entirely, which is what produces the 3.46× speed-up.

## Stack

PySpark 4.1.1 (DataFrame API, Spark SQL, window functions, broadcast joins), Catalyst plan
inspection via `explain(mode="formatted")`, Spark UI for stage and task level evidence,
matplotlib, Jupyter, Docker.

## Repository layout

```
notebooks/melbourne-parking-analysis.ipynb   full analysis with all outputs (42 cells, executed end to end)
report/melbourne-parking-analysis.pdf        the same notebook as a PDF, no Jupyter needed
screenshots/                                 Spark UI evidence (jobs, stages, task distribution, SQL DAGs)
```

The Spark UI screenshots are embedded in the notebook, so it renders completely on GitHub and in
any viewer. The `screenshots/` folder keeps the original PNGs at full resolution.

## Data

The source data is not in this repository. `sensordata.csv` alone is 9.2 GB, well beyond
GitHub's limits, and the datasets were supplied for coursework. They derive from the City of
Melbourne's on-street parking sensor and pedestrian/vehicle count open data.

To re-run the notebook, place `sensordata.csv`, `traffic_count.csv`, `area.json` and
`street.json` next to it — the code reads them by bare filename — and run it from a directory
where `screenshots/` is a sibling of the notebook.

## Notes

Spark's local run times depend on the machine, so the timing figures above reproduce as a ratio
rather than as exact seconds. The two measured runs were taken back to back in the same session
with the cache cleared, so they are comparable to each other.
