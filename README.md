# Movie / OTT Streaming Watch-History & Rating Pipeline

**Stack:** Python · Azure Data Factory · ADLS Gen2 · PySpark (Databricks / Synapse) · Azure SQL (T-SQL) · Power BI

A streaming platform lands raw watch events every day: messy device names, null ratings, duplicate session IDs re-delivered a day later, and a catalog with inconsistent casing and missing runtimes. This pipeline ingests one day at a time, cleans and validates it in PySpark, models a star schema in SQL, and serves a Power BI report on watch hours, completion rate by genre and device trends.

> All data is **synthetic** (`src/generate_data.py`). The Python and PySpark parts were run end to end locally and checked against ground truth (results below). The T-SQL, ADF JSON and DAX are deployment-ready templates that were **not** executed against Azure or Power BI.

---

## 1. Architecture

```
 Python generator           ADF (daily tumbling-window trigger)                 PySpark                     Azure SQL                 Power BI
 ────────────────    ┌─────────────────────────────────────────────┐   ┌──────────────────────┐   ┌────────────────────┐    ┌──────────────┐
 catalog.csv         │ GetMetadata: does dt=<run_date> exist?      │   │ silver_clean.py      │   │ stg.* (truncate)   │    │ Watch hours  │
 users.csv           │ Copy landing → bronze (that day only)       │   │  standardise text    │   │ usp_refresh_       │    │ Completion % │
 events/dt=YYYY-MM-DD│ Spark: silver_clean.py --run-date           │ → │  completion_pct      │ → │   warehouse        │ →  │  by genre    │
   (daily files)     │ Spark: gold_marts.py                        │   │  dedupe session_id   │   │ dim_show/user/     │    │ Device trend │
                     │ Copy gold → stg, EXEC usp_refresh_warehouse │   │  quarantine rejects  │   │  device/date       │    │ Top shows    │
                     └─────────────────────────────────────────────┘   │  silver/ (partitioned│   │ fact_watch_events  │    └──────────────┘
                                                                       │  by event_date)      │   │ reporting views    │
                                                                       └──────────────────────┘   └────────────────────┘
```

| Layer | Tool | What it does |
|---|---|---|
| Generate | Python | 306K event rows over 14 daily files, 310 catalog rows, 5K users, with injected data-quality problems and a `ground_truth.json` of correct answers |
| Orchestrate | ADF | Tumbling-window daily trigger, checks the day's landing partition, copies only that day to bronze, runs both Spark steps, loads SQL, logs failures |
| Clean | PySpark | Standardises text, computes `completion_pct`, validates, dedupes `session_id` within the batch and against the last 3 days of silver, writes silver partitioned by `event_date`, quarantines bad rows |
| Model | Azure SQL | `dim_show`, `dim_user`, `dim_device`, `dim_date`, `fact_watch_events`, idempotent refresh procedure, 4 reporting views, run log |
| Report | Power BI | Watch hours, completion rate by genre, device trends, top shows, audience |

## 2. Cleaning and business rules

| Field | Problem injected | Rule |
|---|---|---|
| `device_type` | `Mobile`, `mobile`, `iOS`, `Smart TV`, `desktop`, null… | Lower-case, strip punctuation, map to `MOBILE` / `TV` / `WEB` / `TABLET`; null or unknown → `UNKNOWN` |
| `genre` | `drama`, ` DRAMA `, `sci fi`, `Science Fiction`, null | Normalised key mapped to 9 canonical genres; null → `Unknown` |
| `title` | upper / lower / extra spaces | Trim, collapse spaces, title-case |
| `country`, `plan`, `content_type` | casing, abbreviations (`IN`, `USA`, `UK`), `Film`, `tv series` | Mapped to canonical values; null country → `Unknown` |
| catalog rows | Duplicate `show_id` with a missing runtime | Keep the row that has a runtime |
| `rating` | null (~40%) and out of range (0, 6, 10, -1) | Out of range → null (kept as an unrated session) |
| `watch_minutes` | null, negative | **Rejected** to `silver/rejects` with a reason |
| `show_id` | Unknown show | **Rejected** (`INVALID_SHOW`) |
| `session_id` | Duplicates, same day and re-delivered next day | One row kept; cross-day duplicates caught by anti-join on the last 3 days of silver |
| runtime | Missing for ~3% of sessions | `completion_pct` stays null; excluded from completion rate |

**Metrics:**
- `completion_pct = min(watch_minutes, runtime) / runtime × 100`. It is capped at 100, and re-watches are flagged `over_100_flag`.
- `completed_flag` is true when `watch_minutes × 100 ≥ 90 × runtime`. Integer arithmetic keeps it exact.
- Completion rate = completed sessions ÷ sessions with a known runtime.

**Run invariant, asserted every day:** `rows_read = rejected + duplicates_removed + rows_written`. A broken invariant fails the job.

**Idempotent by design:** silver uses dynamic partition overwrite, and the cross-batch duplicate check skips the day being reloaded, so re-running a date gives the same result (tested). The SQL fact load deletes and re-inserts the dates in staging.

## 3. Results from the local run (synthetic, seed 7)

| Metric | Value |
|---|---|
| Landing files processed | 14 daily partitions, **305,930** event rows |
| Rejected | 4,162 (901 unknown show, 2,068 null minutes, 1,193 negative minutes) |
| Duplicate sessions removed | 5,930 (same-day and re-delivered next day) |
| Silver rows (unique valid sessions) | **295,838** |
| Catalog | 310 raw rows → 300 shows (10 duplicates resolved) |
| Total watch hours | 215,368 |
| Completion rate | 49.6% (sessions with known runtime only) |
| Average rating | 3.81 (about 60% of sessions are rated) |
| Genre completion rate | Animation 63.7% (highest) … Documentary 33.8% (lowest) |
| Device trend | Mobile 49% of weekday hours vs 34% on weekends; TV 26% vs 44% |
| **Validation** | **32 of 32 checks match the generator's ground truth** (row counts, rejects by reason, minutes by device and genre, completed sessions, ratings) |

The genre and device patterns exist because the generator was given those behaviours. They show the dashboard works, not real viewing habits.

## 4. Repository layout

```
src/generate_data.py      catalog, users, daily event files, ground_truth.json
src/silver_clean.py       cleaning, completion_pct, dedupe, partitioned silver, rejects, run metrics
src/gold_marts.py         star-schema parquet/CSV + aggregates + gold_summary.json
src/validate_results.py   expected (ground truth) vs actual
sql/01_schema.sql         staging, dimensions, fact, run log
sql/02_procedures.sql     usp_refresh_warehouse (MERGE dims, idempotent fact load), usp_log_failure
sql/03_views.sql          top shows by watch hours, completion by genre, device trend (7-day window), user engagement
adf/pl_ott_daily_ingest.json   pipeline template      adf/trigger_daily.json   tumbling-window trigger
powerbi/dax_and_model.md  model, DAX measures, report pages
run_local.sh              one-command local run
```

## 5. How to run

**Local (about 10 minutes).** Requires Python 3.10+ and Java 17+ (PySpark). On Windows use WSL2.
```bash
python3 -m venv .venv && source .venv/bin/activate
./run_local.sh                 # or: ./run_local.sh 1000000  for a bigger run
```
Manual steps:
```bash
python3 src/generate_data.py --out data/input --events 300000
python3 src/silver_clean.py --input data/input --output data/lake      # all days, one at a time
python3 src/silver_clean.py --run-date 2026-11-05                       # a single day, as ADF would run it
python3 src/gold_marts.py --output data/lake
python3 src/validate_results.py data/input data/lake
```
Outputs: `data/lake/silver/` (partitioned parquet and rejects), `data/lake/gold_csv/*.csv` (for Power BI), `data/lake/gold_summary.json`, `data/lake/_runs/` (per-day run metrics).

**Azure (optional, untested templates).**
1. Create ADLS Gen2 containers `landing`, `bronze`, `silver`. Upload `data/input/raw/` to `landing`.
2. Run `sql/01_schema.sql`, `02_procedures.sql`, `03_views.sql` on an Azure SQL database.
3. Upload `silver_clean.py` and `gold_marts.py` to DBFS (or Synapse job definitions) and switch paths to `abfss://`.
4. Import the ADF pipeline and trigger, replace placeholder linked services and datasets, and start the trigger.
5. Point Power BI at the `dwh` tables (see `powerbi/dax_and_model.md`).

## 6. Resume entry

**OTT Streaming Watch-History & Rating Pipeline** | Python, ADF, PySpark, Azure SQL, Power BI | github.com/dev-namansahu
- Built a daily-incremental pipeline (ADF, ADLS Gen2, PySpark) that cleaned 300K+ synthetic watch events, standardised device, genre and country fields, computed `completion_pct`, and removed 5.9K duplicate sessions, including re-deliveries across days, into partitioned silver tables with quarantined rejects and an asserted row-count invariant.
- Modelled a star schema in Azure SQL (`dim_show`, `dim_user`, `dim_device`, `dim_date`, `fact_watch_events`) with an idempotent MERGE/delete-insert refresh procedure and reporting views using window functions.
- Delivered a Power BI report for watch hours, completion rate by genre and device trends; validated the pipeline against generator ground truth (32 of 32 checks).

Quote only the numbers you reproduce yourself, and say the data is synthetic.

## 7. Interview talking points

- **Why a tumbling-window trigger?** One run per day window, automatic retry, and backfill of missed days.
- **How do you dedupe across days?** Row-number within the batch, then a left-anti join against the last 3 days of silver. The lookback bounds the scan, and the current day is excluded so a re-run is idempotent.
- **Why reject instead of fix?** Negative or null watch time and unknown shows can't be repaired. They go to a quarantine table with a reason so the data owner can fix the source.
- **Why `try_cast`?** Dirty input must not crash the job under Spark's ANSI mode. Failed casts become nulls that validation then catches.
- **Why is completion rate not computed when runtime is null?** Imputing runtime would invent data, so those sessions are excluded and the report footnote says so.
- **How do you prove it is correct?** The generator knows the right answers. `validate_results.py` compares them to the pipeline output.

## 8. Limitations and next steps

- Synthetic data, one region, no seek or pause events, and sessions are not stitched across devices.
- SQL, ADF and DAX are untested against Azure and Power BI. Expect to adjust datasets, linked services and paths.
- Next steps: Delta Lake with MERGE for silver, Great Expectations checks, a GitHub Actions job running `run_local.sh`, and a Power BI screenshot in this README.
"# ott-watch-pipeline" 
